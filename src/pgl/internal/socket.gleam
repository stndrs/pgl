import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision
import gleam/result
import neon/net
import neon/ssl
import neon/tcp
import pgl/internal
import pgl/internal/decode
import pgl/internal/encode

pub opaque type InternalSocket {
  Tcp(tcp.Tcp)
  Ssl(ssl.Ssl)
}

pub opaque type Builder {
  Builder(host: String, port: Int, ipv6: Bool, timeout: Int)
}

pub opaque type Socket {
  Socket(
    subject: Subject(Msg),
    host: String,
    timeout: Int,
    parameters: Dict(String, String),
  )
}

type State {
  State(
    socket: InternalSocket,
    subject: Subject(Msg),
    timeout: Int,
    ping_timer: Option(process.Timer),
  )
}

pub opaque type Msg {
  StartPing(interval: Int)
  StopPing
  Ping(interval: Int)
  SslUpgrade(
    client: Subject(Result(Nil, internal.InternalError)),
    host: String,
    verified: Bool,
  )
  Send(client: Subject(Result(Nil, internal.SocketError)), payload: BitArray)
  Receive(
    client: Subject(Result(BitArray, internal.SocketError)),
    length: Int,
    timeout: Int,
  )
  Shutdown(client: Subject(Result(Nil, internal.SocketError)))
}

pub fn new() -> Builder {
  Builder(
    host: internal.default_host,
    port: internal.default_port,
    ipv6: False,
    timeout: 1000,
  )
}

pub fn host(builder: Builder, host: String) -> Builder {
  Builder(..builder, host:)
}

pub fn port(builder: Builder, port: Int) -> Builder {
  Builder(..builder, port:)
}

pub fn timeout(builder: Builder, timeout: Int) -> Builder {
  Builder(..builder, timeout:)
}

pub fn ipv6(builder: Builder, ipv6: Bool) -> Builder {
  Builder(..builder, ipv6:)
}

const socket_factory_name = "pgl_sockets"

// The outer `actor.call` deadline must be strictly larger than the inner
// TCP receive timeout so the actor can reply with a `Timeout` error rather
// than the caller's `process.call` panicking on its own deadline.
const call_timeout_buffer = 1000

pub opaque type Factory {
  Factory(
    name: process.Name(factory.Message(Builder, Socket)),
    builder: Builder,
  )
}

pub fn factory(builder: Builder) -> Factory {
  socket_factory_name
  |> process.new_name
  |> Factory(builder:)
}

/// Assign Key/Value pairs to a Socket's parameters Dict.
pub fn parameter(sock: Socket, key: String, value: String) -> Socket {
  let parameters = sock.parameters |> dict.insert(key, value)
  Socket(..sock, parameters:)
}

pub fn connect(factory: Factory) -> Result(Socket, actor.StartError) {
  factory.get_by_name(factory.name)
  |> factory.start_child(factory.builder)
  |> result.map(fn(started) { started.data })
}

pub fn supervised(
  factory: Factory,
) -> supervision.ChildSpecification(factory.Supervisor(Builder, Socket)) {
  factory.worker_child(start_socket)
  |> factory.named(factory.name)
  |> factory.supervised
}

fn start_socket(builder: Builder) -> actor.StartResult(Socket) {
  let Builder(host:, port:, ipv6:, timeout:) = builder

  actor.new_with_initialiser(1000, fn(subject) {
    tcp_connect(host, port, ipv6)
    |> result.map(fn(sock) {
      let selector = process.new_selector() |> process.select(subject)

      let socket = Socket(subject:, host:, timeout:, parameters: dict.new())

      State(socket: sock, subject:, timeout:, ping_timer: None)
      |> actor.initialised
      |> actor.selecting(selector)
      |> actor.returning(socket)
    })
  })
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn start_ping(socket: Socket, interval: Int) -> Nil {
  process.send(socket.subject, StartPing(interval:))
}

pub fn stop_ping(socket: Socket) -> Nil {
  process.send(socket.subject, StopPing)
}

pub fn to_ssl(
  socket: Socket,
  verified verified: Bool,
) -> Result(Socket, internal.InternalError) {
  actor.call(socket.subject, 1000, SslUpgrade(_, socket.host, verified))
  |> result.replace(socket)
}

pub fn send(
  socket: Socket,
  payload: BitArray,
) -> Result(Socket, internal.InternalError) {
  actor.call(socket.subject, 1000, Send(_, payload))
  |> result.map_error(fn(kind) {
    internal.SocketError(kind:, message: "Failed to send")
  })
  |> result.replace(socket)
}

pub fn receive(
  conn: Socket,
  length: Int,
) -> Result(BitArray, internal.InternalError) {
  actor.call(conn.subject, conn.timeout + call_timeout_buffer, Receive(
    _,
    length,
    conn.timeout,
  ))
  |> result.map_error(fn(kind) {
    internal.SocketError(kind:, message: "Failed to receive")
  })
}

pub fn shutdown(conn: Socket) -> Result(Nil, internal.InternalError) {
  actor.call(conn.subject, 1000, Shutdown)
  |> result.map_error(fn(kind) {
    internal.SocketError(kind:, message: "Failed to shutdown")
  })
}

fn ping(
  sock: InternalSocket,
  timeout: Int,
) -> Result(Nil, internal.InternalError) {
  encode.sync()
  |> socket_send(sock, _)
  |> result.map_error(internal.SocketError(_, ""))
  |> flush(sock, timeout)
}

fn flush(
  res: Result(b, internal.InternalError),
  sock: InternalSocket,
  timeout: Int,
) -> Result(b, internal.InternalError) {
  receive_message(sock, timeout)
  |> result.try(fn(msg) {
    case msg {
      internal.ParameterStatus(_, _) -> flush(res, sock, timeout)
      internal.ReadyForQuery(status: _) -> res
      _ -> flush(res, sock, timeout)
    }
  })
}

fn receive_message(
  sock: InternalSocket,
  timeout: Int,
) -> Result(internal.Message, internal.InternalError) {
  net.timeout(timeout)
  |> result.replace_error(internal.SocketError(internal.Timeout, ""))
  |> result.try(fn(timeout) {
    socket_receive(sock, internal.header_size, timeout)
    |> result.map_error(internal.SocketError(_, ""))
    |> result.try(fn(data) {
      case data {
        <<code:bits-size(8), size:int-size(32)>> -> {
          case size - 4 {
            len if len < 0 -> {
              internal.DecodingError
              |> internal.ProtocolError(message: "Invalid message length")
              |> Error
            }
            0 -> decode.message(code, <<>>)
            size1 -> {
              socket_receive(sock, size1, timeout)
              |> result.map_error(internal.SocketError(_, ""))
              |> result.try(decode.message(code, _))
            }
          }
        }
        _ -> {
          internal.DecodingError
          |> internal.ProtocolError(message: "Unexpected data received")
          |> Error
        }
      }
    })
  })
}

fn handle_message(state: State, msg: Msg) -> actor.Next(State, Msg) {
  case msg {
    StartPing(interval:) -> {
      let ping_timer =
        process.send_after(state.subject, interval, Ping(interval:))

      State(..state, ping_timer: Some(ping_timer))
      |> actor.continue
    }
    StopPing -> {
      case state.ping_timer {
        Some(timer) -> process.cancel_timer(timer)
        None -> process.TimerNotFound
      }

      State(..state, ping_timer: None)
      |> actor.continue
    }

    Ping(interval:) -> {
      case ping(state.socket, state.timeout) {
        Ok(_) -> {
          let ping_timer =
            process.send_after(state.subject, interval, Ping(interval:))

          State(..state, ping_timer: Some(ping_timer))
          |> actor.continue
        }
        // A failing keepalive means the idle connection is dead. Stop the
        // actor (closing the socket) instead of re-arming the timer and
        // pinging a dead connection forever.
        Error(_) -> {
          let _ = socket_shutdown(state.socket)
          actor.stop()
        }
      }
    }
    SslUpgrade(client:, host:, verified:) -> {
      case tcp_to_ssl(state.socket, host, verified) {
        Ok(ssl) -> {
          actor.send(client, Ok(Nil))

          State(..state, socket: ssl)
        }
        Error(err) -> {
          let err =
            internal.SocketError(kind: err, message: "Failed to connect SSL")

          actor.send(client, Error(err))

          state
        }
      }
      |> actor.continue
    }
    Send(client:, payload:) -> {
      socket_send(state.socket, payload)
      |> actor.send(client, _)

      actor.continue(state)
    }
    Receive(client:, length:, timeout:) -> {
      net.timeout(timeout)
      |> result.map_error(fn(_) { internal.ConnectError("Invalid timeout") })
      |> result.try(fn(timeout) {
        socket_receive(state.socket, length, timeout)
      })
      |> actor.send(client, _)

      actor.continue(state)
    }
    Shutdown(client:) -> {
      let _ = socket_send(state.socket, encode.terminate())

      socket_shutdown(state.socket)
      |> actor.send(client, _)

      actor.stop()
    }
  }
}

fn tcp_to_ssl(
  socket: InternalSocket,
  host: String,
  verified: Bool,
) -> Result(InternalSocket, internal.SocketError) {
  case socket {
    Tcp(sock) -> {
      let verifier = case verified {
        True -> ssl.verify_peer
        False -> ssl.verify_none
      }

      let host = net.hostname(host)

      ssl.from_tcp(sock, host)
      |> verifier
      |> ssl.connect
      |> result.map(Ssl)
      |> result.map_error(ssl_error_to_socket_error)
    }
    _ -> Ok(socket)
  }
}

fn tcp_connect(
  host: String,
  port: Int,
  ipv6: Bool,
) -> Result(InternalSocket, String) {
  net.port(port)
  |> result.replace_error(internal.ConnectError("Invalid port"))
  |> result.try(fn(port) {
    let ip_version = case ipv6 {
      True -> net.Ipv6
      False -> net.Ipv4
    }

    host
    |> net.hostname
    |> tcp.new(port)
    |> tcp.ip_version(ip_version)
    |> tcp.connect
    |> result.map_error(tcp_error_to_socket_error)
    |> result.map(Tcp)
  })
  |> result.map_error(internal.socket_error_to_string)
}

fn tcp_error_to_socket_error(error: tcp.TcpError) -> internal.SocketError {
  case error {
    tcp.Closed -> internal.Closed
    tcp.Timeout -> internal.Timeout
    tcp.SystemLimit -> internal.SystemLimit
    tcp.Posix(code) -> internal.Posix(code)
    tcp.TcpError(message) -> internal.TcpError(message)
    tcp.NotOwner -> internal.TcpError("Not Owner")
    tcp.InvalidPid -> internal.TcpError("Invalid Pid")
  }
}

fn ssl_error_to_socket_error(error: ssl.SslError) -> internal.SocketError {
  case error {
    ssl.Closed -> internal.Closed
    ssl.Timeout -> internal.Timeout
    ssl.Posix(code) -> internal.Posix(code)
    ssl.TlsAlert(alert, message) ->
      internal.TlsAlert(tls_alert_to_string(alert) <> " | " <> message)
    ssl.SslError(message) -> internal.SslSockError(message)
    ssl.SslNotStarted -> internal.SslSockError("SSL Not Started")
    ssl.InvalidPid -> internal.SslSockError("Invalid Pid")
    ssl.NotOwner -> internal.SslSockError("Not Owner")
  }
}

fn tls_alert_to_string(alert: ssl.TlsAlert) -> String {
  case alert {
    ssl.CloseNotify -> "close_notify"
    ssl.UnexpectedMessage -> "unexpected_message"
    ssl.BadRecordMac -> "bad_record_mac"
    ssl.RecordOverflow -> "record_overflow"
    ssl.HandshakeFailure -> "handshake_failure"
    ssl.BadCertificate -> "bad_certificate"
    ssl.UnsupportedCertificate -> "unsupported_certificate"
    ssl.CertificateRevoked -> "certificate_revoked"
    ssl.CertificateExpired -> "certificate_expired"
    ssl.CertificateUnknown -> "certificate_unknown"
    ssl.IllegalParameter -> "illegal_parameter"
    ssl.UnknownCa -> "unknown_ca"
    ssl.AccessDenied -> "access_denied"
    ssl.DecodeError -> "decode_error"
    ssl.DecryptError -> "decrypt_error"
    ssl.ExportRestriction -> "export_restriction"
    ssl.ProtocolVersion -> "protocol_version"
    ssl.InsufficientSecurity -> "insufficient_security"
    ssl.InternalError -> "internal_error"
    ssl.InappropriateFallback -> "inappropriate_fallback"
    ssl.UserCanceled -> "user_canceled"
    ssl.NoRenegotiation -> "no_renegotiation"
    ssl.UnsupportedExtension -> "unsupported_extension"
    ssl.CertificateUnobtainable -> "certificate_unobtainable"
    ssl.UnrecognizedName -> "unrecognized_name"
    ssl.BadCertificateStatusResponse -> "bad_certificate_status_response"
    ssl.BadCertificateHashValue -> "bad_certificate_hash_value"
    ssl.UnknownPskIdentity -> "unknown_psk_identity"
    ssl.NoApplicationProtocol -> "no_application_protocol"
    ssl.CertificateRequired -> "certificate_required"
  }
}

fn socket_send(
  socket: InternalSocket,
  payload: BitArray,
) -> Result(Nil, internal.SocketError) {
  case socket {
    Tcp(sock) ->
      tcp.send(sock, payload) |> result.map_error(tcp_error_to_socket_error)
    Ssl(sock) ->
      ssl.send(sock, payload) |> result.map_error(ssl_error_to_socket_error)
  }
}

fn socket_receive(
  socket: InternalSocket,
  length: Int,
  timeout: net.Timeout,
) -> Result(BitArray, internal.SocketError) {
  case socket {
    Tcp(sock) ->
      tcp.receive(sock, length, timeout)
      |> result.map_error(tcp_error_to_socket_error)
    Ssl(sock) ->
      ssl.receive(sock, length, timeout)
      |> result.map_error(ssl_error_to_socket_error)
  }
}

fn socket_shutdown(socket: InternalSocket) -> Result(Nil, internal.SocketError) {
  case socket {
    Tcp(sock) ->
      tcp.shutdown(sock) |> result.map_error(tcp_error_to_socket_error)
    Ssl(sock) ->
      ssl.shutdown(sock) |> result.map_error(ssl_error_to_socket_error)
  }
}
