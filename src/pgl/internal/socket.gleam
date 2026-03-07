import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision
import gleam/result
import neon/net
import neon/ssl
import neon/tcp
import pgl/internal

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

pub opaque type Msg {
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

      sock
      |> actor.initialised
      |> actor.selecting(selector)
      |> actor.returning(socket)
    })
  })
  |> actor.on_message(handle_message)
  |> actor.start
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
  actor.call(conn.subject, conn.timeout, Receive(_, length, conn.timeout))
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

fn handle_message(
  sock: InternalSocket,
  msg: Msg,
) -> actor.Next(InternalSocket, Msg) {
  case msg {
    SslUpgrade(client:, host:, verified:) -> {
      case tcp_to_ssl(sock, host, verified) {
        Ok(ssl) -> {
          actor.send(client, Ok(Nil))

          ssl
        }
        Error(err) -> {
          let err =
            internal.SocketError(kind: err, message: "Failed to connect SSL")

          actor.send(client, Error(err))

          sock
        }
      }
      |> actor.continue
    }
    Send(client:, payload:) -> {
      socket_send(sock, payload)
      |> actor.send(client, _)

      actor.continue(sock)
    }
    Receive(client:, length:, timeout:) -> {
      net.timeout(timeout)
      |> result.map_error(fn(_) { internal.ConnectError("Invalid Port") })
      |> result.try(fn(timeout) { socket_receive(sock, length, timeout) })
      |> actor.send(client, _)

      actor.continue(sock)
    }
    Shutdown(client:) -> {
      socket_shutdown(sock)
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
