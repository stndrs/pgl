import gleam/dict.{type Dict}
import gleam/erlang/charlist.{type Charlist}
import gleam/erlang/process.{type Subject}
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision
import gleam/result
import pgl/internal

type TcpSocket

type SslSocket

pub opaque type InternalSocket {
  Tcp(TcpSocket)
  Ssl(SslSocket)
}

pub opaque type SocketBuilder {
  SocketBuilder(
    host: String,
    port: Int,
    timeout: Int,
    send: Sender,
    receive: Receiver,
    shutdown: Disconnector,
  )
}

pub opaque type Socket {
  Socket(
    subject: Subject(Msg),
    host: String,
    timeout: Int,
    parameters: Dict(String, String),
    send: Sender,
    receive: Receiver,
    shutdown: Disconnector,
  )
}

pub opaque type Msg {
  SslUpgrade(
    client: Subject(Result(Nil, internal.PglError)),
    host: String,
    verified: Bool,
  )
  Send(
    client: Subject(Result(Nil, internal.PosixError)),
    send: Sender,
    payload: BitArray,
  )
  Receive(
    client: Subject(Result(BitArray, internal.PosixError)),
    receive: Receiver,
    length: Int,
    timeout: Int,
  )
  Shutdown(
    client: Subject(Result(Nil, internal.PosixError)),
    shutdown: Disconnector,
  )
}

pub fn new() -> SocketBuilder {
  SocketBuilder(
    host: internal.default_host,
    port: internal.default_port,
    timeout: 1000,
    send: socket_send,
    receive: socket_receive,
    shutdown: socket_shutdown,
  )
}

pub fn host(builder: SocketBuilder, host: String) -> SocketBuilder {
  SocketBuilder(..builder, host:)
}

pub fn port(builder: SocketBuilder, port: Int) -> SocketBuilder {
  SocketBuilder(..builder, port:)
}

pub fn timeout(builder: SocketBuilder, timeout: Int) -> SocketBuilder {
  SocketBuilder(..builder, timeout:)
}

pub type Sender =
  fn(InternalSocket, BitArray) -> Result(Nil, internal.PosixError)

pub type Receiver =
  fn(InternalSocket, Int, Int) -> Result(BitArray, internal.PosixError)

pub type Disconnector =
  fn(InternalSocket) -> Result(Nil, internal.PosixError)

pub fn with_send(builder: SocketBuilder, send: Sender) -> SocketBuilder {
  SocketBuilder(..builder, send:)
}

pub fn with_receive(builder: SocketBuilder, receive: Receiver) -> SocketBuilder {
  SocketBuilder(..builder, receive:)
}

pub fn with_shutdown(
  builder: SocketBuilder,
  shutdown: Disconnector,
) -> SocketBuilder {
  SocketBuilder(..builder, shutdown:)
}

/// Assign Key/Value pairs to a Socket's parameters Dict.
pub fn parameter(sock: Socket, key: String, value: String) -> Socket {
  let parameters = sock.parameters |> dict.insert(key, value)
  Socket(..sock, parameters:)
}

pub fn connect(
  name: process.Name(factory.Message(SocketBuilder, Socket)),
  builder: SocketBuilder,
) -> Result(Socket, internal.PglError) {
  factory.get_by_name(name)
  |> factory.start_child(builder)
  |> result.map_error(fn(_start_error) {
    internal.PglError("Failed to start connection")
  })
  |> result.map(fn(started) { started.data })
}

pub fn supervised(
  name: process.Name(factory.Message(SocketBuilder, Socket)),
) -> supervision.ChildSpecification(factory.Supervisor(SocketBuilder, Socket)) {
  factory.worker_child(start_socket)
  |> factory.named(name)
  |> factory.supervised
}

fn start_socket(builder: SocketBuilder) -> actor.StartResult(Socket) {
  let SocketBuilder(host:, port:, timeout:, send:, receive:, shutdown:) =
    builder

  actor.new_with_initialiser(1000, fn(subject) {
    tcp_connect(host, port)
    |> result.map_error(fn(_) { "Failed to start connection" })
    |> result.map(fn(sock) {
      let selector = process.new_selector() |> process.select(subject)

      let socket =
        Socket(
          subject:,
          host:,
          timeout:,
          parameters: dict.new(),
          send:,
          receive:,
          shutdown:,
        )

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
) -> Result(Socket, internal.PglError) {
  actor.call(socket.subject, 1000, SslUpgrade(_, socket.host, verified))
  |> result.replace(socket)
}

pub fn send(
  socket: Socket,
  payload: BitArray,
) -> Result(Socket, internal.PglError) {
  actor.call(socket.subject, 1000, Send(_, socket.send, payload))
  |> result.map_error(fn(code) {
    internal.SocketError(code:, message: "Failed to send")
  })
  |> result.replace(socket)
}

pub fn receive(conn: Socket, length: Int) -> Result(BitArray, internal.PglError) {
  actor.call(conn.subject, conn.timeout, Receive(
    _,
    conn.receive,
    length,
    conn.timeout,
  ))
  |> result.map_error(fn(code) {
    internal.SocketError(code:, message: "Failed to receive")
  })
}

pub fn shutdown(conn: Socket) -> Result(Nil, internal.PglError) {
  actor.call(conn.subject, 1000, Shutdown(_, conn.shutdown))
  |> result.map_error(fn(code) {
    internal.SocketError(code:, message: "Failed to shutdown")
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
          actor.send(client, Error(err))

          sock
        }
      }
      |> actor.continue
    }
    Send(client:, send:, payload:) -> {
      send(sock, payload)
      |> result.replace(Nil)
      |> actor.send(client, _)

      actor.continue(sock)
    }
    Receive(client:, receive:, length:, timeout:) -> {
      receive(sock, length, timeout)
      |> actor.send(client, _)

      actor.continue(sock)
    }
    Shutdown(client:, shutdown:) -> {
      shutdown(sock)
      |> actor.send(client, _)

      actor.stop()
    }
  }
}

fn tcp_to_ssl(
  socket: InternalSocket,
  host: String,
  verified: Bool,
) -> Result(InternalSocket, internal.PglError) {
  case socket {
    Tcp(sock) -> {
      sock
      |> ssl_connect(host, verified)
      |> result.map(Ssl)
    }
    _ -> Ok(socket)
  }
  |> result.map_error(fn(code) {
    internal.SocketError(code:, message: "Failed to connect SSL")
  })
}

fn tcp_connect(
  host: String,
  port: Int,
) -> Result(InternalSocket, internal.PglError) {
  host
  |> charlist.from_string
  |> tcp_connect_(port)
  |> result.map(Tcp)
  |> result.map_error(fn(code) {
    internal.SocketError(code:, message: "Failed to connect")
  })
}

fn socket_send(
  socket: InternalSocket,
  payload: BitArray,
) -> Result(Nil, internal.PosixError) {
  case socket {
    Tcp(sock) -> tcp_send(sock, payload)
    Ssl(sock) -> ssl_send(sock, payload)
  }
}

fn socket_receive(
  socket: InternalSocket,
  length: Int,
  timeout: Int,
) -> Result(BitArray, internal.PosixError) {
  case socket {
    Tcp(sock) -> tcp_receive(sock, length, timeout)
    Ssl(sock) -> ssl_receive(sock, length)
  }
}

fn socket_shutdown(socket: InternalSocket) -> Result(Nil, internal.PosixError) {
  case socket {
    Tcp(sock) -> tcp_shutdown(sock)
    Ssl(sock) -> ssl_shutdown(sock)
  }
}

// SSL

@external(erlang, "pgl_ffi", "gen_tcp_connect")
fn tcp_connect_(
  host: Charlist,
  port: Int,
) -> Result(TcpSocket, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_recv")
fn tcp_receive(
  socket: TcpSocket,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_send")
fn tcp_send(
  socket: TcpSocket,
  packet: BitArray,
) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_shutdown")
fn tcp_shutdown(socket: TcpSocket) -> Result(Nil, internal.PosixError)

// SSL

@external(erlang, "pgl_ffi", "ssl_connect")
fn ssl_connect(
  socket: TcpSocket,
  host: String,
  verified: Bool,
) -> Result(SslSocket, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_send")
fn ssl_send(
  socket: SslSocket,
  payload: BitArray,
) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_recv")
fn ssl_receive(
  socket: SslSocket,
  length: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_shutdown")
fn ssl_shutdown(socket: SslSocket) -> Result(Nil, internal.PosixError)
