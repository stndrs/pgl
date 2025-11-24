import gleam/dict.{type Dict}
import gleam/erlang/charlist.{type Charlist}
import gleam/result
import pgl/config.{type Config}
import pgl/internal

pub type Connect =
  fn(Config) -> Result(Sock, internal.PglError)

pub type Send =
  fn(Sock, BitArray) -> Result(Nil, internal.PglError)

pub type Receive =
  fn(Sock, Int, Int) -> Result(BitArray, internal.PglError)

pub type Shutdown =
  fn(Sock) -> Result(Nil, internal.PglError)

pub opaque type SocketBuilder {
  SocketBuilder(connect: Connect, send: Send, recv: Receive, shutdown: Shutdown)
}

pub const tcp: SocketBuilder = SocketBuilder(
  connect: tcp_connect,
  send: tcp_send,
  recv: tcp_recv,
  shutdown: tcp_shutdown,
)

pub type Sock

pub opaque type Socket {
  Socket(
    conn: Sock,
    conf: Config,
    parameters: Dict(String, String),
    send: Send,
    recv: Receive,
    shutdown: Shutdown,
  )
}

fn set_parameters(sock: Socket, parameters: Dict(String, String)) -> Socket {
  Socket(..sock, parameters:)
}

/// Returns a Socket type
pub fn new() -> SocketBuilder {
  SocketBuilder(
    connect: fn(_) { Error(connect_error(internal.Closed)) },
    send: fn(_, _) { Error(send_error(internal.Closed)) },
    recv: fn(_, _, _) { Error(receive_error(internal.Closed)) },
    shutdown: fn(_) { Error(shutdown_error(internal.Closed)) },
  )
}

/// Assign Key/Value pairs to a Socket's parameters Dict.
pub fn set_parameter(sock: Socket, key: String, value: String) -> Socket {
  let parameters = sock.parameters |> dict.insert(key, value)
  Socket(..sock, parameters:)
}

/// Set the connect function to the Socket. The provided function will be called when
/// a Socket is passed to `socket.connect`.
pub fn set_connect(sock: SocketBuilder, connect: Connect) -> SocketBuilder {
  SocketBuilder(..sock, connect:)
}

/// Set the send function to the Socket. The provided function will be called when
/// a Socket is passed to `socket.send`.
pub fn set_send(sock: SocketBuilder, send: Send) -> SocketBuilder {
  SocketBuilder(..sock, send:)
}

/// Set the recv function to the Socket. The provided function will be called when
/// a Socket is passed to `socket.receive`.
pub fn set_recv(sock: SocketBuilder, recv: Receive) -> SocketBuilder {
  SocketBuilder(..sock, recv:)
}

/// Set the shutdown function to the Socket. The provided function will be called when
/// a Socket is passed to `socket.shutdown`.
pub fn set_shutdown(sock: SocketBuilder, shutdown: Shutdown) -> SocketBuilder {
  SocketBuilder(..sock, shutdown:)
}

/// Calls the Socket's `connect` function
pub fn connect(
  sock: SocketBuilder,
  conf: Config,
) -> Result(Socket, internal.PglError) {
  use conn <- result.map(sock.connect(conf))

  Socket(
    conn:,
    conf:,
    parameters: dict.new(),
    send: sock.send,
    recv: sock.recv,
    shutdown: sock.shutdown,
  )
}

/// Calls the Socket's `send` function
pub fn send(
  sock: Socket,
  payload: BitArray,
) -> Result(Socket, internal.PglError) {
  sock.send(sock.conn, payload)
  |> result.replace(sock)
}

/// Calls the Socket's `recv` function. The `length` argument indicates the number
/// of bytes to read. `receive`'s timeout is determined by the value set in the `Config`
/// configured when the Socket was first created with `socket.new`.
pub fn receive(sock: Socket, length: Int) -> Result(BitArray, internal.PglError) {
  sock.recv(sock.conn, length, sock.conf.recv_timeout)
}

/// Calls the Socket's `shutdown` function. This will disconnect the Socket's connection if
/// it has one. If the Socket doesn't have a connection, this function returns an error. This
/// function also returns an error if shutdown fails.
pub fn shutdown(sock: Socket) -> Result(Nil, internal.PglError) {
  sock.shutdown(sock.conn)
}

// Default socket functions

fn tcp_connect(conf: Config) -> Result(Sock, internal.PglError) {
  charlist.from_string(conf.host)
  |> tcp_connect_(conf.port)
  |> result.map_error(connect_error)
}

fn tcp_send(port: Sock, payload: BitArray) -> Result(Nil, internal.PglError) {
  tcp_send_(port, payload)
  |> result.map_error(send_error)
}

fn tcp_recv(
  port: Sock,
  read_bytes_num: Int,
  timeout_milliseconds: Int,
) -> Result(BitArray, internal.PglError) {
  tcp_recv_(port, read_bytes_num, timeout_milliseconds)
  |> result.map_error(receive_error)
}

fn tcp_shutdown(port: Sock) -> Result(Nil, internal.PglError) {
  tcp_shutdown_(port)
  |> result.map_error(shutdown_error)
}

pub fn ssl_error(message: String) -> internal.PglError {
  internal.SocketError(kind: internal.SslError, message:)
}

pub fn ssl_upgrade(
  sock: Socket,
  verified verified: Bool,
) -> Result(Socket, internal.PglError) {
  ssl(sock, verified:)
  |> connect(sock.conf)
  |> result.map(set_parameters(_, sock.parameters))
}

fn ssl(sock: Socket, verified verified: Bool) -> SocketBuilder {
  let ssl_connect = fn(conf: Config) {
    ssl_connect_(sock.conn, conf.host, verified)
    |> result.map_error(connect_error)
  }

  SocketBuilder(
    connect: ssl_connect,
    send: ssl_send,
    recv: ssl_recv,
    shutdown: ssl_shutdown,
  )
}

fn ssl_send(ssl: b, payload: BitArray) -> Result(Nil, internal.PglError) {
  ssl_send_(ssl, payload)
  |> result.map_error(send_error)
}

fn ssl_recv(
  ssl: b,
  read_bytes_num: Int,
  timeout_milliseconds: Int,
) -> Result(BitArray, internal.PglError) {
  ssl_recv_(ssl, read_bytes_num, timeout_milliseconds)
  |> result.map_error(receive_error)
}

fn ssl_shutdown(ssl: b) -> Result(Nil, internal.PglError) {
  ssl_shutdown_(ssl)
  |> result.map_error(shutdown_error)
}

// Error helpers

fn connect_error(code: internal.PosixError) -> internal.PglError {
  internal.SocketError(
    kind: internal.ConnectError(code:),
    message: "Failed to connect",
  )
}

fn send_error(code: internal.PosixError) -> internal.PglError {
  internal.SocketError(
    kind: internal.SendError(code:),
    message: "Failed to send",
  )
}

fn receive_error(code: internal.PosixError) -> internal.PglError {
  internal.SocketError(
    kind: internal.ReceiveError(code:),
    message: "Failed to receive",
  )
}

fn shutdown_error(code: internal.PosixError) -> internal.PglError {
  internal.SocketError(
    kind: internal.ShutdownError(code:),
    message: "Failed to shutdown",
  )
}

// FFI

// TCP Connection

@external(erlang, "pgl_ffi", "gen_tcp_connect")
fn tcp_connect_(host: Charlist, port: Int) -> Result(Sock, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_recv")
fn tcp_recv_(
  sock: Sock,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_send")
fn tcp_send_(sock: Sock, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_shutdown")
fn tcp_shutdown_(sock: Sock) -> Result(Nil, internal.PosixError)

// SSL Connection

@external(erlang, "pgl_ffi", "ssl_connect")
fn ssl_connect_(
  sock: a,
  host: String,
  verified: Bool,
) -> Result(b, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_send")
fn ssl_send_(conn: b, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_recv")
fn ssl_recv_(
  conn: b,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_shutdown")
fn ssl_shutdown_(conn: b) -> Result(Nil, internal.PosixError)
