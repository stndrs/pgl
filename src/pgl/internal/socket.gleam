import gleam/dict.{type Dict}
import gleam/erlang/charlist.{type Charlist}
import gleam/result
import pgl/internal

pub type Conn

pub opaque type Socket {
  Socket(
    conn: Conn,
    host: String,
    parameters: Dict(String, String),
    send: fn(Conn, BitArray) -> Result(Nil, internal.PglError),
    receive: fn(Conn, Int, Int) -> Result(BitArray, internal.PglError),
    shutdown: fn(Conn) -> Result(Nil, internal.PglError),
  )
}

pub type Sender =
  fn(Conn, BitArray) -> Result(Nil, internal.PglError)

pub type Receiver =
  fn(Conn, Int, Int) -> Result(BitArray, internal.PglError)

pub type Disconnector =
  fn(Conn) -> Result(Nil, internal.PglError)

pub fn with_send(sock: Socket, send: Sender) -> Socket {
  Socket(..sock, send:)
}

pub fn with_receive(sock: Socket, receive: Receiver) -> Socket {
  Socket(..sock, receive:)
}

pub fn with_shutdown(sock: Socket, shutdown: Disconnector) -> Socket {
  Socket(..sock, shutdown:)
}

/// Assign Key/Value pairs to a Socket's parameters Dict.
pub fn parameter(sock: Socket, key: String, value: String) -> Socket {
  let parameters = sock.parameters |> dict.insert(key, value)
  Socket(..sock, parameters:)
}

/// Creates a TCP connection and returns a Socket
pub fn connect(host: String, port: Int) -> Result(Socket, internal.PglError) {
  use conn <- result.map(tcp_connect(host, port))

  Socket(
    conn:,
    host:,
    parameters: dict.new(),
    send: tcp_send,
    receive: tcp_receive,
    shutdown: tcp_shutdown,
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

/// Calls the Socket's `receive` function. The `length` argument indicates the number
/// of bytes to read. `receive`'s timeout is 1000ms.
pub fn receive(sock: Socket, length: Int) -> Result(BitArray, internal.PglError) {
  sock.receive(sock.conn, length, 1000)
}

/// Calls the Socket's `shutdown` function. This will disconnect the Socket's connection if
/// it has one. If the Conn doesn't have a connection, this function returns an error. This
/// function also returns an error if shutdown fails.
pub fn shutdown(sock: Socket) -> Result(Nil, internal.PglError) {
  sock.shutdown(sock.conn)
}

// Default Conn functions

fn tcp_connect(host: String, port: Int) -> Result(Conn, internal.PglError) {
  charlist.from_string(host)
  |> tcp_connect_(port)
  |> result.map_error(connect_error)
}

fn tcp_send(tcp: Conn, payload: BitArray) -> Result(Nil, internal.PglError) {
  tcp_send_(tcp, payload)
  |> result.map_error(send_error)
}

fn tcp_receive(
  tcp: Conn,
  read_bytes_num: Int,
  timeout_milliseconds: Int,
) -> Result(BitArray, internal.PglError) {
  tcp_receive_(tcp, read_bytes_num, timeout_milliseconds)
  |> result.map_error(receive_error)
}

fn tcp_shutdown(tcp: Conn) -> Result(Nil, internal.PglError) {
  tcp_shutdown_(tcp)
  |> result.map_error(shutdown_error)
}

pub fn ssl_error(message: String) -> internal.PglError {
  internal.SocketError(kind: internal.SslError, message:)
}

pub fn ssl_upgrade(
  sock: Socket,
  verified verified: Bool,
) -> Result(Socket, internal.PglError) {
  use ssl <- result.map(ssl_connect(sock.conn, sock.host, verified))

  Socket(
    ..sock,
    conn: ssl,
    send: ssl_send,
    receive: ssl_receive,
    shutdown: ssl_shutdown,
  )
}

fn ssl_connect(
  tcp: Conn,
  host: String,
  verified verified: Bool,
) -> Result(Conn, internal.PglError) {
  ssl_connect_(tcp, host, verified)
  |> result.map_error(connect_error)
}

fn ssl_send(ssl: Conn, payload: BitArray) -> Result(Nil, internal.PglError) {
  ssl_send_(ssl, payload)
  |> result.map_error(send_error)
}

fn ssl_receive(
  ssl: Conn,
  read_bytes_num: Int,
  timeout_milliseconds: Int,
) -> Result(BitArray, internal.PglError) {
  ssl_receive_(ssl, read_bytes_num, timeout_milliseconds)
  |> result.map_error(receive_error)
}

fn ssl_shutdown(ssl: Conn) -> Result(Nil, internal.PglError) {
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
fn tcp_connect_(host: Charlist, port: Int) -> Result(Conn, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_recv")
fn tcp_receive_(
  tcp: Conn,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_send")
fn tcp_send_(tcp: Conn, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_shutdown")
fn tcp_shutdown_(tcp: Conn) -> Result(Nil, internal.PosixError)

// SSL Connection

@external(erlang, "pgl_ffi", "ssl_connect")
fn ssl_connect_(
  tcp: Conn,
  host: String,
  verified: Bool,
) -> Result(Conn, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_send")
fn ssl_send_(ssl: Conn, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_recv")
fn ssl_receive_(
  ssl: Conn,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_shutdown")
fn ssl_shutdown_(conn: Conn) -> Result(Nil, internal.PosixError)
