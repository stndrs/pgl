import gleam/dict.{type Dict}
import gleam/erlang/charlist.{type Charlist}
import gleam/result
import pgl/internal

pub type Sock

pub opaque type Socket {
  Socket(
    conn: Sock,
    host: String,
    timeout: Int,
    parameters: Dict(String, String),
    send: fn(Sock, BitArray) -> Result(Nil, internal.PglError),
    receive: fn(Sock, Int, Int) -> Result(BitArray, internal.PglError),
    shutdown: fn(Sock) -> Result(Nil, internal.PglError),
  )
}

pub fn new(conn: Sock) -> Socket {
  Socket(
    conn:,
    host: "",
    timeout: 1000,
    parameters: dict.new(),
    send: tcp_send,
    receive: tcp_receive,
    shutdown: tcp_shutdown,
  )
}

pub fn with_send(sock: Socket, send: Sender) -> Socket {
  Socket(..sock, send:)
}

pub fn with_receive(sock: Socket, receive: Receiver) -> Socket {
  Socket(..sock, receive:)
}

pub fn with_shutdown(sock: Socket, shutdown: Disconnector) -> Socket {
  Socket(..sock, shutdown:)
}

pub fn set_parameter(sock: Socket, key: String, value: String) -> Socket {
  let parameters = dict.insert(sock.parameters, key, value)

  Socket(..sock, parameters:)
}

/// Assign Key/Value pairs to a Sock's parameters Dict.
pub fn parameter(sock: Socket, key: String, value: String) -> Socket {
  let parameters = sock.parameters |> dict.insert(key, value)
  Socket(..sock, parameters:)
}

/// Calls the Sock's `connect` function
pub fn connect(
  host: String,
  port: Int,
  timeout: Int,
) -> Result(Socket, internal.PglError) {
  use conn <- result.map(tcp_connect(host, port))

  Socket(
    conn:,
    host:,
    timeout:,
    parameters: dict.new(),
    send: tcp_send,
    receive: tcp_receive,
    shutdown: tcp_shutdown,
  )
}

pub type Sender =
  fn(Sock, BitArray) -> Result(Nil, internal.PglError)

pub type Receiver =
  fn(Sock, Int, Int) -> Result(BitArray, internal.PglError)

pub type Disconnector =
  fn(Sock) -> Result(Nil, internal.PglError)

/// Calls the Sock's `send` function
pub fn send(
  sock: Socket,
  payload: BitArray,
) -> Result(Socket, internal.PglError) {
  sock.send(sock.conn, payload)
  |> result.replace(sock)
}

/// Calls the Sock's `recv` function. The `length` argument indicates the number
/// of bytes to read. `receive`'s timeout is determined by the value set in the `Config`
/// configured when the Sock was first created with `Sock.new`.
pub fn receive(sock: Socket, length: Int) -> Result(BitArray, internal.PglError) {
  sock.receive(sock.conn, length, sock.timeout)
}

/// Calls the Sock's `shutdown` function. This will disconnect the Sock's connection if
/// it has one. If the Sock doesn't have a connection, this function returns an error. This
/// function also returns an error if shutdown fails.
pub fn shutdown(sock: Socket) -> Result(Nil, internal.PglError) {
  sock.shutdown(sock.conn)
}

// Default Sock functions

fn tcp_connect(host: String, port: Int) -> Result(Sock, internal.PglError) {
  charlist.from_string(host)
  |> tcp_connect_(port)
  |> result.map_error(connect_error)
}

fn tcp_send(tcp: Sock, payload: BitArray) -> Result(Nil, internal.PglError) {
  tcp_send_(tcp, payload)
  |> result.map_error(send_error)
}

fn tcp_receive(
  tcp: Sock,
  read_bytes_num: Int,
  timeout_milliseconds: Int,
) -> Result(BitArray, internal.PglError) {
  tcp_recv_(tcp, read_bytes_num, timeout_milliseconds)
  |> result.map_error(receive_error)
}

fn tcp_shutdown(tcp: Sock) -> Result(Nil, internal.PglError) {
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
  tcp: Sock,
  host: String,
  verified verified: Bool,
) -> Result(Sock, internal.PglError) {
  ssl_connect_(tcp, host, verified)
  |> result.map_error(connect_error)
}

fn ssl_send(ssl: Sock, payload: BitArray) -> Result(Nil, internal.PglError) {
  ssl_send_(ssl, payload)
  |> result.map_error(send_error)
}

fn ssl_receive(
  ssl: Sock,
  read_bytes_num: Int,
  timeout_milliseconds: Int,
) -> Result(BitArray, internal.PglError) {
  ssl_recv_(ssl, read_bytes_num, timeout_milliseconds)
  |> result.map_error(receive_error)
}

fn ssl_shutdown(ssl: Sock) -> Result(Nil, internal.PglError) {
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
  tcp: Sock,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_send")
fn tcp_send_(tcp: Sock, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_shutdown")
fn tcp_shutdown_(tcp: Sock) -> Result(Nil, internal.PosixError)

// SSL Connection

@external(erlang, "pgl_ffi", "ssl_connect")
fn ssl_connect_(
  tcp: Sock,
  host: String,
  verified: Bool,
) -> Result(Sock, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_send")
fn ssl_send_(ssl: Sock, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_recv")
fn ssl_recv_(
  ssl: Sock,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_shutdown")
fn ssl_shutdown_(conn: Sock) -> Result(Nil, internal.PosixError)
