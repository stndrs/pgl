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
    send: fn(Conn, BitArray) -> Result(Nil, internal.PosixError),
    receive: fn(Conn, Int, Int) -> Result(BitArray, internal.PosixError),
    shutdown: fn(Conn) -> Result(Nil, internal.PosixError),
  )
}

pub type Sender =
  fn(Conn, BitArray) -> Result(Nil, internal.PosixError)

pub type Receiver =
  fn(Conn, Int, Int) -> Result(BitArray, internal.PosixError)

pub type Disconnector =
  fn(Conn) -> Result(Nil, internal.PosixError)

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
  charlist.from_string(host)
  |> tcp_connect(port)
  |> result.map(fn(conn) {
    Socket(
      conn:,
      host:,
      parameters: dict.new(),
      send: tcp_send,
      receive: tcp_receive,
      shutdown: tcp_shutdown,
    )
  })
  |> result.map_error(fn(code) {
    internal.SocketError(
      kind: internal.ConnectError(code:),
      message: "Failed to connect",
    )
  })
}

/// Calls the Socket's `send` function
pub fn send(
  sock: Socket,
  payload: BitArray,
) -> Result(Socket, internal.PglError) {
  sock.send(sock.conn, payload)
  |> result.replace(sock)
  |> result.map_error(fn(code) {
    internal.SocketError(
      kind: internal.SendError(code:),
      message: "Failed to send",
    )
  })
}

/// Calls the Socket's `receive` function. The `length` argument indicates the number
/// of bytes to read. `receive`'s timeout is 1000ms.
pub fn receive(sock: Socket, length: Int) -> Result(BitArray, internal.PglError) {
  sock.receive(sock.conn, length, 1000)
  |> result.map_error(fn(code) {
    internal.SocketError(
      kind: internal.ReceiveError(code:),
      message: "Failed to receive",
    )
  })
}

/// Calls the Socket's `shutdown` function. This will disconnect the Socket's connection if
/// it has one. If the Conn doesn't have a connection, this function returns an error. This
/// function also returns an error if shutdown fails.
pub fn shutdown(sock: Socket) -> Result(Nil, internal.PglError) {
  sock.shutdown(sock.conn)
  |> result.map_error(fn(code) {
    internal.SocketError(
      kind: internal.ShutdownError(code:),
      message: "Failed to receive",
    )
  })
}

// Default Conn functions

pub fn ssl_error(message: String) -> internal.PglError {
  internal.SocketError(kind: internal.SslError, message:)
}

pub fn ssl_upgrade(
  sock: Socket,
  verified verified: Bool,
) -> Result(Socket, internal.PglError) {
  ssl_connect(sock.conn, sock.host, verified)
  |> result.map(fn(conn) {
    Socket(
      ..sock,
      conn:,
      send: ssl_send,
      receive: ssl_receive,
      shutdown: ssl_shutdown,
    )
  })
  |> result.map_error(fn(code) {
    internal.SocketError(
      kind: internal.ConnectError(code:),
      message: "Failed to connect SSL",
    )
  })
}

// FFI

// TCP Connection

@external(erlang, "pgl_ffi", "gen_tcp_connect")
fn tcp_connect(host: Charlist, port: Int) -> Result(Conn, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_recv")
fn tcp_receive(
  tcp: Conn,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_send")
fn tcp_send(tcp: Conn, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_shutdown")
fn tcp_shutdown(tcp: Conn) -> Result(Nil, internal.PosixError)

// SSL Connection

@external(erlang, "pgl_ffi", "ssl_connect")
fn ssl_connect(
  tcp: Conn,
  host: String,
  verified: Bool,
) -> Result(Conn, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_send")
fn ssl_send(ssl: Conn, packet: BitArray) -> Result(Nil, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_recv")
fn ssl_receive(
  ssl: Conn,
  read_bytes_num: Int,
  timeout_milliseconds timeout: Int,
) -> Result(BitArray, internal.PosixError)

@external(erlang, "pgl_ffi", "ssl_shutdown")
fn ssl_shutdown(conn: Conn) -> Result(Nil, internal.PosixError)
