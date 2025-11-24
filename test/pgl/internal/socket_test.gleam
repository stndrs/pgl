import gleam/function
import pgl/config.{type Config}
import pgl/internal
import pgl/internal/socket.{type Socket}

pub fn send_test() {
  let sock = with_mock_socket(config.default, function.identity)

  let assert Ok(result) = socket.send(sock, <<"no-op":utf8>>)
  let assert True = result == sock
}

pub fn send_error_test() {
  let sock =
    with_mock_socket(config.default, fn(sb) {
      let send = fn(_, _) { send_error() }

      socket.set_send(sb, send)
    })

  let assert Error(internal.SocketError(
    kind: internal.SendError(code: internal.Timeout),
    message: "Failed to send",
  )) = socket.send(sock, <<"attempt":utf8>>)
}

pub fn receive_test() {
  let sock =
    with_mock_socket(config.default, fn(sb) {
      let recv = fn(_, _, _) { Ok(<<"working":utf8>>) }

      socket.set_recv(sb, recv)
    })

  let assert Ok(<<"working":utf8>>) = socket.receive(sock, 0)
}

pub fn receive_error_test() {
  let sock =
    with_mock_socket(config.default, fn(sb) {
      let recv = fn(_, _, _) { receive_error() }

      socket.set_recv(sb, recv)
    })

  let assert Error(internal.SocketError(
    kind: internal.ReceiveError(code: internal.Timeout),
    message: "Failed to receive",
  )) = socket.receive(sock, 0)
}

pub fn shutdown_test() {
  let assert Ok(Nil) =
    with_mock_socket(config.default, function.identity)
    |> socket.shutdown
}

pub fn shutdown_error_test() {
  let sock =
    with_mock_socket(config.default, fn(sb) {
      socket.set_shutdown(sb, fn(_) { shutdown_error() })
    })

  let assert Error(internal.SocketError(
    kind: internal.ShutdownError(code: internal.Closed),
    message: "Failed to shutdown",
  )) = socket.shutdown(sock)
}

pub fn connect_error_test() {
  let sb =
    with_mock_socket_builder(fn(sb) {
      socket.set_connect(sb, fn(_) { connect_error() })
    })

  let assert Error(internal.SocketError(
    kind: internal.ConnectError(code: internal.Timeout),
    message: "Failed to connect",
  )) = socket.connect(sb, config.default)
}

pub fn connect_real_test() {
  let conf =
    config.default
    |> config.set_database("gleam_pgl_test")
    |> config.set_username("postgres")
    |> config.set_password("postgres")

  let assert Ok(sock) =
    socket.tcp
    |> socket.connect(conf)

  let assert Ok(Nil) = socket.shutdown(sock)
}

// Error helpers

fn connect_error() -> Result(a, internal.PglError) {
  internal.SocketError(
    kind: internal.ConnectError(code: internal.Timeout),
    message: "Failed to connect",
  )
  |> Error
}

fn send_error() -> Result(Nil, internal.PglError) {
  internal.SendError(internal.Timeout)
  |> internal.SocketError("Failed to send")
  |> Error
}

fn receive_error() -> Result(BitArray, internal.PglError) {
  internal.SocketError(
    kind: internal.ReceiveError(code: internal.Timeout),
    message: "Failed to receive",
  )
  |> Error
}

fn shutdown_error() -> Result(Nil, internal.PglError) {
  internal.SocketError(
    kind: internal.ShutdownError(code: internal.Closed),
    message: "Failed to shutdown",
  )
  |> Error
}

// Mock socket and port helpers 

pub fn with_mock_socket(
  conf: Config,
  next: fn(socket.SocketBuilder) -> socket.SocketBuilder,
) -> Socket {
  let assert Ok(sock) =
    with_mock_socket_builder(next)
    |> socket.connect(conf)

  sock
}

pub fn with_mock_socket_builder(next: fn(socket.SocketBuilder) -> t) -> t {
  socket.new()
  |> socket.set_connect(fn(_) { Ok(coerce(Nil)) })
  |> socket.set_send(fn(_, _) { Ok(Nil) })
  |> socket.set_recv(fn(_, _, _) { Ok(<<"working":utf8>>) })
  |> socket.set_shutdown(fn(_conn) { Ok(Nil) })
  |> next
}

@external(erlang, "pgl_ffi", "coerce")
fn coerce(a: a) -> b
