import gleam/erlang/port.{type Port}
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as supervisor
import global_value
import pgl/internal
import pgl/internal/socket

pub fn connect() {
  let assert Ok(tcp_port) = tcp_listen(0)
  let assert Ok(port_num) = inet_port(tcp_port)

  let builder =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(port_num)

  sockets() |> new_socket(builder)
}

pub fn new_socket(
  name: process.Name(_),
  builder: socket.SocketBuilder,
) -> socket.Socket {
  let assert Ok(started) =
    factory.get_by_name(name)
    |> factory.start_child(builder)

  started.data
}

pub fn sockets() -> process.Name(_) {
  use <- global_value.create_with_unique_name("socket_test_factory")

  let name = process.new_name("socket_test_sockets")

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(socket.supervised(name))
    |> supervisor.start

  name
}

pub fn connect_error_test() {
  let assert Error(actor.InitFailed("Failed to start connection")) =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(1)
    |> socket.connect
}

pub fn send_test() {
  let sock = connect()

  let assert Ok(_) = socket.send(sock, <<"bits":utf8>>)
  let assert Ok(_) = socket.shutdown(sock)
}

pub fn send_error_test() {
  let builder =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.with_send(fn(_, _) { Error(internal.Econnreset) })

  let sock =
    sockets()
    |> new_socket(builder)

  let assert Error(internal.SocketError(
    internal.SendError(_posix),
    "Failed to send",
  )) = socket.send(sock, <<"bits":utf8>>)
}

pub fn receive_test() {
  let builder =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.with_receive(fn(_, _, _) { Ok(<<"bits":utf8>>) })

  let sock =
    sockets()
    |> new_socket(builder)

  let assert Ok(<<"bits":utf8>>) = socket.receive(sock, 0)

  let assert Ok(_) = socket.shutdown(sock)
}

pub fn receive_error_test() {
  let builder =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.with_receive(fn(_, _, _) { Error(internal.Closed) })

  let sock =
    sockets()
    |> new_socket(builder)

  let assert Error(internal.SocketError(
    internal.ReceiveError(_posix),
    "Failed to receive",
  )) = socket.receive(sock, 5)
}

@external(erlang, "pgl_ffi", "gen_tcp_listen")
fn tcp_listen(port: Int) -> Result(Port, internal.PosixError)

@external(erlang, "inet", "port")
fn inet_port(port: Port) -> Result(Int, Nil)
