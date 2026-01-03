import gleam/erlang/port.{type Port}
import gleam/otp/static_supervisor as supervisor
import pgl/internal
import pgl/internal/socket

pub fn connect() {
  let assert Ok(tcp_port) = tcp_listen(0)
  let assert Ok(port_num) = inet_port(tcp_port)

  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(port_num)
    |> socket.factory

  let assert Ok(_) = supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  sock
}

pub fn connect_ipv6_test() {
  let assert Ok(tcp_port) = tcp_listen_ipv6(0)
  let assert Ok(port_num) = inet_port(tcp_port)

  let sockets =
    socket.new()
    |> socket.host("::1")
    |> socket.ipv6(True)
    |> socket.port(port_num)
    |> socket.factory

  let assert Ok(_) = supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  sock
}

pub fn supervise(sockets: socket.Factory) {
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(socket.supervised(sockets))
  |> supervisor.start
}

pub fn connect_error_test() {
  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(1)
    |> socket.factory

  let assert Ok(_) = supervise(sockets)

  let assert Error(_) = socket.connect(sockets)
}

pub fn send_test() {
  let sock = connect()

  let assert Ok(_) = socket.send(sock, <<"bits":utf8>>)
  let assert Ok(_) = socket.shutdown(sock)
}

pub fn send_error_test() {
  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.with_send(fn(_, _) { Error(internal.Econnreset) })
    |> socket.factory

  let assert Ok(_) = supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  let assert Error(internal.SocketError(internal.Econnreset, "Failed to send")) =
    socket.send(sock, <<"bits":utf8>>)
}

pub fn receive_test() {
  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.with_receive(fn(_, _, _) { Ok(<<"bits":utf8>>) })
    |> socket.factory

  let assert Ok(_) = supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  let assert Ok(<<"bits":utf8>>) = socket.receive(sock, 0)

  let assert Ok(_) = socket.shutdown(sock)
}

pub fn receive_error_test() {
  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.with_receive(fn(_, _, _) { Error(internal.Closed) })
    |> socket.factory

  let assert Ok(_) = supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  let assert Error(internal.SocketError(internal.Closed, "Failed to receive")) =
    socket.receive(sock, 5)
}

@external(erlang, "pgl_ffi", "gen_tcp_listen")
fn tcp_listen(port: Int) -> Result(Port, internal.PosixError)

@external(erlang, "pgl_ffi", "gen_tcp_listen_ipv6")
fn tcp_listen_ipv6(port: Int) -> Result(Port, internal.PosixError)

@external(erlang, "inet", "port")
fn inet_port(port: Port) -> Result(Int, Nil)
