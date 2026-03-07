import gleam/otp/static_supervisor as supervisor
import neon/net
import neon/tcp
import pgl/internal
import pgl/internal/socket

pub fn connect() {
  let assert Ok(port) = net.port(0)
  let assert Ok(localhost) = net.parse_ip_address("127.0.0.1")
  let assert Ok(socket) = tcp.listen(port, localhost)
  let assert Ok(port) = tcp.port(socket)

  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(net.port_to_int(port))
    |> socket.factory

  let assert Ok(_) = supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  sock
}

pub fn connect_ipv6_test() {
  let assert Ok(port) = net.port(0)
  let assert Ok(ipv6_localhost) = net.parse_ip_address("::1")
  let assert Ok(socket) = tcp.listen(port, ipv6_localhost)
  let assert Ok(port) = tcp.port(socket)

  let sockets =
    socket.new()
    |> socket.host("::1")
    |> socket.ipv6(True)
    |> socket.port(net.port_to_int(port))
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
