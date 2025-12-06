import gleam/erlang/port.{type Port}
import pgl/internal
import pgl/internal/socket

pub fn connect_test() {
  let assert Ok(tcp_port) = tcp_listen(0)
  let assert Ok(port_num) = inet_port(tcp_port)
  let assert Ok(sock) = socket.connect("127.0.0.1", port_num)

  sock
}

pub fn connect_error_test() {
  let assert Error(internal.SocketError(
    internal.ConnectError(_posix),
    "Failed to connect",
  )) = socket.connect("127.0.0.1", 1)
}

pub fn send_test() {
  let sock = connect_test()

  let assert Ok(_) = socket.send(sock, <<"bits":utf8>>)

  let assert Ok(_) = socket.shutdown(sock)
}

pub fn send_error_test() {
  let sock = connect_test()

  let assert Ok(_) = socket.shutdown(sock)

  let assert Error(internal.SocketError(
    internal.SendError(_posix),
    "Failed to send",
  )) = socket.send(sock, <<"bits":utf8>>)
}

pub fn receive_test() {
  let sock =
    connect_test()
    |> socket.with_receive(fn(_, _, _) { Ok(<<"bits":utf8>>) })

  let assert Ok(<<"bits":utf8>>) = socket.receive(sock, 0)

  let assert Ok(_) = socket.shutdown(sock)
}

pub fn receive_error_test() {
  let sock =
    connect_test()
    |> socket.with_receive(fn(_, _, _) {
      Error(internal.SocketError(
        internal.ReceiveError(internal.Econnrefused),
        "Failed to receive",
      ))
    })

  let assert Error(internal.SocketError(
    internal.ReceiveError(_posix),
    "Failed to receive",
  )) = socket.receive(sock, 5)
}

@external(erlang, "pgl_ffi", "gen_tcp_listen")
fn tcp_listen(port: Int) -> Result(Port, internal.PosixError)

@external(erlang, "inet", "port")
fn inet_port(port: Port) -> Result(Int, Nil)
