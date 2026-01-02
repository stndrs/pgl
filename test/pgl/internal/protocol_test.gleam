import gleam/option
import pgl/internal
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/socket.{type Socket}
import pgl/internal/socket_test
import pgl/internal/type_cache

pub fn ssl_upgrade_unexpected_receive_test() {
  let conf =
    protocol.config
    |> protocol.ssl(option.Some(False))

  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.with_receive(fn(_, _, _) { Ok(<<"X":utf8>>) })
    |> socket.factory

  let assert Ok(_) = socket_test.supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  let assert Error(internal.ProtocolError(
    kind: internal.SslError,
    message: "Failed to upgrade SSL",
  )) = protocol.auth(sock, conf)
}

pub fn protocol_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok([[<<"1":utf8>>]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
}

pub fn auth_failure_test() {
  let conf = protocol.config

  let sock = connect()

  let assert Error(internal.PostgresError(code:, name:, message:, fields: _)) =
    protocol.auth(sock, conf)

  let assert "28000" = code
  let assert "invalid_authorization_specification" = name
  let assert "no PostgreSQL user name specified in startup packet" = message
}

pub fn protocol_bootstrap_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok(_) =
    encode.query(type_cache.bootstrap_sql)
    |> protocol.simple(sock)
}

pub fn ping_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok(_) = protocol.ping(sock)
}

fn connect() -> Socket {
  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.factory

  let assert Ok(_) = socket_test.supervise(sockets)
  let assert Ok(sock) = socket.connect(sockets)

  sock
}
