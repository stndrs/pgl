import gleam/option.{Some}
import pgl/internal
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/socket.{type Socket}
import pgl/internal/socket_test
import pgl/internal/type_cache

pub fn config_test() {
  let conf =
    protocol.config
    |> protocol.application("pgl")
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")
    |> protocol.connection_parameters([#("timezone", "MDT")])
    |> protocol.ssl(Some(True))

  assert "pgl" == conf.application
  assert "gleam_pgl_test" == conf.database
  assert "postgres" == conf.username
  assert "postgres" == conf.password
  assert [#("timezone", "MDT")] == conf.connection_parameters
  assert option.Some(True) == conf.ssl
}

pub fn protocol_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")
    |> protocol.ssl(Some(False))

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok([[option.Some(<<"1":utf8>>)]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
}

pub fn auth_failure_test() {
  let conf =
    protocol.config
    |> protocol.ssl(Some(False))

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
    |> protocol.ssl(Some(False))

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok(_) =
    encode.query(type_cache.bootstrap_sql)
    |> protocol.simple(sock)
}

pub fn auth_cleartext_password_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("cleartext_user")
    |> protocol.password("cleartext_pass")
    |> protocol.ssl(Some(False))

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok([[option.Some(<<"1":utf8>>)]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
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
