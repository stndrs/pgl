import gleam/option.{Some}
import gleam/string
import pgl/internal
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/socket.{type Socket}
import pgl/internal/socket_test
import pgl/internal/type_cache

pub fn protocol_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")
    |> protocol.ssl(internal.SslUnverified)

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok([[Some(<<"1":utf8>>)]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
}

pub fn auth_failure_test() {
  let conf =
    protocol.config
    |> protocol.ssl(internal.SslUnverified)

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
    |> protocol.ssl(internal.SslUnverified)

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
    |> protocol.ssl(internal.SslUnverified)

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok([[Some(<<"1":utf8>>)]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
}

pub fn auth_md5_password_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("md5_user")
    |> protocol.password("md5_pass")
    |> protocol.ssl(internal.SslUnverified)

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok([[Some(<<"1":utf8>>)]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
}

pub fn auth_trust_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("trust_user")
    |> protocol.ssl(internal.SslUnverified)

  let sock = connect()
  let assert Ok(sock) = protocol.auth(sock, conf)

  let assert Ok([[Some(<<"1":utf8>>)]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
}

pub fn ssl_verified_rejects_self_signed_test() {
  let conf =
    protocol.config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")
    |> protocol.ssl(internal.SslVerified)

  let sock = connect()
  let assert Error(err) = protocol.auth(sock, conf)

  let assert internal.SocketError(internal.TlsAlert(alert), message) = err

  assert string.contains(alert, "bad_certificate")
  assert "Failed to connect SSL" == message
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
