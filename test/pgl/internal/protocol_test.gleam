import gleam/option
import pgl/internal
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/socket
import pgl/internal/socket_test
import pgl/internal/type_cache

pub fn ssl_upgrade_unexpected_receive_test() {
  let conf =
    protocol.default_config
    |> protocol.ssl(option.Some(False))

  let sb =
    socket_test.with_mock_socket_builder(fn(sb) {
      socket.set_recv(sb, fn(_, _, _) { Ok(<<"X":utf8>>) })
    })

  let assert Error(internal.SocketError(
    kind: internal.SslError,
    message: "Failed to upgrade SSL",
  )) = protocol.auth(sb, conf)
}

pub fn protocol_test() {
  let conf =
    protocol.default_config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")

  let assert Ok(sock) =
    socket.tcp
    |> protocol.auth(conf)

  let assert Ok([[<<"1":utf8>>]]) =
    encode.query("SELECT 1")
    |> protocol.simple(sock)
}

pub fn auth_failure_test() {
  let assert Error(internal.PostgresError(err)) =
    socket.tcp
    |> protocol.auth(protocol.default_config)

  let assert "28000" = err.code
  let assert "invalid_authorization_specification" = err.name
  let assert "no PostgreSQL user name specified in startup packet" = err.message
}

pub fn protocol_bootstrap_test() {
  let conf =
    protocol.default_config
    |> protocol.database("gleam_pgl_test")
    |> protocol.username("postgres")
    |> protocol.password("postgres")

  let assert Ok(sock) =
    socket.tcp
    |> protocol.auth(conf)

  let assert Ok(_) =
    encode.query(type_cache.bootstrap_sql)
    |> protocol.simple(sock)
}
