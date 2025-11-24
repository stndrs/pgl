import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/uri.{type Uri}

pub type Config {
  Config(
    application: String,
    host: String,
    port: Int,
    user: String,
    password: String,
    database: String,
    timeout: Int,
    ping_timeout: Int,
    send_timeout: Int,
    recv_timeout: Int,
    ssl: Ssl,
    rows_as_maps: Bool,
    // pool config
    connect_timeout: Int,
    pool_size: Int,
  )
}

pub const default_port = 5432

pub const default = Config(
  application: "",
  host: "127.0.0.1",
  port: default_port,
  user: "",
  password: "",
  database: "",
  timeout: 5000,
  ping_timeout: 1000,
  send_timeout: 5000,
  recv_timeout: 5000,
  ssl: SslDisabled,
  rows_as_maps: False,
  connect_timeout: 500,
  pool_size: 1,
)

pub type Ssl {
  SslDisabled
  SslVerified
  SslUnverified
}

pub fn set_application(conf: Config, application: String) -> Config {
  Config(..conf, application:)
}

pub fn set_host(conf: Config, host: String) -> Config {
  Config(..conf, host:)
}

pub fn set_port(conf: Config, port: Int) -> Config {
  Config(..conf, port:)
}

pub fn set_username(conf: Config, user: String) -> Config {
  Config(..conf, user:)
}

pub fn set_password(conf: Config, password: String) -> Config {
  Config(..conf, password:)
}

pub fn set_database(conf: Config, database: String) -> Config {
  Config(..conf, database:)
}

pub fn set_timeout(conf: Config, timeout: Int) -> Config {
  Config(..conf, timeout:)
}

pub fn set_ping_timeout(conf: Config, ping_timeout: Int) -> Config {
  Config(..conf, ping_timeout:)
}

pub fn set_send_timeout(conf: Config, send_timeout: Int) -> Config {
  Config(..conf, send_timeout:)
}

pub fn set_recv_timeout(conf: Config, recv_timeout: Int) -> Config {
  Config(..conf, recv_timeout:)
}

pub fn set_ssl(conf: Config, ssl: Ssl) -> Config {
  Config(..conf, ssl:)
}

pub fn set_rows_as_maps(conf: Config, rows_as_maps: Bool) -> Config {
  Config(..conf, rows_as_maps:)
}

pub fn set_pool_size(conf: Config, pool_size: Int) -> Config {
  Config(..conf, pool_size:)
}

pub fn from_url(url: String) -> Result(Config, Nil) {
  use uri <- result.try(uri.parse(url))
  use conf <- result.try(options_from_uri(uri))
  use conf <- result.try(apply_user_info(conf, uri))
  use conf <- result.try(apply_host(conf, uri))
  use conf <- result.try(apply_port(conf, uri))
  use conf <- result.try(apply_database(conf, uri))

  apply_ssl_mode(conf, uri)
}

fn options_from_uri(uri: Uri) -> Result(Config, Nil) {
  use scheme <- try_option(uri.scheme)

  case scheme {
    "postgres" | "postgresql" -> Ok(default)
    _ -> Error(Nil)
  }
}

fn apply_user_info(conf: Config, uri: Uri) -> Result(Config, Nil) {
  use user_info <- try_option(uri.userinfo)

  case string.split(user_info, ":") {
    [username] -> Ok(set_username(conf, username))
    [username, password] -> {
      set_username(conf, username)
      |> set_password(password)
      |> Ok
    }
    _ -> Error(Nil)
  }
}

fn apply_host(conf: Config, uri: Uri) -> Result(Config, Nil) {
  use host <- try_option(uri.host)

  Ok(set_host(conf, host))
}

fn apply_port(conf: Config, uri: Uri) -> Result(Config, Nil) {
  case uri.port {
    Some(port) -> set_port(conf, port)
    None -> conf
  }
  |> Ok
}

fn apply_database(conf: Config, uri: Uri) -> Result(Config, Nil) {
  case string.split(uri.path, "/") {
    ["", database] -> Ok(set_database(conf, database))
    _ -> Error(Nil)
  }
}

fn apply_ssl_mode(conf: Config, uri: Uri) -> Result(Config, Nil) {
  case uri.query {
    None -> Ok(SslDisabled)
    Some(query) -> {
      use query <- result.try(uri.parse_query(query))
      use sslmode <- result.try(list.key_find(query, "sslmode"))

      case sslmode {
        "require" -> Ok(SslUnverified)
        "verify-ca" | "verify-full" -> Ok(SslVerified)
        "disable" -> Ok(SslDisabled)
        _ -> Error(Nil)
      }
    }
  }
  |> result.map(set_ssl(conf, _))
}

fn try_option(maybe: Option(a), next: fn(a) -> Result(b, Nil)) -> Result(b, Nil) {
  maybe
  |> option.to_result(Nil)
  |> result.try(next)
}
