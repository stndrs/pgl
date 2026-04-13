//// PostgreSQL database client

import db_pool
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process
import gleam/function
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor.{type Supervisor} as supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/uri.{type Uri}
import pg_value
import pg_value/type_info.{type TypeInfo}
import pgl/internal
import pgl/internal/conn
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/query_cache.{type QueryCache}
import pgl/internal/socket.{type Socket}
import pgl/internal/type_cache.{type TypeCache}

// ---------- Config ---------- //

pub type Config {
  Config(
    /// Application's name.
    application: String,
    /// (default: 127.0.0.1) Database server hostname.
    host: String,
    /// (default: 5432) Database server port.
    port: Int,
    /// Database username.
    username: String,
    /// Database user password.
    password: String,
    /// Database to use.
    database: String,
    /// Other Postgres connection parameters.
    connection_parameters: List(#(String, String)),
    /// (default: SslDisabled) SSL enabled or disabled.
    ssl: Ssl,
    /// (default: False) Return rows as `Dict` or n-tuple.
    rows_as_dict: Bool,
    /// (default: Ipv4) The IP version to use
    ip_version: IpVersion,
    /// (default: 1) Connection pool size.
    pool_size: Int,
    /// (default: 1000) Idle connections ping the database every `idle_interval`.
    idle_interval: Int,
    /// (default: 50) How long checking out a connection should take.
    queue_target: Int,
  )
}

/// A default configuration with a connection pool size of 1.
/// At minimum you need to set the username, password, and
/// database values.
pub const default = Config(
  application: "",
  host: internal.default_host,
  port: internal.default_port,
  username: "",
  password: "",
  database: "",
  connection_parameters: [],
  ssl: SslVerified,
  rows_as_dict: False,
  ip_version: Ipv4,
  pool_size: 5,
  idle_interval: 1000,
  queue_target: 50,
)

/// The IP version to use
pub type IpVersion {
  Ipv4
  Ipv6
}

pub type Ssl {
  /// Disables SSL leaving connections unsecured. Avoid using this in production.
  SslDisabled
  /// Enables SSL and checks the CA certificate.
  SslVerified
  /// Enables SSL but does not check the CA certificate.
  SslUnverified
}

/// Name of the application connecting to the database.
pub fn application(conf: Config, application: String) -> Config {
  Config(..conf, application:)
}

/// The database server hostname.
pub fn host(conf: Config, host: String) -> Config {
  Config(..conf, host:)
}

/// The port on which the database server is listening.
pub fn port(conf: Config, port: Int) -> Config {
  Config(..conf, port:)
}

/// The username to connect to the database as.
pub fn username(conf: Config, username: String) -> Config {
  Config(..conf, username:)
}

/// The password of the user.
pub fn password(conf: Config, password: String) -> Config {
  Config(..conf, password:)
}

/// The name of the database to use.
pub fn database(conf: Config, database: String) -> Config {
  Config(..conf, database:)
}

/// Sets other postgres connection parameters.
pub fn connection_parameter(
  conf: Config,
  name name: String,
  value value: String,
) -> Config {
  let connection_parameters =
    list.prepend(conf.connection_parameters, #(name, value))

  Config(..conf, connection_parameters:)
}

/// Whether SSL should be used.
pub fn ssl(conf: Config, ssl: Ssl) -> Config {
  Config(..conf, ssl:)
}

/// Configures rows to be returns as `Dict` rather than n-tuples.
pub fn rows_as_dict(conf: Config, rows_as_dict: Bool) -> Config {
  Config(..conf, rows_as_dict:)
}

/// Which IP version to use
pub fn ip_version(conf: Config, ip_version: IpVersion) -> Config {
  Config(..conf, ip_version:)
}

/// Sets the size of the connection pool.
pub fn pool_size(conf: Config, pool_size: Int) -> Config {
  Config(..conf, pool_size:)
}

/// How often idle connections should ping the database server.
pub fn idle_interval(conf: Config, idle_interval: Int) -> Config {
  Config(..conf, idle_interval:)
}

/// How long it should take to check out a connection from the connection pool.
pub fn queue_target(conf: Config, queue_target: Int) -> Config {
  Config(..conf, queue_target:)
}

/// Build a `Config` from a connection url
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
  uri.scheme
  |> option.map(fn(scheme) {
    case scheme {
      "postgres" | "postgresql" -> Ok(default)
      _ -> Error(Nil)
    }
  })
  |> option.lazy_unwrap(fn() { Error(Nil) })
}

fn apply_user_info(conf: Config, uri: Uri) -> Result(Config, Nil) {
  uri.userinfo
  |> option.map(fn(user_info) {
    case string.split(user_info, ":") {
      [user] -> Ok(username(conf, user))
      [user, pass] -> {
        conf
        |> username(user)
        |> password(pass)
        |> Ok
      }
      _ -> Error(Nil)
    }
  })
  |> option.unwrap(Error(Nil))
}

fn apply_host(conf: Config, uri: Uri) -> Result(Config, Nil) {
  case uri.host {
    Some(val) -> Ok(host(conf, val))
    None -> Error(Nil)
  }
}

fn apply_port(conf: Config, uri: Uri) -> Result(Config, Nil) {
  uri.port
  |> option.map(port(conf, _))
  |> option.unwrap(conf)
  |> Ok
}

fn apply_database(conf: Config, uri: Uri) -> Result(Config, Nil) {
  case string.split(uri.path, "/") {
    ["", db] -> Ok(database(conf, db))
    _ -> Error(Nil)
  }
}

fn apply_ssl_mode(conf: Config, uri: Uri) -> Result(Config, Nil) {
  case uri.query {
    None -> Ok(conf)
    Some(query) -> {
      use query <- result.try(uri.parse_query(query))
      case list.key_find(query, "sslmode") {
        Error(Nil) -> Ok(conf)
        Ok(sslmode) ->
          case sslmode {
            "require" | "prefer" | "allow" -> Ok(ssl(conf, SslUnverified))
            "verify-ca" | "verify-full" -> Ok(ssl(conf, SslVerified))
            "disable" -> Ok(ssl(conf, SslDisabled))
            _ -> Error(Nil)
          }
      }
    }
  }
}

pub type PglError {
  QueryError(message: String)
  ConnectionError(message: String)
  ConnectionTimeout
  AuthenticationError(message: String)
  ProtocolError(message: String)
  SocketError(message: String)
  PostgresError(
    code: String,
    name: String,
    message: String,
    fields: Dict(Field, String),
  )
}

/// Returns a formatted string representation of the provided error.
pub fn error_to_string(err: PglError) -> String {
  case err {
    QueryError(message) -> internal.format_error("QueryError", message)
    ConnectionError(message) ->
      internal.format_error("ConnectionError", message)
    ConnectionTimeout -> internal.format_error("ConnectionTimeout", "")
    AuthenticationError(message) ->
      internal.format_error("AuthenticationError", message)
    ProtocolError(message) -> internal.format_error("ProtocolError", message)
    SocketError(message) -> internal.format_error("SocketError", message)
    PostgresError(code:, name:, message:, fields: _) -> {
      internal.format_error_with_values(
        "PostgresError",
        "",
        [#("code", code), #("name", name), #("message", message)],
        function.identity,
      )
    }
  }
}

// https://www.postgresql.org/docs/current/protocol-error-fields.html
/// Error and notice message fields
pub type Field {
  Severity
  Code
  Message
  Detail
  Hint
  Position
  InternalPosition
  InternalQuery
  Where
  Schema
  Table
  Column
  DataType
  Constraint
  File
  Line
  Routine
  Other(BitArray)
}

fn from_internal_error(err: internal.InternalError) -> PglError {
  case err {
    internal.PostgresError(code:, name:, message:, fields:) -> {
      let fields =
        {
          use #(field, val) <- list.map(dict.to_list(fields))
          let field1 = field_from_bit_array(field)
          #(field1, val)
        }
        |> dict.from_list

      PostgresError(code:, name:, message:, fields:)
    }
    internal.AuthenticationError(kind, message) -> {
      let message =
        "[" <> internal.auth_error_to_string(kind) <> "] " <> message

      AuthenticationError(message)
    }
    internal.SocketError(code, message) -> {
      let message =
        "[" <> internal.posix_error_to_string(code) <> "] " <> message

      SocketError(message)
    }
    internal.ProtocolError(kind, message) -> {
      let message =
        "[" <> internal.protocol_error_to_string(kind) <> "] " <> message

      ProtocolError(message)
    }
  }
}

fn field_from_bit_array(field_type: BitArray) -> Field {
  case field_type {
    <<"S":utf8>> -> Severity
    <<"C":utf8>> -> Code
    <<"M":utf8>> -> Message
    <<"D":utf8>> -> Detail
    <<"H":utf8>> -> Hint
    <<"P":utf8>> -> Position
    <<"p":utf8>> -> InternalPosition
    <<"q":utf8>> -> InternalQuery
    <<"W":utf8>> -> Where
    <<"s":utf8>> -> Schema
    <<"t":utf8>> -> Table
    <<"c":utf8>> -> Column
    <<"d":utf8>> -> DataType
    <<"n":utf8>> -> Constraint
    <<"F":utf8>> -> File
    <<"L":utf8>> -> Line
    <<"R":utf8>> -> Routine
    bits -> Other(bits)
  }
}

pub type TransactionError(error) {
  RollbackError(cause: error)
  NotInTransaction
  TransactionError(message: String)
}

/// A configured `Db`. Must be started via `pgl.start` or `pgl.supervised`
/// before use. Once started, `Db` can be passed to `with_connection`.
pub opaque type Db {
  Db(
    pool: process.Name(db_pool.Message(Socket, PglError)),
    sockets: socket.Factory,
    type_cache: TypeCache,
    query_cache: QueryCache,
    config: Config,
  )
}

/// Creates a new `Db` record. This must be passed to `pgl.start` or
/// `pgl.supervised` before it can be used.
pub fn new(config: Config) -> Db {
  let sockets =
    socket.new()
    |> socket.host(config.host)
    |> socket.port(config.port)
    |> socket.ipv6({
      case config.ip_version {
        Ipv4 -> False
        Ipv6 -> True
      }
    })
    |> socket.factory

  let pool = process.new_name("pgl_pool")
  let query_cache = query_cache.new()

  let type_cache =
    type_cache.new()
    |> type_cache.on_connect(fn() { authenticated_connection(config, sockets) })

  Db(pool:, sockets:, type_cache:, query_cache:, config:)
}

pub fn start(db: Db) -> actor.StartResult(Supervisor) {
  let pool =
    db_pool.new()
    |> db_pool.size(db.config.pool_size)
    |> db_pool.interval(db.config.idle_interval)
    |> db_pool.on_open(fn() { connect(db) })
    |> db_pool.on_close(disconnect)
    |> db_pool.on_interval(fn(conn) {
      let _ = ping(conn)

      Nil
    })

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(type_cache.supervised(db.type_cache))
  |> supervisor.add(query_cache.supervised(db.query_cache))
  |> supervisor.add(socket.supervised(db.sockets))
  |> supervisor.add(db_pool.supervised(pool, db.pool, 1000))
  |> supervisor.start
}

pub fn supervised(db: Db) -> supervision.ChildSpecification(Supervisor) {
  let pool_supervisor =
    supervision.supervisor(fn() { start(db) })
    |> supervision.restart(supervision.Transient)

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(pool_supervisor)
  |> supervisor.supervised
}

fn connect(db: Db) -> Result(Socket, PglError) {
  authenticated_connection(db.config, db.sockets)
  |> result.map_error(fn(_) { ConnectionError("Failed to open connection") })
}

fn disconnect(sock: Socket) -> Result(Nil, PglError) {
  socket.shutdown(sock)
  |> result.map_error(from_internal_error)
}

fn authenticated_connection(
  config: Config,
  sockets: socket.Factory,
) -> Result(Socket, Nil) {
  let conf =
    protocol.config
    |> protocol.application(config.application)
    |> protocol.connection_parameters(config.connection_parameters)
    |> protocol.username(config.username)
    |> protocol.password(config.password)
    |> protocol.database(config.database)
    |> protocol.ssl(case config.ssl {
      SslDisabled -> None
      SslVerified -> Some(True)
      SslUnverified -> Some(False)
    })

  sockets
  |> socket.connect
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(sock) {
    protocol.auth(sock, conf)
    |> result.map_error(fn(_) { Nil })
  })
}

/// Creates a `Connection`.
pub fn connection(db: Db) -> Connection {
  Pool(db:)
}

fn pool_error_to_pgl_error(err: db_pool.PoolError(PglError)) -> PglError {
  case err {
    db_pool.ConnectionError(err) -> err
    db_pool.ConnectionTimeout -> ConnectionTimeout
  }
}

/// Shuts down the `Db`
pub fn shutdown(db: Db) -> Result(Nil, PglError) {
  db.pool
  |> process.named_subject
  |> db_pool.shutdown(1000)
  |> result.map_error(pool_error_to_pgl_error)
}

fn ping(sock: Socket) -> Result(Socket, PglError) {
  protocol.ping(sock)
  |> result.map_error(from_internal_error)
}

// ---------- Connection ---------- //

/// A `Connection` that can be reference to the connection pool, or a
/// single connection.
pub opaque type Connection {
  Pool(db: Db)
  Connection(conn: conn.Conn, db: Db)
}

fn with_single_connection(
  connection: Connection,
  next: fn(conn.Conn, Db) -> Result(t, PglError),
) -> Result(t, PglError) {
  case connection {
    Pool(db:) -> {
      let self = process.self()

      use conn <- result.try(checkout(db, self))

      let res = next(conn, db)

      checkin(db, conn.sock, self)

      res
    }
    Connection(conn:, db:) -> next(conn, db)
  }
}

fn checkout(db: Db, self: process.Pid) -> Result(conn.Conn, PglError) {
  db.pool
  |> process.named_subject
  |> db_pool.checkout(self, db.config.queue_target)
  |> result.map(conn.new(_, self))
  |> result.map_error(pool_error_to_pgl_error)
}

fn checkin(db: Db, sock: Socket, self: process.Pid) -> Nil {
  db.pool
  |> process.named_subject
  |> db_pool.checkin(sock, self)
}

// ---------- Query ---------- //

/// `Queried` captures the number of rows queried, the fields that
/// were queried, and rows returned by the query. The rows returned
/// need to be decoded.
pub type Queried {
  Queried(count: Int, fields: List(String), rows: List(Dynamic))
}

/// Holds a SQL string and a list of query parameters.
pub type Query {
  Query(sql: String, params: List(pg_value.Value))
}

/// Returns a `Query` with the provided SQL string.
pub fn sql(sql: String) -> Query {
  Query(sql:, params: [])
}

/// Sets a list of query parameters for the provided `Query`.
pub fn params(q: Query, params: List(pg_value.Value)) -> Query {
  Query(..q, params:)
}

/// Perform a query with the given SQL string and list of parameters.
pub fn query(q: Query, connection: Connection) -> Result(Queried, PglError) {
  use conn, db <- with_single_connection(connection)

  extended_query(q.sql, q.params, conn, db)
  |> result.map_error(from_internal_error)
  |> result.try(to_queried(_, db.config.rows_as_dict))
}

/// Uses [pipelining][1] to send multiple queries to the database server without
/// waiting for previous queries to complete. Reduces the number of network
/// roundtrips needed for multiple queries.
///
/// [1]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING
/// 
pub fn batch(
  queries: List(Query),
  connection: Connection,
) -> Result(List(Queried), PglError) {
  use conn, db <- with_single_connection(connection)

  let messages =
    queries
    |> list.try_map(fn(query) {
      let Query(sql, params) = query

      use oids <- result.try(query_cache.lookup(db.query_cache, sql))
      use info <- result.map(type_cache.lookup(db.type_cache, oids))

      encode.cached(sql, params, info, pg_value.encode)
    })
    |> result.lazy_unwrap(fn() {
      list.map(queries, fn(q) { encode.uncached(q.sql, q.params) })
    })

  let ext = extended(db)

  protocol.pipeline()
  |> protocol.batch_process(ext, messages, conn.sock)
  |> result.map_error(from_internal_error)
  |> result.try(fn(exts) {
    use ext <- list.try_map(exts)

    to_queried(ext, db.config.rows_as_dict)
  })
}

fn to_queried(
  ext: protocol.Extended(pg_value.Value),
  rows_as_dict: Bool,
) -> Result(Queried, PglError) {
  let values = list.reverse(ext.values)

  case rows_as_dict {
    True -> rows_to_dicts(ext.fields, values)
    False -> Ok(list.map(values, dynamic.array))
  }
  |> result.map(Queried(count: ext.count, fields: ext.fields, rows: _))
  |> result.map_error(fn(_) { QueryError("Failed to process queried rows") })
}

fn rows_to_dicts(
  fields: List(String),
  values: List(List(Dynamic)),
) -> Result(List(Dynamic), Nil) {
  use rows <- result.map(list.try_map(values, list.strict_zip(fields, _)))

  {
    use row <- list.map(rows)
    use #(col, val) <- list.map(row)

    #(dynamic.string(col), val)
  }
  |> list.map(dynamic.properties)
}

/// Perform a query with the given SQL string. This function will send the
/// SQL string as is to the postgres database server.
pub fn execute(sql: String, on connection: Connection) -> Result(Int, PglError) {
  use conn, db <- with_single_connection(connection)

  extended_query(sql, [], conn, db)
  |> result.map(fn(rows) { rows.count })
  |> result.map_error(from_internal_error)
}

fn extended_query(
  sql: String,
  params: List(pg_value.Value),
  conn: conn.Conn,
  db: Db,
) -> Result(protocol.Extended(pg_value.Value), internal.InternalError) {
  let message =
    encode_from_cache(sql, params, db)
    |> result.lazy_unwrap(fn() { encode.uncached(sql, params) })

  extended(db)
  |> protocol.process(message, conn.sock)
}

fn extended(db: Db) -> protocol.Extended(pg_value.Value) {
  protocol.extended()
  |> protocol.on_decode_row(fn(vals, oids) { decode_row(vals, oids, db) })
  |> protocol.on_param_description(fn(sql, params, oids) {
    on_param_description(sql, params, oids, db)
    |> result.map_error(fn(_) {
      internal.ProtocolError(
        kind: internal.ProcessingError,
        message: "Failed to describe statement parameters",
      )
    })
  })
}

fn encode_from_cache(
  sql: String,
  params: List(pg_value.Value),
  db: Db,
) -> Result(encode.Query(pg_value.Value, TypeInfo), Nil) {
  use oids <- result.try(query_cache.lookup(db.query_cache, sql))
  use info <- result.map(type_cache.lookup(db.type_cache, oids))

  encode.cached(sql, params, info, pg_value.encode)
  |> encode.with_sync
}

fn on_param_description(
  sql: String,
  params: List(pg_value.Value),
  oids: List(Int),
  db: Db,
) -> Result(BitArray, Nil) {
  use _ <- result.try(query_cache.insert(db.query_cache, sql, oids))
  use info <- result.try(type_cache.lookup(db.type_cache, oids))

  encode.cached(sql, params, info, pg_value.encode)
  |> encode.with_sync
  |> encode.to_bit_array
  |> result.map_error(fn(_) { Nil })
}

fn decode_row(
  values: List(BitArray),
  oids: List(Int),
  db: Db,
) -> Result(List(Dynamic), internal.InternalError) {
  type_cache.lookup(db.type_cache, oids)
  |> result.map_error(fn(_) {
    internal.ProtocolError(
      kind: internal.DecodingError,
      message: "Failed to decode row",
    )
  })
  |> result.try(decode_row_values(values, _))
}

fn decode_row_values(
  values: List(BitArray),
  infos: List(TypeInfo),
) -> Result(List(Dynamic), internal.InternalError) {
  list.strict_zip(values, infos)
  |> result.map_error(fn(_) {
    internal.DecodingError
    |> internal.ProtocolError(message: "Mismatched values and infos")
  })
  |> result.try(fn(vals_infos) {
    list.try_map(vals_infos, fn(val_info) {
      let #(val, info) = val_info

      pg_value.decode(val, info)
      |> result.map_error(internal.ProtocolError(internal.DecodingError, _))
    })
  })
}

/// Starts a transaction and then calls the provided function, passing it the
/// transaction connection. If the given function throws an exception, the
/// transaction will be rolled back the exception propagated up.
/// If the given function returns an error result, the transaction will also
/// be rolled back and an error result returned.
pub fn transaction(
  connection: Connection,
  next: fn(Connection) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  use conn, db <- with_transaction(connection)

  let res = do_transaction(conn, fn(conn) { next(Connection(conn:, db:)) })

  checkin(db, conn.sock, conn.caller)

  res
}

fn do_transaction(
  conn: conn.Conn,
  next: fn(conn.Conn) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  use conn <- result.try(transaction_query("BEGIN", conn))

  internal.assert_on_crash(
    fn() { transaction_query("ROLLBACK", conn) },
    "ROLLBACK",
    fn() { next(conn) },
  )
  |> result.map_error(fn(err) {
    case transaction_query("ROLLBACK", conn) {
      Ok(_) -> RollbackError(err)
      Error(err) -> err
    }
  })
  |> result.try(fn(res) {
    transaction_query("COMMIT", conn)
    |> result.replace(res)
  })
}

/// Begins a transaction.
///
/// You should typically use `pgl.transaction` to take care of starting and
/// ending transactions.
pub fn begin(
  connection: Connection,
) -> Result(Connection, TransactionError(error)) {
  use conn, db <- with_transaction(connection)

  transaction_query("BEGIN", conn)
  |> result.map(Connection(_, db:))
}

// Checks out a connection but does not check it back in automatically.
// Must call `commit` or `rollback` to check the connection back in.
fn with_transaction(
  connection: Connection,
  next: fn(conn.Conn, Db) -> Result(t, TransactionError(error)),
) -> Result(t, TransactionError(error)) {
  case connection {
    Pool(db:) -> {
      let self = process.self()

      checkout(db, self)
      |> result.map_error(fn(pgl_error) {
        pgl_error
        |> error_to_string
        |> TransactionError
      })
      |> result.try(next(_, db))
    }
    Connection(conn:, db:) -> next(conn, db)
  }
}

/// Commits a transaction.
///
/// You should typically use `pgl.transaction` to take care of starting and
/// ending transactions.
pub fn commit(
  connection: Connection,
) -> Result(Connection, TransactionError(error)) {
  case connection {
    Pool(..) -> Error(NotInTransaction)
    Connection(conn:, db:) -> {
      transaction_query("COMMIT", conn)
      |> result.map(fn(_) {
        checkin(db, conn.sock, conn.caller)

        Pool(db:)
      })
    }
  }
}

/// Rolls back to a savepoint if one exists, otherwise rolls back the transaction.
///
/// You should typically use `pgl.transaction` to take care of starting and
/// ending transactions.
pub fn rollback(
  connection: Connection,
) -> Result(Connection, TransactionError(error)) {
  case connection {
    Pool(..) -> Error(NotInTransaction)
    Connection(conn:, db:) -> {
      do_rollback(conn, db)
      |> result.map(fn(_) { Pool(db:) })
    }
  }
}

fn do_rollback(
  conn: conn.Conn,
  db: Db,
) -> Result(conn.Conn, TransactionError(error)) {
  case conn.rollback_savepoint_statement(conn) {
    Ok(stmt) -> {
      transaction_query(stmt, conn)
      |> result.replace(conn)
    }
    _ -> {
      transaction_query("ROLLBACK", conn)
      |> result.map(fn(_) {
        checkin(db, conn.sock, conn.caller)

        conn
      })
    }
  }
}

/// Creates a new savepoint.
pub fn savepoint(
  connection: Connection,
  next: fn(Connection) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  case connection {
    Pool(..) -> Error(NotInTransaction)
    Connection(conn:, db:) -> {
      use conn <- do_savepoint(conn, db)

      Connection(conn:, db:) |> next
    }
  }
}

fn do_savepoint(
  conn: conn.Conn,
  db: Db,
  next: fn(conn.Conn) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  let conn = conn.next_savepoint(conn)

  let rollback_and_release = fn(conn: conn.Conn) {
    do_rollback(conn, db)
    |> result.try(do_release_savepoint)
  }

  conn.savepoint_statement(conn)
  |> transaction_query(conn)
  |> result.try(fn(conn) {
    internal.assert_on_crash(
      fn() { rollback_and_release(conn) },
      "rollback and release savepoint",
      fn() { next(conn) },
    )
    |> result.map_error(fn(err) {
      case rollback_and_release(conn) {
        Ok(_) -> RollbackError(err)
        Error(err) -> err
      }
    })
  })
  |> result.try(fn(res) {
    do_release_savepoint(conn)
    |> result.replace(res)
  })
}

fn do_release_savepoint(
  conn: conn.Conn,
) -> Result(conn.Conn, TransactionError(error)) {
  conn.release_savepoint_statement(conn)
  |> result.replace_error(NotInTransaction)
  |> result.try(fn(stmt) {
    transaction_query(stmt, conn)
    |> result.map(conn.prev_savepoint)
  })
}

fn transaction_query(
  query: String,
  conn: conn.Conn,
) -> Result(conn.Conn, TransactionError(error)) {
  let packet = encode.query(query)

  protocol.simple(packet, conn.sock)
  |> result.replace(conn)
  |> result.map_error(fn(err) {
    err
    |> internal.error_to_string
    |> TransactionError
  })
}
