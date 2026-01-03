import db_pool
import exception
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor.{type Supervisor} as supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/uri.{type Uri}
import pg_value
import pg_value/type_info.{type TypeInfo}
import pgl/internal
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/query_cache.{type QueryCache}
import pgl/internal/socket.{type Socket}
import pgl/internal/type_cache.{type TypeCache}

// ---------- Config ---------- //

pub type Config {
  Config(
    application: String,
    host: String,
    port: Int,
    username: String,
    password: String,
    database: String,
    connection_parameters: List(#(String, String)),
    ssl: Ssl,
    rows_as_dict: Bool,
    // pool options
    pool_size: Int,
    idle_interval: Int,
    queue_target: Int,
  )
}

pub const default = Config(
  application: "",
  host: internal.default_host,
  port: internal.default_port,
  username: "",
  password: "",
  database: "",
  connection_parameters: [],
  ssl: SslDisabled,
  rows_as_dict: False,
  pool_size: 1,
  idle_interval: 1000,
  queue_target: 50,
)

pub type Ssl {
  SslDisabled
  SslVerified
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
  |> result.map(ssl(conf, _))
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
    internal.AuthenticationError(_, _) -> {
      err
      |> internal.error_to_string
      |> AuthenticationError
    }
    internal.SocketError(_, _) -> {
      err
      |> internal.error_to_string
      |> SocketError
    }
    internal.ProtocolError(_, _) -> {
      err
      |> internal.error_to_string
      |> ProtocolError
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

pub opaque type Db {
  Db(
    pool: process.Name(db_pool.Message(Connection, PglError)),
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

fn connect(db: Db) -> Result(Connection, PglError) {
  let Db(pool: _, sockets:, config:, type_cache:, query_cache:) = db

  authenticated_connection(config, sockets)
  |> result.map(fn(sock) {
    Connection(
      sock:,
      savepoint: None,
      type_cache:,
      query_cache:,
      rows_as_dict: config.rows_as_dict,
    )
  })
  |> result.map_error(fn(_) { ConnectionError("Failed to open connection") })
}

fn disconnect(conn: Connection) -> Result(Nil, PglError) {
  socket.shutdown(conn.sock)
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

  sockets
  |> socket.connect
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(sock) {
    protocol.auth(sock, conf)
    |> result.map_error(fn(_) { Nil })
  })
}

/// Checks out a connection passes it to the provided function. After the provided
/// function returns, the connection is checked back in.
///
/// Example:
///
/// ```gleam
/// pgl.with_connection(db, fn(conn) {
///   pgl.query(sql, [], conn)
/// })
/// ```
///
pub fn with_connection(db: Db, next: fn(Connection) -> t) -> Result(t, PglError) {
  let self = process.self()
  let pool = process.named_subject(db.pool)

  db_pool.checkout(pool, self, db.config.queue_target)
  |> result.map_error(pool_error_to_pgl_error)
  |> result.map(fn(conn) {
    let res = next(conn)

    db_pool.checkin(pool, conn, self)

    res
  })
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

fn ping(conn: Connection) -> Result(Connection, PglError) {
  protocol.ping(conn.sock)
  |> result.replace(conn)
  |> result.map_error(from_internal_error)
}

// ---------- Connection ---------- //

/// A single database connection
pub opaque type Connection {
  Connection(
    sock: Socket,
    savepoint: Option(Int),
    type_cache: TypeCache,
    query_cache: QueryCache,
    rows_as_dict: Bool,
  )
}

fn rows_to_maps(
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

// ---------- Query ---------- //

/// `Queried` captures the number of rows queried, the fields that
/// were queried, and rows returned by the query. The rows returned
/// need to be decoded.
pub type Queried {
  Queried(count: Int, fields: List(String), rows: List(Dynamic))
}

/// Holds a SQL string and the list of query parameters.
pub type Query {
  Query(sql: String, params: List(pg_value.Value))
}

/// Returns a `Query` with the provided SQL string.
pub fn sql(sql: String) -> Query {
  Query(sql:, params: [])
}

/// Sets the list of query parameters for the provided `Query`.
pub fn params(q: Query, params: List(pg_value.Value)) -> Query {
  Query(..q, params:)
}

/// Perform a query with the given SQL string and list of parameters.
pub fn query(
  sql: String,
  params: List(pg_value.Value),
  conn: Connection,
) -> Result(Queried, PglError) {
  extended_query(sql, params, conn)
  |> result.map_error(from_internal_error)
  |> result.try(to_queried(_, conn.rows_as_dict))
}

/// Uses [pipelining][1] to send multiple queries to the database server without
/// waiting for previous queries to complete. Reduces the number of network
/// roundtrips needed for multiple queries.
///
/// [1]: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING
/// 
pub fn batch(
  queries: List(Query),
  conn: Connection,
) -> Result(List(Queried), PglError) {
  let messages =
    queries
    |> list.try_map(fn(query) {
      let Query(sql, params) = query

      use oids <- result.try(query_cache.lookup(conn.query_cache, sql))
      use info <- result.map(type_cache.lookup(conn.type_cache, oids))

      encode.cached(sql, params, info, pg_value.encode)
    })
    |> result.lazy_unwrap(fn() {
      list.map(queries, fn(q) { encode.uncached(q.sql, q.params) })
    })

  let ext = extended(conn)

  protocol.pipeline()
  |> protocol.batch_process(ext, messages, conn.sock)
  |> result.map_error(from_internal_error)
  |> result.try(fn(exts) {
    use ext <- list.try_map(exts)

    to_queried(ext, conn.rows_as_dict)
  })
}

fn to_queried(
  ext: protocol.Extended(pg_value.Value),
  rows_as_dict: Bool,
) -> Result(Queried, PglError) {
  let values = list.reverse(ext.values)

  case rows_as_dict {
    True -> rows_to_maps(ext.fields, values)
    False -> Ok(list.map(values, dynamic.array))
  }
  |> result.map(Queried(count: ext.count, fields: ext.fields, rows: _))
  |> result.map_error(fn(_) { QueryError("Failed to process queried rows") })
}

/// Perform a query with the given SQL string. This function will send the
/// SQL string as is to the postgres database server.
pub fn execute(sql: String, on conn: Connection) -> Result(Int, PglError) {
  extended_query(sql, [], conn)
  |> result.map(fn(rows) { rows.count })
  |> result.map_error(from_internal_error)
}

fn extended_query(
  sql: String,
  params: List(pg_value.Value),
  conn: Connection,
) -> Result(protocol.Extended(pg_value.Value), internal.InternalError) {
  let message =
    encode_from_cache(sql, params, conn)
    |> result.lazy_unwrap(fn() { encode.uncached(sql, params) })

  extended(conn)
  |> protocol.process(message, conn.sock)
}

fn extended(conn: Connection) -> protocol.Extended(pg_value.Value) {
  protocol.extended()
  |> protocol.on_decode_row(fn(vals, oids) { decode_row(vals, oids, conn) })
  |> protocol.on_param_description(fn(sql, params, oids) {
    on_param_description(sql, params, oids, conn)
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
  conn: Connection,
) -> Result(encode.Query(pg_value.Value, TypeInfo), Nil) {
  use oids <- result.try(query_cache.lookup(conn.query_cache, sql))
  use info <- result.map(type_cache.lookup(conn.type_cache, oids))

  encode.cached(sql, params, info, pg_value.encode)
  |> encode.with_sync
}

fn on_param_description(
  sql: String,
  params: List(pg_value.Value),
  oids: List(Int),
  conn: Connection,
) -> Result(BitArray, Nil) {
  use _ <- result.try(query_cache.insert(conn.query_cache, sql, oids))
  use info <- result.try(type_cache.lookup(conn.type_cache, oids))

  encode.cached(sql, params, info, pg_value.encode)
  |> encode.with_sync
  |> encode.to_bit_array
  |> result.map_error(fn(_) { Nil })
}

fn decode_row(
  values: List(BitArray),
  oids: List(Int),
  conn: Connection,
) -> Result(List(Dynamic), internal.InternalError) {
  type_cache.lookup(conn.type_cache, oids)
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
  conn: Connection,
  next: fn(Connection) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  use tx <- result.try(begin(conn))

  exception.on_crash(fn() { rollback(tx) }, fn() { next(tx) })
  |> result.map_error(fn(err) {
    case rollback(tx) {
      Ok(_tx) -> RollbackError(err)
      Error(err) -> err
    }
  })
  |> result.try(fn(res) { commit(tx) |> result.replace(res) })
}

/// Begins a transaction
pub fn begin(conn: Connection) -> Result(Connection, TransactionError(error)) {
  let packet = encode.query("BEGIN")

  protocol.simple(packet, conn.sock)
  |> result.replace(conn)
  |> result.map_error(fn(err) {
    err
    |> internal.error_to_string
    |> TransactionError
  })
}

/// Commits a transaction
pub fn commit(conn: Connection) -> Result(Connection, TransactionError(error)) {
  let packet = encode.query("COMMIT")

  protocol.simple(packet, conn.sock)
  |> result.replace(conn)
  |> result.map_error(fn(err) {
    err
    |> internal.error_to_string
    |> TransactionError
  })
}

/// Rolls back a transaction
pub fn rollback(conn: Connection) -> Result(Connection, TransactionError(error)) {
  case conn.savepoint {
    Some(num) -> rollback_savepoint(num, conn)
    None -> {
      let packet = encode.query("ROLLBACK")

      protocol.simple(packet, conn.sock)
      |> result.replace(conn)
      |> result.map_error(fn(err) {
        err
        |> internal.error_to_string
        |> TransactionError
      })
    }
  }
}

/// Creates a new savepoint.
pub fn savepoint(
  conn: Connection,
  next: fn(Connection) -> Result(t, error),
) -> Result(t, TransactionError(error)) {
  use conn1 <- result.try(next_savepoint(conn))

  exception.on_crash(fn() { rollback_and_release(conn1) }, fn() { next(conn1) })
  |> result.map_error(fn(err) {
    case rollback_and_release(conn1) {
      Ok(_conn1) -> RollbackError(err)
      Error(err) -> err
    }
  })
  |> result.try(fn(res) { release_savepoint(conn1) |> result.replace(res) })
}

fn rollback_and_release(
  conn: Connection,
) -> Result(Connection, TransactionError(error)) {
  rollback(conn)
  |> result.try(release_savepoint)
}

const savepoint_name = "pgl_savepoint"

fn next_savepoint(
  conn: Connection,
) -> Result(Connection, TransactionError(error)) {
  let num = case conn.savepoint {
    Some(num) -> num
    None -> 0
  }

  let statement = "SAVEPOINT " <> savepoint_name <> int.to_string(num)
  let savepoint = num + 1

  let packet = encode.query(statement)

  protocol.simple(packet, conn.sock)
  |> result.map(fn(_) { set_savepoint(conn, savepoint) })
  |> result.map_error(fn(err) {
    err
    |> internal.error_to_string
    |> TransactionError
  })
}

fn set_savepoint(conn: Connection, savepoint: Int) -> Connection {
  Connection(..conn, savepoint: Some(savepoint))
}

/// Releases a savepoint.
pub fn release_savepoint(
  conn: Connection,
) -> Result(Connection, TransactionError(error)) {
  case conn.savepoint {
    Some(num) -> {
      let statement =
        "RELEASE SAVEPOINT " <> savepoint_name <> int.to_string(num - 1)

      let packet = encode.query(statement)

      protocol.simple(packet, conn.sock)
      |> result.replace(conn)
      |> result.map_error(fn(err) {
        err
        |> internal.error_to_string
        |> TransactionError
      })
    }
    None -> Error(NotInTransaction)
  }
}

fn rollback_savepoint(
  num: Int,
  conn: Connection,
) -> Result(Connection, TransactionError(error)) {
  let savepoint = num - 1
  let statement =
    "ROLLBACK TO SAVEPOINT "
    <> savepoint_name
    <> int.to_string(savepoint)
    <> ";"

  let packet = encode.query(statement)

  protocol.simple(packet, conn.sock)
  |> result.replace(conn)
  |> result.map_error(fn(err) {
    err
    |> internal.error_to_string
    |> TransactionError
  })
}
