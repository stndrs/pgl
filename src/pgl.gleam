import db/pool
import exception
import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid}
import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor.{type Supervisor} as supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import pgl/config.{type Config}
import pgl/internal
import pgl/internal/decode
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/query_cache.{type QueryCache}
import pgl/internal/socket.{type Socket}
import pgl/internal/type_cache.{type TypeCache}
import pgl/types

pub type Value {
  Null
  Bool(Bool)
  Int(Int)
  Float(Float)
  Text(String)
  Bytea(BitArray)
  Time(calendar.TimeOfDay)
  Date(calendar.Date)
  Timestamp(timestamp.Timestamp)
  Interval(duration.Duration)
  Array(List(Value))
}

pub fn value_to_string(val: Value) -> String {
  case val {
    Null -> "NULL"
    Bool(val) -> bool_to_string(val)
    Int(val) -> int.to_string(val)
    Float(val) -> float.to_string(val)
    Text(val) -> text_to_string(val)
    Bytea(val) -> bytea_to_string(val)
    Time(val) -> time_to_string(val)
    Date(val) -> date_to_string(val)
    Timestamp(val) -> timestamp_to_string(val)
    Interval(val) -> duration_to_string(val)
    Array(vals) -> array_to_string(vals)
  }
}

fn text_to_string(val: String) -> String {
  let val = string.replace(in: val, each: "'", with: "\\'")

  single_quote(val)
}

// https://www.postgresql.org/docs/current/arrays.html#ARRAYS-INPUT
fn array_to_string(val: List(Value)) -> String {
  let elems = case val {
    [] -> ""
    [val] -> value_to_string(val)
    vals -> {
      vals
      |> list.map(value_to_string)
      |> string.join(", ")
    }
  }

  "ARRAY[" <> elems <> "]"
}

// https://www.postgresql.org/docs/current/datatype-boolean.html#DATATYPE-BOOLEAN
fn bool_to_string(val: Bool) -> String {
  case val {
    True -> "TRUE"
    False -> "FALSE"
  }
}

// https://www.postgresql.org/docs/current/datatype-binary.html#DATATYPE-BINARY-BYTEA-HEX-FORMAT
fn bytea_to_string(val: BitArray) -> String {
  let val = "\\x" <> bit_array.base16_encode(val)

  single_quote(val)
}

fn date_to_string(date: calendar.Date) -> String {
  let year = int.to_string(date.year)
  let month = calendar.month_to_int(date.month) |> pad_zero
  let day = pad_zero(date.day)

  let date = year <> "-" <> month <> "-" <> day

  single_quote(date)
}

fn time_to_string(tod: calendar.TimeOfDay) -> String {
  let hours = pad_zero(tod.hours)
  let minutes = pad_zero(tod.minutes)
  let seconds = pad_zero(tod.seconds)
  let milliseconds = tod.nanoseconds / 1_000_000

  let msecs = case milliseconds < 100 {
    True if milliseconds == 0 -> ""
    True if milliseconds < 10 -> ".00" <> int.to_string(milliseconds)
    True -> ".0" <> int.to_string(milliseconds)
    False -> "." <> int.to_string(milliseconds)
  }

  let time = hours <> ":" <> minutes <> ":" <> seconds <> msecs

  single_quote(time)
}

fn timestamp_to_string(ts: Timestamp) -> String {
  timestamp.to_rfc3339(ts, calendar.utc_offset)
  |> single_quote
}

fn duration_to_string(dur: Duration) -> String {
  duration.to_iso8601_string(dur)
  |> single_quote
}

fn single_quote(val: String) -> String {
  "'" <> val <> "'"
}

fn pad_zero(n: Int) -> String {
  case n < 10 {
    True -> "0" <> int.to_string(n)
    False -> int.to_string(n)
  }
}

pub type PglError {
  PglError(message: String)
  PostgresError(
    code: String,
    name: String,
    message: String,
    fields: Dict(String, String),
  )
}

fn from_internal_error(err: internal.PglError) -> PglError {
  case err {
    internal.PglError(message:) -> PglError(message:)
    internal.PostgresError(internal.PgError(code:, name:, message:, fields:)) -> {
      let fields =
        fields
        |> dict.to_list
        |> list.map(fn(field_value) {
          let #(field, value) = field_value
          let field = internal.field_to_string(field)
          #(field, value)
        })
        |> dict.from_list

      PostgresError(code:, name:, message:, fields:)
    }
    err -> PglError(internal.error_to_string(err))
  }
}

pub type TransactionError(error) {
  RollbackError(cause: error)
  NotInTransaction(message: String)
  FailedTransaction(message: String, cause: PglError)
  TransactionError
}

// ---------- Values ---------- //

pub const null = Null

pub fn bool(val: Bool) -> Value {
  Bool(val)
}

pub fn int(val: Int) -> Value {
  Int(val)
}

pub fn float(val: Float) -> Value {
  Float(val)
}

pub fn text(val: String) -> Value {
  Text(val)
}

pub fn bytea(val: BitArray) -> Value {
  Bytea(val)
}

pub fn time(val: calendar.TimeOfDay) -> Value {
  Time(val)
}

pub fn date(val: calendar.Date) -> Value {
  Date(val)
}

pub fn timestamp(ts: Timestamp) -> Value {
  Timestamp(ts)
}

pub fn interval(val: Duration) -> Value {
  Interval(val)
}

pub fn array(vals: List(a), of kind: fn(a) -> Value) -> Value {
  vals
  |> list.map(kind)
  |> Array
}

pub fn nullable(inner_type: fn(a) -> Value, value: Option(a)) -> Value {
  case value {
    Some(term) -> inner_type(term)
    None -> Null
  }
}

// ---------- Pool ---------- //

pub opaque type Db {
  Db(pool: pool.Pool(Connection), config: Config, tc: TypeCache, qc: QueryCache)
}

pub fn new(config: Config) -> Db {
  let tc = type_cache.new()
  let qc = query_cache.new()
  let pool = pool.new()

  Db(pool:, config:, tc:, qc:)
}

pub fn start(db: Db) -> actor.StartResult(Supervisor) {
  let pool =
    db.pool
    |> pool.size(db.config.pool_size)
    |> pool.on_open(fn() { connect(db) })
    |> pool.on_close(disconnect)
    |> pool.on_ping(fn(conn) {
      let _ = ping(conn)

      Nil
    })

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(type_cache.supervised(db.tc))
  |> supervisor.add(query_cache.supervised(db.qc))
  |> supervisor.add(pool.supervised(pool, 1000))
  |> supervisor.start
}

pub fn supervised(db: Db) -> supervision.ChildSpecification(Supervisor) {
  let pool_supervisor =
    supervision.worker(fn() { start(db) })
    |> supervision.timeout(db.config.timeout)
    |> supervision.restart(supervision.Transient)

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(pool_supervisor)
  |> supervisor.supervised
}

pub fn with_connection(db: Db, next: fn(Connection) -> t) -> Result(t, PglError) {
  let self = process.self()

  use conn <- result.map(checkout(db, self))

  let res = next(conn)

  checkin(db, conn, self)

  res
}

pub fn checkout(db: Db, caller: Pid) -> Result(Connection, PglError) {
  pool.checkout(db.pool, caller, db.config.timeout)
  |> result.map_error(PglError)
}

pub fn checkin(db: Db, conn: Connection, caller: Pid) -> Nil {
  pool.checkin(db.pool, conn, caller)
}

pub fn shutdown(db: Db) -> Result(Nil, PglError) {
  pool.shutdown(db.pool, db.config.timeout)
  |> result.map_error(PglError)
}

pub fn ping(conn: Connection) -> Result(Connection, PglError) {
  protocol.ping(conn.sock)
  |> result.replace(conn)
  |> result.map_error(from_internal_error)
}

// ---------- Connection ---------- //

pub opaque type Connection {
  Connection(
    sock: Socket,
    savepoint: Option(Int),
    tc: TypeCache,
    qc: QueryCache,
    conf: Config,
  )
}

fn to_queried(
  ext: protocol.Extended(Value),
  conf: Config,
) -> Result(Queried, internal.PglError) {
  let values = list.reverse(ext.values)

  let rows = case conf.rows_as_maps {
    True -> rows_to_maps(ext.fields, values)
    False -> list.map(values, dynamic.array)
  }

  Ok(Queried(count: ext.count, fields: ext.fields, rows:))
}

fn rows_to_maps(
  fields: List(String),
  values: List(List(Dynamic)),
) -> List(Dynamic) {
  use row <- list.map(values)

  lists_zip_(fields, row) |> maps_from_list_
}

/// Creates a new connection to the database.
fn connect(db: Db) -> Result(Connection, String) {
  socket.tcp
  |> protocol.auth(db.config)
  |> result.map(Connection(_, None, db.tc, db.qc, db.config))
  |> result.map_error(internal.error_to_string)
}

/// Shuts down a database connection.
pub fn disconnect(conn: Connection) -> Nil {
  let _ = socket.shutdown(conn.sock)

  Nil
}

// ---------- Query ---------- //

pub type Query {
  Query(sql: String, params: List(Value))
}

pub fn with_params(q: Query, params: List(Value)) -> Query {
  Query(..q, params:)
}

/// Perform a query with the given SQL string and list of parameters.
pub fn query(
  sql: String,
  params: List(Value),
  conn: Connection,
) -> Result(Queried, PglError) {
  extended_query(sql, params, conn)
  |> result.try(to_queried(_, conn.conf))
  |> result.map_error(from_internal_error)
}

pub fn pipeline(
  queries: List(Query),
  conn: Connection,
) -> Result(List(protocol.Extended(Value)), internal.PglError) {
  let messages =
    queries
    |> list.try_map(fn(query) {
      let Query(sql, params) = query

      use oids <- result.try(query_cache.lookup(conn.qc, sql))
      use info <- result.map(type_cache.lookup(conn.tc, oids, conn.conf))

      encode.cached(sql, params, info, type_encoder)
    })
    |> result.lazy_unwrap(fn() {
      list.map(queries, fn(q) { encode.uncached(q.sql, q.params) })
    })

  let ext = extended(conn)

  protocol.pipeline()
  |> protocol.batch_process(ext, messages, conn.sock)
}

/// Perform a query with the given SQL string. This function does not accept
/// any parameters and will send the SQL string as is to the postgres database
/// server.
pub fn exec(sql: String, on conn: Connection) -> Result(Int, PglError) {
  extended_query(sql, [], conn)
  |> result.map(fn(rows) { rows.count })
  |> result.map_error(from_internal_error)
}

fn extended_query(
  sql: String,
  params: List(Value),
  conn: Connection,
) -> Result(protocol.Extended(Value), internal.PglError) {
  let message =
    encode_from_cache(sql, params, conn)
    |> result.lazy_unwrap(fn() { encode.uncached(sql, params) })

  extended(conn)
  |> protocol.process(message, conn.sock)
}

fn extended(conn: Connection) -> protocol.Extended(Value) {
  protocol.extended()
  |> protocol.on_decode_row(fn(vals, oids) { decode_row(vals, oids, conn) })
  |> protocol.on_param_description(fn(sql, params, oids) {
    on_param_description(sql, params, oids, conn)
  })
}

fn encode_from_cache(
  sql: String,
  params: List(Value),
  conn: Connection,
) -> Result(encode.Query(Value, types.TypeInfo), internal.PglError) {
  use oids <- result.try(query_cache.lookup(conn.qc, sql))
  use info <- result.map(type_cache.lookup(conn.tc, oids, conn.conf))

  encode.cached(sql, params, info, type_encoder)
  |> encode.with_sync
}

fn type_encoder(value: Value, info: types.TypeInfo) {
  case value {
    Null -> types.encode(value, info, with: types.null)
    Bool(val) -> types.encode(val, info, with: types.bool)
    Int(val) -> types.encode(val, info, with: types.int)
    Float(val) -> types.encode(val, info, with: types.float)
    Text(val) -> types.encode(val, info, with: types.text)
    Bytea(val) -> types.encode(val, info, with: types.raw)
    Time(val) -> types.encode(val, info, with: types.time)
    Date(val) -> types.encode(val, info, with: types.date)
    Timestamp(ts) -> types.encode(ts, info, with: types.timestamp)
    Interval(val) -> types.encode(val, info, with: types.interval)
    Array(val) ->
      types.encode(val, info, with: fn(val, elem_ti) {
        types.array(val, elem_ti, of: type_encoder)
      })
  }
}

fn on_param_description(
  sql: String,
  params: List(Value),
  oids: List(Int),
  conn: Connection,
) -> Result(BitArray, internal.PglError) {
  query_cache.insert(conn.qc, sql, oids)

  use info <- result.map(type_cache.lookup(conn.tc, oids, conn.conf))

  encode.cached(sql, params, info, type_encoder)
  |> encode.with_sync
  |> encode.to_bit_array
}

fn decode_row(
  values: List(BitArray),
  oids: List(Int),
  conn: Connection,
) -> Result(List(Dynamic), internal.PglError) {
  use type_info <- result.try(type_cache.lookup(conn.tc, oids, conn.conf))

  decode_row_values(values, type_info)
}

fn decode_row_values(
  values: List(BitArray),
  infos: List(types.TypeInfo),
) -> Result(List(Dynamic), internal.PglError) {
  list.strict_zip(values, infos)
  |> result.replace_error(decode.error("Mismatched values and infos"))
  |> result.try(fn(vals_infos) {
    list.try_map(vals_infos, fn(val_info) {
      let #(val, info) = val_info

      types.decode(val, info)
      |> result.map_error(internal.decode_error)
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

pub fn begin(conn: Connection) -> Result(Connection, TransactionError(error)) {
  let packet = encode.query("BEGIN")

  protocol.simple(packet, conn.sock)
  |> result.replace(conn)
  |> result.replace_error(TransactionError)
}

/// Commits a transaction
pub fn commit(conn: Connection) -> Result(Connection, TransactionError(error)) {
  let packet = encode.query("COMMIT")

  protocol.simple(packet, conn.sock)
  |> result.replace(conn)
  |> result.replace_error(TransactionError)
}

/// Rolls back a transaction
pub fn rollback(conn: Connection) -> Result(Connection, TransactionError(error)) {
  case conn.savepoint {
    Some(num) -> rollback_savepoint(num, conn)
    None -> {
      let packet = encode.query("ROLLBACK")

      protocol.simple(packet, conn.sock)
      |> result.replace(conn)
      |> result.replace_error(TransactionError)
    }
  }
}

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
  |> result.try(fn(res) { release(conn1) |> result.replace(res) })
}

fn rollback_and_release(
  conn: Connection,
) -> Result(Connection, TransactionError(error)) {
  rollback(conn)
  |> result.try(release)
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
  |> result.replace_error(TransactionError)
}

fn set_savepoint(conn: Connection, savepoint: Int) -> Connection {
  Connection(..conn, savepoint: Some(savepoint))
}

pub fn release(conn: Connection) -> Result(Connection, TransactionError(error)) {
  case conn.savepoint {
    Some(num) -> release_savepoint(num, conn)
    None -> Error(NotInTransaction(""))
  }
}

fn release_savepoint(
  num: Int,
  conn: Connection,
) -> Result(Connection, TransactionError(error)) {
  let statement =
    "RELEASE SAVEPOINT " <> savepoint_name <> int.to_string(num - 1)

  let packet = encode.query(statement)

  protocol.simple(packet, conn.sock)
  |> result.replace(conn)
  |> result.replace_error(TransactionError)
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
  |> result.replace_error(TransactionError)
}

pub type Queried {
  Queried(count: Int, fields: List(String), rows: List(Dynamic))
}

@external(erlang, "lists", "zip")
fn lists_zip_(fields: List(String), row: List(Dynamic)) -> Dynamic

@external(erlang, "maps", "from_list")
fn maps_from_list_(list: Dynamic) -> Dynamic
