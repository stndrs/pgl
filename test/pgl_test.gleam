import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode.{type Decoder}
import gleam/int
import gleam/list
import gleam/order
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import gleeunit
import global_value
import pgl
import pgl/config
import pgl/internal

pub fn main() {
  gleeunit.main()
}

pub fn parse_url_test() {
  let assert Ok(conf) =
    "postgres://postgres:supersecretpassword@localhost:5433/gleam_pgl_test"
    |> config.from_url

  let assert True =
    config.Config(
      ..config.default,
      host: "localhost",
      port: 5433,
      database: "gleam_pgl_test",
      user: "postgres",
      password: "supersecretpassword",
      ssl: config.SslDisabled,
    )
    == conf
}

pub fn parse_url_alternative_schema_test() {
  let assert Ok(conf) =
    "postgresql://postgres:supersecretpassword@localhost:5433/gleam_pgl_test"
    |> config.from_url

  let assert True =
    config.Config(
      ..config.default,
      host: "localhost",
      port: 5433,
      database: "gleam_pgl_test",
      user: "postgres",
      password: "supersecretpassword",
    )
    == conf
}

pub fn parse_url_invalid_protocol_test() {
  let assert Error(Nil) =
    config.from_url(
      "mysql://u:supersecretpassword@localhost:5432/gleam_pgl_test",
    )
}

pub fn parse_url_invalid_path_test() {
  let assert Error(Nil) =
    config.from_url("postgres://user:pass@db:5432/some/path")
}

pub fn parse_url_ssl_mode_require_test() {
  let assert Ok(conf) =
    "postgres://user:pass@localhost:5432/gleam_pgl_test?sslmode=require"
    |> config.from_url

  let assert True =
    config.Config(
      ..config.default,
      host: "localhost",
      port: 5432,
      database: "gleam_pgl_test",
      user: "user",
      password: "pass",
      ssl: config.SslUnverified,
    )
    == conf
}

pub fn parse_url_ssl_mode_verify_test() {
  let assert Ok(conf) =
    "postgres://user:pass@localhost:5432/gleam_pgl_test?sslmode=verify-ca"
    |> config.from_url

  let assert True =
    config.Config(
      ..config.default,
      host: "localhost",
      port: 5432,
      database: "gleam_pgl_test",
      user: "user",
      password: "pass",
      ssl: config.SslVerified,
    )
    == conf

  let assert Ok(conf) =
    "postgres://user:pass@localhost:5432/gleam_pgl_test?sslmode=verify-full"
    |> config.from_url

  let assert True =
    config.Config(
      ..config.default,
      host: "localhost",
      port: 5432,
      database: "gleam_pgl_test",
      user: "user",
      password: "pass",
      ssl: config.SslVerified,
    )
    == conf
}

const drop_table_sql = "DROP TABLE IF EXISTS users;"

const create_table_sql = "
 CREATE TABLE IF NOT EXISTS users (
   id SERIAL PRIMARY KEY,
   name VARCHAR(50) NOT NULL,
   active boolean NOT NULL DEFAULT true,
   nicknames VARCHAR(50)[] NOT NULL,
   birthday DATE NOT NULL,
   created_at TIMESTAMP NOT NULL
 );"

fn global_pool() -> pgl.Db {
  use <- global_value.create_with_unique_name("pgl_pool_test")

  let assert Ok(conf) =
    "postgres://postgres:postgres@127.0.0.1/gleam_pgl_test"
    |> config.from_url

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  db
}

fn global_pool_ssl() -> pgl.Db {
  use <- global_value.create_with_unique_name("pgl_pool_ssl_test")

  let assert Ok(conf) =
    "postgres://postgres:postgres@127.0.0.1:5433/gleam_pgl_test?sslmode=require"
    |> config.from_url

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  db
}

fn global_pool_rows_as_maps() -> pgl.Db {
  use <- global_value.create_with_unique_name("pgl_pool_rows_as_maps_test")

  let assert Ok(conf) =
    "postgres://postgres:postgres@127.0.0.1/gleam_pgl_test"
    |> config.from_url

  let conf = config.set_rows_as_maps(conf, True)

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  db
}

fn start_default(next: fn(pgl.Connection) -> t) -> t {
  let assert Ok(res) =
    global_pool()
    |> with_setup_conn(next)

  res
}

fn start_ssl(next: fn(pgl.Connection) -> t) -> t {
  let assert Ok(res) =
    global_pool_ssl()
    |> with_setup_conn(next)

  res
}

fn with_setup_conn(
  pool: pgl.Db,
  next: fn(pgl.Connection) -> t,
) -> Result(t, pgl.PglError) {
  use conn <- pgl.with_connection(pool)

  let assert Ok(_) = pgl.exec(drop_table_sql, conn)
  let assert Ok(_) = pgl.exec(create_table_sql, conn)

  next(conn)
}

pub fn ping_test() {
  use conn <- start_default()

  let assert Ok(_) = pgl.ping(conn)
}

pub type User {
  User(
    id: Int,
    name: String,
    active: Bool,
    nicknames: List(String),
    birthday: calendar.Date,
    created_at: timestamp.Timestamp,
  )
}

fn user_decoder() -> Decoder(User) {
  use id <- decode.field(0, decode.int)
  use name <- decode.field(1, decode.string)
  use active <- decode.field(2, decode.bool)
  use nicknames <- decode.field(3, decode.list(of: decode.string))
  use birthday <- decode.field(4, decode_date())
  use created_at <- decode.field(5, decode_timestamp())

  User(id:, name:, active:, nicknames:, created_at:, birthday:)
  |> decode.success
}

fn inserting_new_rows(conn: pgl.Connection) {
  let assert Ok(returned) =
    insert_into_users([
      "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
      "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
    ])
    |> returning(["*"])
    |> pgl.query([], conn)

  let assert 2 = returned.count

  let assert Ok([william, stephen]) =
    returned.rows
    |> list.try_map(fn(row) { decode.run(row, user_decoder()) })

  let assert Ok(william_created_at) =
    timestamp.parse_rfc3339("2025-09-30T09:17:30.100Z")
  let william_birthday = calendar.Date(1990, calendar.February, 9)

  let assert "William" = william.name
  let assert False = william.active
  let assert ["William", "Will"] = william.nicknames
  let assert True = william.created_at == william_created_at
  let assert True = william.birthday == william_birthday

  let assert Ok(stephen_created_at) =
    timestamp.parse_rfc3339("2025-01-06T20:01:06.000Z")
  let stephen_birthday = calendar.Date(1993, calendar.January, 1)

  let assert "Stephen" = stephen.name
  let assert True = stephen.active
  let assert ["Steve"] = stephen.nicknames
  let assert True = stephen.created_at == stephen_created_at
  let assert True = stephen.birthday == stephen_birthday
}

pub fn inserting_new_rows_test() {
  use conn <- start_default()

  inserting_new_rows(conn)
}

pub fn inserting_new_rows_ssl_test() {
  use conn <- start_ssl()

  inserting_new_rows(conn)
}

pub fn inserting_new_rows_and_returning_test() {
  use conn <- start_default()

  let assert Ok(returned) =
    insert_into_users([
      "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
      "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
    ])
    |> returning(["name"])
    |> pgl.query([], conn)

  let assert 2 = returned.count
  let assert True =
    returned.rows
    == [
      dynamic.array([dynamic.string("William")]),
      dynamic.array([dynamic.string("Stephen")]),
    ]
}

pub fn pipeline_multiple_query_test() {
  use conn <- start_default()

  let insert1 =
    insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> returning(["name", "active", "nicknames"])

  let insert2 =
    insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Dick', 'Robin', 'Nightwing'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> returning(["name", "active", "nicknames"])

  let params1 = [pgl.Text("Margaret"), pgl.Bool(True)]

  let params2 = [pgl.Text("Richard"), pgl.Bool(False)]

  let assert Ok(rows) =
    [pgl.Query(insert1, params1), pgl.Query(insert2, params2)]
    |> pgl.pipeline(conn)

  list.try_map(rows, fn(_rows) { Ok(Nil) })
}

pub fn pipeline_multiple_different_queries_test() {
  use conn <- start_default()

  let insert1 =
    insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> returning(["name", "active", "nicknames"])

  let params1 = [pgl.Text("Margaret"), pgl.Bool(True)]

  let assert Ok(rows) =
    [pgl.Query(insert1, params1), pgl.Query("SELECT 1", [])]
    |> pgl.pipeline(conn)

  list.try_map(rows, fn(_rows) { Ok(Nil) })
}

fn insert_into_users(values: List(String)) -> String {
  let values_str = string.join(values, "), (")

  "INSERT INTO users VALUES (" <> values_str <> ")"
}

fn returning(sql: String, columns: List(String)) -> String {
  sql <> " RETURNING " <> string.join(columns, ", ")
}

pub fn null_to_string_test() {
  let assert "NULL" = pgl.null |> pgl.value_to_string
}

pub fn bool_to_string_test() {
  let assert "TRUE" = pgl.bool(True) |> pgl.value_to_string
  let assert "FALSE" = pgl.bool(False) |> pgl.value_to_string
}

pub fn int_to_string_test() {
  let assert "42" = pgl.int(42) |> pgl.value_to_string
  let assert "0" = pgl.int(0) |> pgl.value_to_string
  let assert "-123" = pgl.int(-123) |> pgl.value_to_string
}

pub fn float_to_string_test() {
  let assert "3.14" = pgl.float(3.14) |> pgl.value_to_string
  let assert "0.0" = pgl.float(0.0) |> pgl.value_to_string
  let assert "-2.5" = pgl.float(-2.5) |> pgl.value_to_string
}

pub fn text_to_string_test() {
  let assert "'hello'" = pgl.text("hello") |> pgl.value_to_string
  let assert "''" = pgl.text("") |> pgl.value_to_string
  let assert "'It\\'s working'" =
    pgl.text("It's working") |> pgl.value_to_string
  let assert "'Say \\'hello\\''" =
    pgl.text("Say 'hello'") |> pgl.value_to_string
}

pub fn bytea_to_string_test() {
  let assert "'\\x48656C6C6F'" =
    pgl.bytea(<<"Hello":utf8>>) |> pgl.value_to_string
  let assert "'\\x'" = pgl.bytea(<<>>) |> pgl.value_to_string
  let assert "'\\xDEADBEEF'" =
    pgl.bytea(<<0xDE, 0xAD, 0xBE, 0xEF>>) |> pgl.value_to_string
}

pub fn time_to_string_test() {
  let assert "'14:30:45'" =
    pgl.time(calendar.TimeOfDay(14, 30, 45, 0)) |> pgl.value_to_string
  let assert "'00:00:00'" =
    pgl.time(calendar.TimeOfDay(0, 0, 0, 0)) |> pgl.value_to_string
  let assert "'23:59:59.123'" =
    pgl.time(calendar.TimeOfDay(23, 59, 59, 123_456_000)) |> pgl.value_to_string
  let assert "'09:05:03'" =
    pgl.time(calendar.TimeOfDay(9, 5, 3, 0)) |> pgl.value_to_string
  let assert "'09:05:03.400'" =
    pgl.time(calendar.TimeOfDay(9, 5, 3, 400_000_000)) |> pgl.value_to_string
  let assert "'09:05:03.012'" =
    pgl.time(calendar.TimeOfDay(9, 5, 3, 12_000_000)) |> pgl.value_to_string
  let assert "'09:05:03.007'" =
    pgl.time(calendar.TimeOfDay(9, 5, 3, 7_000_000)) |> pgl.value_to_string
}

pub fn date_to_string_test() {
  let assert "'2025-01-15'" =
    pgl.date(calendar.Date(2025, calendar.January, 15)) |> pgl.value_to_string
  let assert "'1990-02-09'" =
    pgl.date(calendar.Date(1990, calendar.February, 9)) |> pgl.value_to_string
  let assert "'2000-12-31'" =
    pgl.date(calendar.Date(2000, calendar.December, 31)) |> pgl.value_to_string
}

pub fn timestamp_to_string_test() {
  let assert Ok(ts) = timestamp.parse_rfc3339("2025-01-15T14:30:45Z")
  let assert "'2025-01-15T14:30:45Z'" = pgl.timestamp(ts) |> pgl.value_to_string

  let assert Ok(ts2) = timestamp.parse_rfc3339("2000-12-31T23:59:59.123456789Z")
  let assert "'2000-12-31T23:59:59.123456789Z'" =
    pgl.timestamp(ts2) |> pgl.value_to_string
}

pub fn interval_to_string_test() {
  let assert "'PT1H30M'" =
    pgl.interval(duration.hours(1) |> duration.add(duration.minutes(30)))
    |> pgl.value_to_string

  let assert "'PT0S'" = pgl.interval(duration.seconds(0)) |> pgl.value_to_string

  let assert "'PT5M30S'" =
    pgl.interval(duration.minutes(5) |> duration.add(duration.seconds(30)))
    |> pgl.value_to_string
}

pub fn rows_as_maps_test() {
  global_pool_rows_as_maps()
  |> with_setup_conn(fn(conn) {
    let sql =
      insert_into_users([
        "DEFAULT, 'James', true, ARRAY['Jim'], '2233-04-22', '2263-01-09 11:30:22'",
        "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
        "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
      ])

    let assert Ok(count) = pgl.exec(sql, conn)
    let assert 3 = count

    let assert Ok(queried) =
      "SELECT * FROM users"
      |> pgl.query([], conn)

    let assert 3 = queried.count

    let assert Ok([james, william, steve]) =
      queried.rows
      |> list.try_map(decode.run(_, user_with_fields_decoder()))

    let assert 1 = james.id
    let assert 2 = william.id
    let assert 3 = steve.id
  })
}

fn user_with_fields_decoder() -> Decoder(User) {
  use id <- decode.field("id", decode.int)
  use name <- decode.field("name", decode.string)
  use active <- decode.field("active", decode.bool)
  use nicknames <- decode.field("nicknames", decode.list(of: decode.string))
  use birthday <- decode.field("birthday", decode_date())
  use created_at <- decode.field("created_at", decode_timestamp())

  User(id:, name:, active:, nicknames:, created_at:, birthday:)
  |> decode.success
}

pub fn selecting_rows_test() {
  use conn <- start_default()
  let sql =
    insert_into_users([
      "DEFAULT, 'James', true, ARRAY['Jim'], '2233-04-22', '2263-01-09 11:30:22'",
    ])

  let assert Ok(count) = pgl.exec(sql, conn)

  let assert 1 = count

  let assert Ok(returned) =
    pgl.query("SELECT * FROM users WHERE name = $1", [pgl.Text("James")], conn)

  let assert 1 = returned.count

  let assert Ok(james) =
    returned.rows
    |> list.first

  let assert Ok(ts) = timestamp.parse_rfc3339("2263-01-09T11:30:22Z")
  let #(seconds, nanoseconds) = timestamp.to_unix_seconds_and_nanoseconds(ts)

  let microseconds = { seconds * 1_000_000 } + { nanoseconds / 1000 }

  let assert True =
    james
    == dynamic.array([
      dynamic.int(1),
      dynamic.string("James"),
      dynamic.bool(True),
      dynamic.array([dynamic.string("Jim")]),
      dynamic.array([dynamic.int(2233), dynamic.int(4), dynamic.int(22)]),
      dynamic.int(microseconds),
    ])
}

pub fn varchar_encoding_test() {
  use conn <- start_default()

  let sql = "SELECT $1::VARCHAR, $2::VARCHAR, $3::VARCHAR"
  let params = [
    pgl.Text("howdy"),
    pgl.Text(""),
    pgl.Text("postgres"),
  ]

  let assert Ok(result) = pgl.query(sql, params, conn)

  let assert 1 = result.count

  let assert True =
    result.rows
    == [
      dynamic.array([
        dynamic.string("howdy"),
        dynamic.string(""),
        dynamic.string("postgres"),
      ]),
    ]
}

pub fn null_encoding_test() {
  use conn <- start_default()

  let sql = "SELECT $1::TEXT, $1 IS NULL, $2::INT"
  let params = [pgl.Null, pgl.Int(42)]

  let assert Ok(result) = pgl.query(sql, params, conn)

  let assert 1 = result.count

  let assert True =
    result.rows
    == [
      dynamic.array([dynamic.string(""), dynamic.bool(True), dynamic.int(42)]),
    ]
}

pub fn array_encoding_test() {
  use conn <- start_default()

  let sql =
    "SELECT ARRAY['howdy', 'postgres']::TEXT[], ARRAY[1, 2, 3]::INT[], ARRAY[]::TEXT[]"
  let params = []

  let assert Ok(result) = pgl.query(sql, params, conn)

  let assert 1 = result.count

  let row_decoder = {
    use text_array <- decode.field(0, decode.list(of: decode.string))
    use int_array <- decode.field(1, decode.list(of: decode.int))
    use empty_array <- decode.field(2, decode.list(of: decode.string))

    decode.success(#(text_array, int_array, empty_array))
  }

  let assert Ok([#(text, int, empty)]) =
    result.rows
    |> list.try_map(decode.run(_, row_decoder))

  let assert ["howdy", "postgres"] = text
  let assert [1, 2, 3] = int
  let assert [] = empty
}

pub fn mixed_types_with_encoding_test() {
  use conn <- start_default()

  let sql =
    insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> returning(["name", "active", "nicknames"])

  let params = [pgl.Text("Margaret"), pgl.Bool(True)]

  let assert Ok(result) = pgl.query(sql, params, conn)

  let assert 1 = result.count

  let row_decoder = {
    use name <- decode.field(0, decode.string)
    use active <- decode.field(1, decode.bool)
    use nicknames <- decode.field(2, decode.list(of: decode.string))

    decode.success(#(name, active, nicknames))
  }

  let assert Ok([#(name, active, nicknames)]) =
    result.rows
    |> list.try_map(decode.run(_, row_decoder))

  let assert "Margaret" = name
  let assert True = active
  let assert ["Peggy"] = nicknames
}

pub fn error_handling_test() {
  use conn <- start_default()

  let sql = "SELECT * FROM non_existent_table"
  let params = []

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.query(sql, params, conn)

  let assert "42P01" = code
  let assert "undefined_table" = name
  let assert "relation \"non_existent_table\" does not exist" = message
}

pub fn invalid_sql_test() {
  use conn <- start_default()
  let sql = "select       select"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.exec(sql, conn)

  let assert "42601" = code
  let assert "syntax_error" = name
  let assert "syntax error at or near \"select\"" = message
}

pub fn insert_constraint_error_test() {
  use conn <- start_default()

  let assert Error(pgl.PostgresError(code:, name:, message:, fields:)) =
    insert_into_users([
      "900, 'William', false, ARRAY['William', 'Will'], '1990-02-09', now()",
      "900, 'Stephen', true, ARRAY['Steve'], '1993-01-01', now()",
    ])
    |> pgl.exec(conn)

  let assert "23505" = code
  let assert "unique_violation" = name
  let assert "duplicate key value violates unique constraint \"users_pkey\"" =
    message

  let assert Ok(constraint) = dict.get(fields, "Constraint")
  let assert "users_pkey" = constraint

  let assert Ok(detail) = dict.get(fields, "Detail")
  let assert "Key (id)=(900) already exists." = detail

  let assert Ok(table) = dict.get(fields, "Table")
  let assert "users" = table

  let assert Ok(schema) = dict.get(fields, "Schema")
  let assert "public" = schema
}

pub fn select_from_unknown_table_test() {
  use conn <- start_default()
  let sql = "SELECT * FROM unknown"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.exec(sql, conn)

  let assert "42P01" = code
  let assert "undefined_table" = name
  let assert "relation \"unknown\" does not exist" = message
}

pub fn insert_with_incorrect_type_test() {
  use conn <- start_default()

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    insert_into_users(["true, true, true, true"])
    |> pgl.exec(conn)

  let assert "42804" = code
  let assert "datatype_mismatch" = name
  let assert "column \"id\" is of type integer but expression is of type boolean" =
    message
}

pub fn execute_with_wrong_number_of_arguments_test() {
  use conn <- start_default()
  let sql = "SELECT * FROM users WHERE id = $1"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.exec(sql, conn)

  let assert "34000" = code
  let assert "invalid_cursor_name" = name
  let assert "portal \"\" does not exist" = message
}

pub fn insert_with_values_test() {
  use conn <- start_default()

  let sql =
    "INSERT INTO users (name, nicknames, birthday, created_at) VALUES ($1, $2, $3, $4)"

  let values = [
    pgl.text("Richard"),
    pgl.array(["Dick", "Robin", "Nightwing"], of: pgl.text),
    pgl.date(calendar.Date(2011, calendar.March, 20)),
    pgl.timestamp(timestamp.system_time()),
  ]

  let assert Ok(_) = pgl.query(sql, values, conn)
}

pub fn transaction_commit_test() {
  use conn <- start_default()

  setup_users_table(conn)

  let assert Ok(#(id1, id2)) = {
    use tx <- result.map(pgl.begin(conn))

    let id1 = insert_into_users_table(tx, "one")
    let id2 = insert_into_users_table(tx, "two")

    let assert Ok(_) = pgl.commit(tx)

    #(id1, id2)
  }

  let assert Ok(queried) =
    pgl.query("SELECT id FROM users ORDER BY id", [], conn)

  let assert Ok([got1, got2]) =
    queried.rows
    |> list.try_map(fn(row) {
      decode.run(row, {
        use id <- decode.field(0, decode.int)
        decode.success(id)
      })
    })

  let assert True = id1 == got1
  let assert True = id2 == got2
}

pub fn transaction_rollback_test() {
  use conn <- start_default()

  setup_users_table(conn)

  use tx <- result.map(pgl.begin(conn))

  let _id1 = insert_into_users_table(tx, "two")
  let _id2 = insert_into_users_table(tx, "three")

  let assert Ok(conn) = pgl.rollback(tx)

  let assert Ok(queried) =
    "SELECT * FROM users"
    |> pgl.query([], conn)

  let assert 0 = queried.count
}

pub fn transaction_error_test() {
  use conn <- start_default()

  let assert Ok(_) =
    "DROP TABLE IF EXISTS tx_test"
    |> pgl.exec(conn)

  let assert Ok(_) =
    "CREATE TABLE tx_test (id INTEGER PRIMARY KEY, name TEXT)"
    |> pgl.exec(conn)

  let assert Ok(_queried) =
    "INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *"
    |> pgl.query([pgl.int(1), pgl.text("Before")], conn)

  let assert Ok(queried) =
    "SELECT COUNT(*) FROM tx_test"
    |> pgl.query([], conn)

  let assert 1 = queried.count

  let assert Error(pgl.RollbackError(pgl.PostgresError(code, name, ..))) = {
    use tx <- pgl.transaction(conn)

    let assert Ok(_queried) =
      "INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *"
      |> pgl.query([pgl.int(2), pgl.text("Transaction")], tx)

    "INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *"
    |> pgl.query([pgl.int(1), pgl.text("Duplicate")], tx)
  }

  let assert "23505" = code
  let assert "unique_violation" = name

  let assert Ok(queried) =
    "SELECT COUNT(*) FROM tx_test"
    |> pgl.query([], conn)

  let assert 1 = queried.count
}

pub fn savepoint_test() {
  use conn <- start_default()

  setup_users_table(conn)

  let assert Ok(_) =
    pgl.transaction(conn, fn(tx) {
      let id1 = insert_into_users_table(tx, "one")

      let assert Ok(_) = pgl.query("SELECT 1", [], tx)

      pgl.savepoint(tx, fn(tx2) {
        let id2 = insert_into_users_table(tx2, "two")

        let assert Ok(_) = pgl.query("SELECT 1", [], tx2)

        Ok(id2)
      })
      |> result.map(fn(id2) { #(id1, id2) })
    })
}

pub fn savepoint_release_test() {
  use conn <- start_default()

  setup_users_table(conn)

  let assert Ok(_) =
    pgl.transaction(conn, fn(tx) {
      let id1 = insert_into_users_table(tx, "one")

      let assert Ok(_) = pgl.query("SELECT 1", [], tx)

      pgl.savepoint(tx, fn(tx2) {
        let id2 = insert_into_users_table(tx2, "two")

        let assert Ok(_) = pgl.query("SELECT 2", [], tx2)

        let assert Error(_) =
          pgl.savepoint(tx2, fn(tx3) {
            let id3 = insert_into_users_table(tx3, "three")

            let assert order.Gt = int.compare(id3, id2)

            Error(internal.PglError("nah"))
          })

        Ok(id2)
      })
      |> result.map(fn(id2) { #(id1, id2) })
    })

  let assert Ok(queried) =
    "SELECT * FROM users WHERE name IN ('one', 'two', 'three')"
    |> pgl.query([], conn)

  let assert 2 = queried.count
}

// Transaction helper functions

fn setup_users_table(conn: pgl.Connection) {
  let assert Ok(_) = pgl.exec("truncate table users", conn)

  Nil
}

fn insert_into_users_table(conn: pgl.Connection, name: String) {
  let assert Ok(returned) =
    insert_into_users([
      "DEFAULT, '" <> name <> "', true, ARRAY[''], '2025-03-04', now()",
    ])
    |> returning(["id"])
    |> pgl.query([], conn)

  let assert Ok(row) =
    returned.rows
    |> list.first

  let assert Ok(ids) = decode.run(row, decode.list(of: decode.int))
  let assert Ok(id) = list.first(ids)
  id
}

// Decoders

fn decode_timestamp() -> Decoder(timestamp.Timestamp) {
  decode.one_of(decode.int, or: [decode.string |> decode.map(fn(_) { 0 })])
  |> decode.map(fn(usecs) {
    usecs
    |> int.multiply(1000)
    |> timestamp.from_unix_seconds_and_nanoseconds(0, _)
  })
}

fn decode_date() -> Decoder(calendar.Date) {
  use year <- decode.field(0, decode.int)
  use month <- decode.field(1, decode.int)
  use day <- decode.field(2, decode.int)

  case calendar.month_from_int(month) {
    Ok(month) -> decode.success(calendar.Date(year, month, day))
    _ -> decode.failure(calendar.Date(1970, calendar.January, 1), "Date")
  }
}
