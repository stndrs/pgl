import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode.{type Decoder}
import gleam/int
import gleam/list
import gleam/order
import gleam/result
import gleam/string
import gleam/time/calendar
import gleam/time/timestamp
import gleeunit
import global_value
import pg_value
import pg_value/interval
import pgl

pub fn main() {
  gleeunit.main()
}

pub fn parse_url_test() {
  let assert Ok(conf) =
    "postgres://postgres:supersecretpassword@localhost:5433/gleam_pgl_test"
    |> pgl.from_url

  assert pgl.Config(
      ..pgl.default,
      host: "localhost",
      port: 5433,
      database: "gleam_pgl_test",
      username: "postgres",
      password: "supersecretpassword",
      ssl: pgl.SslDisabled,
    )
    == conf
}

pub fn parse_url_alternative_schema_test() {
  let assert Ok(conf) =
    "postgresql://postgres:supersecretpassword@localhost:5433/gleam_pgl_test"
    |> pgl.from_url

  assert pgl.Config(
      ..pgl.default,
      host: "localhost",
      port: 5433,
      database: "gleam_pgl_test",
      username: "postgres",
      password: "supersecretpassword",
    )
    == conf
}

pub fn parse_url_invalid_protocol_test() {
  let assert Error(Nil) =
    pgl.from_url("mysql://u:supersecretpassword@localhost:5432/gleam_pgl_test")
}

pub fn parse_url_invalid_path_test() {
  let assert Error(Nil) =
    pgl.from_url("postgres://username:pass@db:5432/some/path")
}

pub fn parse_url_ssl_mode_require_test() {
  let assert Ok(conf) =
    "postgres://username:pass@localhost:5432/gleam_pgl_test?sslmode=require"
    |> pgl.from_url

  assert pgl.Config(
      ..pgl.default,
      host: "localhost",
      port: 5432,
      database: "gleam_pgl_test",
      username: "username",
      password: "pass",
      ssl: pgl.SslUnverified,
    )
    == conf
}

pub fn parse_url_ssl_mode_verify_test() {
  let assert Ok(conf) =
    "postgres://username:pass@localhost:5432/gleam_pgl_test?sslmode=verify-ca"
    |> pgl.from_url

  assert pgl.Config(
      ..pgl.default,
      host: "localhost",
      port: 5432,
      database: "gleam_pgl_test",
      username: "username",
      password: "pass",
      ssl: pgl.SslVerified,
    )
    == conf

  let assert Ok(conf) =
    "postgres://username:pass@localhost:5432/gleam_pgl_test?sslmode=verify-full"
    |> pgl.from_url

  assert pgl.Config(
      ..pgl.default,
      host: "localhost",
      port: 5432,
      database: "gleam_pgl_test",
      username: "username",
      password: "pass",
      ssl: pgl.SslVerified,
    )
    == conf
}

// Config tests

pub fn database_test() {
  let conf = pgl.default
  let result = pgl.database(conf, "test_db")

  assert "test_db" == result.database
  assert result.host == conf.host
  assert result.port == conf.port
}

pub fn host_test() {
  let conf = pgl.default
  let result = pgl.host(conf, "192.168.1.1")

  assert "192.168.1.1" == result.host
  assert result.database == conf.database
  assert result.port == conf.port
}

pub fn port_test() {
  let conf = pgl.default
  let result = pgl.port(conf, 3306)

  assert 3306 == result.port
  assert result.host == conf.host
  assert result.database == conf.database
}

pub fn username_test() {
  let conf = pgl.default
  let result = pgl.username(conf, "admin")

  assert "admin" == result.username
  assert result.host == conf.host
  assert result.password == conf.password
}

pub fn password_test() {
  let conf = pgl.default
  let result = pgl.password(conf, "secret123")

  assert "secret123" == result.password
  assert result.username == conf.username
  assert result.host == conf.host
}

pub fn ssl_test() {
  let conf = pgl.default
  let result = pgl.ssl(conf, pgl.SslVerified)

  assert pgl.SslVerified == result.ssl
  assert result.host == conf.host
  assert result.port == conf.port
}

pub fn ip_version_test() {
  let conf =
    pgl.default
    |> pgl.ip_version(pgl.Ipv6)

  assert pgl.Ipv6 == conf.ip_version
}

pub fn default_values_test() {
  let conf = pgl.default

  assert "127.0.0.1" == conf.host
  assert 5432 == conf.port
  assert "" == conf.username
  assert "" == conf.password
  assert "" == conf.database
  assert pgl.SslDisabled == conf.ssl
  assert pgl.Ipv4 == conf.ip_version
}

pub fn connection_parameters_test() {
  let conf = pgl.default

  assert conf.connection_parameters == []

  let conf =
    conf
    |> pgl.connection_parameter(name: "timezone", value: "MDT")

  assert conf.connection_parameters == [#("timezone", "MDT")]
}

pub fn query_params_test() {
  let query =
    pgl.sql("SELECT * FROM users WHERE id=$1")
    |> pgl.params([pg_value.int(10)])

  let assert [pg_value.Int(10)] = query.params
}

// Database tests

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
    |> pgl.from_url

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  db
}

// fn global_pool_ssl() -> pgl.Db {
//   use <- global_value.create_with_unique_name("pgl_pool_ssl_test")
// 
//   let assert Ok(conf) =
//     "postgres://postgres:postgres@127.0.0.1:5433/gleam_pgl_test?sslmode=require"
//     |> pgl.from_url
// 
//   let db = pgl.new(conf)
// 
//   let assert Ok(_) = pgl.start(db)
// 
//   db
// }

fn global_pool_rows_as_maps() -> pgl.Db {
  use <- global_value.create_with_unique_name("pgl_pool_rows_as_maps_test")

  let assert Ok(conf) =
    "postgres://postgres:postgres@127.0.0.1/gleam_pgl_test"
    |> pgl.from_url

  let conf = pgl.rows_as_dict(conf, True)

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  db
}

fn connect(next: fn(pgl.Connection) -> t) {
  global_pool()
  |> with_setup_conn(next)
}

// fn start_ssl(next: fn(pgl.Connection) -> t) {
//   global_pool_ssl()
//   |> with_setup_conn(next)
// }

fn with_setup_conn(db: pgl.Db, next: fn(pgl.Connection) -> t) {
  use conn <- pgl.with_connection(db)

  let assert Ok(_) = pgl.execute(drop_table_sql, conn)
  let assert Ok(_) = pgl.execute(create_table_sql, conn)

  next(conn)
}

fn with_conn(next: fn(pgl.Connection) -> t) {
  let db = global_pool()

  use conn <- pgl.with_connection(db)

  next(conn)
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

  assert 2 == returned.count

  let assert Ok([william, stephen]) =
    returned.rows
    |> list.try_map(fn(row) { decode.run(row, user_decoder()) })

  let assert Ok(william_created_at) =
    timestamp.parse_rfc3339("2025-09-30T09:17:30.100Z")
  let william_birthday = calendar.Date(1990, calendar.February, 9)

  assert "William" == william.name
  assert False == william.active
  assert ["William", "Will"] == william.nicknames
  assert william.created_at == william_created_at
  assert william.birthday == william_birthday

  let assert Ok(stephen_created_at) =
    timestamp.parse_rfc3339("2025-01-06T20:01:06.000Z")
  let stephen_birthday = calendar.Date(1993, calendar.January, 1)

  assert "Stephen" == stephen.name
  assert stephen.active
  assert ["Steve"] == stephen.nicknames
  assert stephen.created_at == stephen_created_at
  assert stephen.birthday == stephen_birthday
}

pub fn inserting_new_rows_test() {
  use conn <- connect()

  inserting_new_rows(conn)
}

pub fn ipv6_test() {
  let db =
    pgl.default
    |> pgl.database("gleam_pgl_test")
    |> pgl.username("postgres")
    |> pgl.password("postgres")
    |> pgl.host("::1")
    |> pgl.ip_version(pgl.Ipv6)
    |> pgl.new

  let assert Ok(_) = pgl.start(db)
}

// pub fn inserting_new_rows_ssl_test() {
//   use conn <- start_ssl()
// 
//   inserting_new_rows(conn)
// }

pub fn inserting_new_rows_and_returning_test() {
  use conn <- connect()

  let assert Ok(returned) =
    insert_into_users([
      "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
      "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
    ])
    |> returning(["name"])
    |> pgl.query([], conn)

  assert 2 == returned.count
  assert returned.rows
    == [
      dynamic.array([dynamic.string("William")]),
      dynamic.array([dynamic.string("Stephen")]),
    ]
}

pub fn pipeline_multiple_query_test() {
  use conn <- connect()

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

  let params1 = [pg_value.Text("Margaret"), pg_value.Bool(True)]

  let params2 = [pg_value.Text("Richard"), pg_value.Bool(False)]

  let assert Ok(_) =
    [pgl.Query(insert1, params1), pgl.Query(insert2, params2)]
    |> pgl.batch(conn)
}

pub fn pipeline_multiple_different_queries_test() {
  use conn <- connect()

  let insert1 =
    insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> returning(["name", "active", "nicknames"])

  let params1 = [pg_value.Text("Margaret"), pg_value.Bool(True)]

  let assert Ok(_) =
    [pgl.Query(insert1, params1), pgl.Query("SELECT 1", [])]
    |> pgl.batch(conn)
}

pub fn pipeline_dependent_queries_test() {
  let drop1 = "DROP TABLE IF EXISTS new_users;"
  let drop2 = "DROP TABLE IF EXISTS posts;"
  let drop3 = "DROP TABLE IF EXISTS comments;"
  let drop4 = "DROP TABLE IF EXISTS tags;"

  let create1 =
    "CREATE TABLE IF NOT EXISTS new_users (
     id SERIAL PRIMARY KEY,
     name VARCHAR(50) NOT NULL
   );"
  let create2 =
    "CREATE TABLE IF NOT EXISTS posts (
     id SERIAL PRIMARY KEY,
     user_id INTEGER NOT NULL,
     title TEXT NOT NULL,
     content TEXT NOT NULL
   );"
  let create3 =
    "CREATE TABLE IF NOT EXISTS comments (
     id SERIAL PRIMARY KEY,
     post_id INTEGER NOT NULL,
     content TEXT NOT NULL
   );"
  let create4 =
    "CREATE TABLE IF NOT EXISTS tags (
     id SERIAL PRIMARY KEY,
     post_id INTEGER NOT NULL,
     name VARCHAR(20) NOT NULL
   );"

  use conn <- with_conn()

  let assert Ok(_) = pgl.execute(drop1, conn)
  let assert Ok(_) = pgl.execute(drop2, conn)
  let assert Ok(_) = pgl.execute(drop3, conn)
  let assert Ok(_) = pgl.execute(drop4, conn)

  let assert Ok(_) = pgl.execute(create1, conn)
  let assert Ok(_) = pgl.execute(create2, conn)
  let assert Ok(_) = pgl.execute(create3, conn)
  let assert Ok(_) = pgl.execute(create4, conn)

  // create users

  let create_user_sql = "INSERT INTO new_users (name) VALUES ($1) RETURNING id"

  let q1 = pgl.Query(create_user_sql, params: [pg_value.text("Jim")])
  let q2 = pgl.Query(create_user_sql, params: [pg_value.text("Will")])
  let q3 = pgl.Query(create_user_sql, params: [pg_value.text("Jean-Luc")])

  let assert Ok(queried) = pgl.batch([q1, q2, q3], conn)

  let assert Ok(user_ids) =
    queried
    |> list.try_map(fn(queried) {
      queried.rows
      |> list.try_map(fn(row) {
        decode.run(row, {
          use id <- decode.field(0, decode.int)
          decode.success(id)
        })
      })
    })

  let user_ids = list.flatten(user_ids)

  // create posts

  let create_post_sql =
    "INSERT INTO posts (user_id, title, content) VALUES ($1, $2, $3) RETURNING id"

  let post_queries =
    list.flat_map(user_ids, fn(user_id) {
      [
        pgl.Query(create_post_sql, params: [
          pg_value.int(user_id),
          pg_value.text("Unique title 1"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, params: [
          pg_value.int(user_id),
          pg_value.text("Unique title 2"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, params: [
          pg_value.int(user_id),
          pg_value.text("Unique title 3"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, params: [
          pg_value.int(user_id),
          pg_value.text("Unique title 4"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, params: [
          pg_value.int(user_id),
          pg_value.text("Unique title 5"),
          pg_value.text("Unique content"),
        ]),
      ]
    })

  let assert Ok(queried) = pgl.batch(post_queries, conn)

  let assert Ok(post_ids) =
    queried
    |> list.try_map(fn(queried) {
      queried.rows
      |> list.try_map(fn(row) {
        decode.run(row, {
          use id <- decode.field(0, decode.int)
          decode.success(id)
        })
      })
    })

  let post_ids = list.flatten(post_ids)

  // create comments and tags

  let create_comment_sql =
    "INSERT INTO comments (post_id, content) VALUES ($1, $2) RETURNING *"
  let create_tag_sql =
    "INSERT INTO tags (post_id, name) VALUES ($1, $2) RETURNING *"

  let comment_and_tag_queries =
    list.flat_map(post_ids, fn(post_id) {
      [
        pgl.Query(create_comment_sql, params: [
          pg_value.int(post_id),
          pg_value.text("Unique comment 1"),
        ]),
        pgl.Query(create_comment_sql, params: [
          pg_value.int(post_id),
          pg_value.text("Unique comment 2"),
        ]),
        pgl.Query(create_tag_sql, params: [
          pg_value.int(post_id),
          pg_value.text("blog"),
        ]),
        pgl.Query(create_tag_sql, params: [
          pg_value.int(post_id),
          pg_value.text("mid"),
        ]),
      ]
    })

  let assert Ok(queried) = pgl.batch(comment_and_tag_queries, conn)

  let assert Ok(_data) =
    queried
    |> list.try_map(fn(queried) {
      queried.rows
      |> list.try_map(fn(row) {
        decode.run(row, {
          use id <- decode.field(0, decode.int)
          use post_id <- decode.field(1, decode.int)
          use text <- decode.field(2, decode.string)

          decode.success(#(id, post_id, text))
        })
      })
    })
}

fn insert_into_users(values: List(String)) -> String {
  let values_str = string.join(values, "), (")

  "INSERT INTO users VALUES (" <> values_str <> ")"
}

fn returning(sql: String, columns: List(String)) -> String {
  sql <> " RETURNING " <> string.join(columns, ", ")
}

pub fn rows_as_maps_test() {
  let db = global_pool_rows_as_maps()

  with_setup_conn(db, fn(conn) {
    let sql =
      insert_into_users([
        "DEFAULT, 'James', true, ARRAY['Jim'], '2233-04-22', '2263-01-09 11:30:22'",
        "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
        "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
      ])

    let assert Ok(count) = pgl.execute(sql, conn)
    assert 3 == count

    let assert Ok(queried) =
      "SELECT * FROM users"
      |> pgl.query([], conn)

    assert 3 == queried.count

    let assert Ok([james, william, steve]) =
      queried.rows
      |> list.try_map(decode.run(_, user_with_fields_decoder()))

    assert 1 == james.id
    assert 2 == william.id
    assert 3 == steve.id
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
  use conn <- connect()
  let sql =
    insert_into_users([
      "DEFAULT, 'James', true, ARRAY['Jim'], '2233-04-22', '2263-01-09 11:30:22'",
    ])

  let assert Ok(count) = pgl.execute(sql, conn)

  assert 1 == count

  let assert Ok(returned) =
    pgl.query(
      "SELECT * FROM users WHERE name = $1",
      [pg_value.Text("James")],
      conn,
    )

  assert 1 == returned.count

  let assert Ok(james) =
    returned.rows
    |> list.first

  let assert Ok(ts) = timestamp.parse_rfc3339("2263-01-09T11:30:22Z")
  let #(seconds, nanoseconds) = timestamp.to_unix_seconds_and_nanoseconds(ts)

  let microseconds = { seconds * 1_000_000 } + { nanoseconds / 1000 }

  assert james
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
  use conn <- with_conn()

  let sql = "SELECT $1::VARCHAR, $2::VARCHAR, $3::VARCHAR"
  let params = [
    pg_value.Text("howdy"),
    pg_value.Text(""),
    pg_value.Text("postgres"),
  ]

  let assert Ok(result) = pgl.query(sql, params, conn)

  assert 1 == result.count

  assert result.rows
    == [
      dynamic.array([
        dynamic.string("howdy"),
        dynamic.string(""),
        dynamic.string("postgres"),
      ]),
    ]
}

pub fn null_encoding_test() {
  use conn <- with_conn()

  let sql = "SELECT $1::TEXT, $1 IS NULL, $2::INT"
  let params = [pg_value.null, pg_value.int(42)]

  let assert Ok(result) = pgl.query(sql, params, conn)

  assert 1 == result.count

  assert result.rows
    == [
      dynamic.array([dynamic.string(""), dynamic.bool(True), dynamic.int(42)]),
    ]
}

pub fn interval_encoding_test() {
  use conn <- with_conn()

  let sql = "SELECT 'P14MT86430S'::INTERVAL"

  let assert Ok(result) = pgl.query(sql, [], conn)

  assert result.rows
    == [
      dynamic.array([
        dynamic.array([
          dynamic.int(14),
          dynamic.int(0),
          dynamic.int(86_430_000_000),
        ]),
      ]),
    ]
}

pub fn interval_roundtrip_test() {
  use conn <- with_conn()

  let sql = "SELECT $1::INTERVAL"

  let interval =
    interval.months(4)
    |> interval.add(interval.days(3))
    |> interval.add(interval.seconds(30))
    |> interval.add(interval.microseconds(500_000))

  let assert Ok(queried) = pgl.query(sql, [pg_value.interval(interval)], conn)

  let decoder = {
    use interval <- decode.field(0, interval.decoder())
    decode.success(interval)
  }

  let assert Ok([
    interval.Interval(months: 4, days: 3, seconds: 30, microseconds: 500_000),
  ]) =
    queried.rows
    |> list.try_map(decode.run(_, decoder))
}

pub fn array_encoding_test() {
  use conn <- with_conn()

  let sql =
    "SELECT ARRAY['howdy', 'postgres']::TEXT[], ARRAY[1, 2, 3]::INT[], ARRAY[]::TEXT[]"
  let params = []

  let assert Ok(result) = pgl.query(sql, params, conn)

  assert 1 == result.count

  let row_decoder = {
    use text_array <- decode.field(0, decode.list(of: decode.string))
    use int_array <- decode.field(1, decode.list(of: decode.int))
    use empty_array <- decode.field(2, decode.list(of: decode.string))

    decode.success(#(text_array, int_array, empty_array))
  }

  let assert Ok([#(text, int, empty)]) =
    result.rows
    |> list.try_map(decode.run(_, row_decoder))

  assert ["howdy", "postgres"] == text
  assert [1, 2, 3] == int
  assert [] == empty
}

pub fn mixed_types_with_encoding_test() {
  use conn <- connect()

  let sql =
    insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> returning(["name", "active", "nicknames"])

  let params = [pg_value.Text("Margaret"), pg_value.Bool(True)]

  let assert Ok(result) = pgl.query(sql, params, conn)

  assert 1 == result.count

  let row_decoder = {
    use name <- decode.field(0, decode.string)
    use active <- decode.field(1, decode.bool)
    use nicknames <- decode.field(2, decode.list(of: decode.string))

    decode.success(#(name, active, nicknames))
  }

  let assert Ok([#(name, active, nicknames)]) =
    result.rows
    |> list.try_map(decode.run(_, row_decoder))

  assert "Margaret" == name
  assert active
  assert ["Peggy"] == nicknames
}

pub fn error_handling_test() {
  use conn <- with_conn()

  let sql = "SELECT * FROM non_existent_table"
  let params = []

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.query(sql, params, conn)

  assert "42P01" == code
  assert "undefined_table" == name
  assert "relation \"non_existent_table\" does not exist" == message
}

pub fn invalid_sql_test() {
  use conn <- with_conn()
  let sql = "select       select"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.execute(sql, conn)

  assert "42601" == code
  assert "syntax_error" == name
  assert "syntax error at or near \"select\"" == message
}

pub fn insert_constraint_error_test() {
  use conn <- connect()

  let assert Error(pgl.PostgresError(code:, name:, message:, fields:)) =
    insert_into_users([
      "900, 'William', false, ARRAY['William', 'Will'], '1990-02-09', now()",
      "900, 'Stephen', true, ARRAY['Steve'], '1993-01-01', now()",
    ])
    |> pgl.execute(conn)

  let assert "23505" = code
  let assert "unique_violation" = name
  let assert "duplicate key value violates unique constraint \"users_pkey\"" =
    message

  let assert Ok(constraint) = dict.get(fields, pgl.Constraint)
  assert "users_pkey" == constraint

  let assert Ok(detail) = dict.get(fields, pgl.Detail)
  assert "Key (id)=(900) already exists." == detail

  let assert Ok(table) = dict.get(fields, pgl.Table)
  assert "users" == table

  let assert Ok(schema) = dict.get(fields, pgl.Schema)
  assert "public" == schema
}

pub fn select_from_unknown_table_test() {
  use conn <- with_conn()
  let sql = "SELECT * FROM unknown"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.execute(sql, conn)

  assert "42P01" == code
  assert "undefined_table" == name
  assert "relation \"unknown\" does not exist" == message
}

pub fn insert_with_incorrect_type_test() {
  use conn <- connect()

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    insert_into_users(["true, true, true, true"])
    |> pgl.execute(conn)

  assert "42804" == code
  assert "datatype_mismatch" == name
  assert "column \"id\" is of type integer but expression is of type boolean"
    == message
}

pub fn execute_with_wrong_number_of_arguments_test() {
  use conn <- connect()
  let sql = "SELECT * FROM users WHERE id = $1"

  let assert Error(pgl.ProtocolError(
    "(ProcessingError) Failed to describe statement parameters",
  )) = pgl.execute(sql, conn)
}

pub fn insert_with_values_test() {
  use conn <- connect()

  let sql =
    "INSERT INTO users (name, nicknames, birthday, created_at) VALUES ($1, $2, $3, $4)"

  let values = [
    pg_value.text("Richard"),
    pg_value.array(["Dick", "Robin", "Nightwing"], of: pg_value.text),
    pg_value.date(calendar.Date(2011, calendar.March, 20)),
    pg_value.timestamp(timestamp.system_time()),
  ]

  let assert Ok(_) = pgl.query(sql, values, conn)
}

pub fn transaction_commit_test() {
  use conn <- connect()

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

  assert id1 == got1
  assert id2 == got2
}

pub fn transaction_rollback_test() {
  use conn <- connect()

  setup_users_table(conn)

  use tx <- result.map(pgl.begin(conn))

  let _id1 = insert_into_users_table(tx, "two")
  let _id2 = insert_into_users_table(tx, "three")

  let assert Ok(conn) = pgl.rollback(tx)

  let assert Ok(queried) =
    "SELECT * FROM users"
    |> pgl.query([], conn)

  assert 0 == queried.count
}

pub fn transaction_error_test() {
  use conn <- connect()

  let assert Ok(_) =
    "DROP TABLE IF EXISTS tx_test"
    |> pgl.execute(conn)

  let assert Ok(_) =
    "CREATE TABLE tx_test (id INTEGER PRIMARY KEY, name TEXT)"
    |> pgl.execute(conn)

  let assert Ok(_queried) =
    "INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *"
    |> pgl.query([pg_value.int(1), pg_value.text("Before")], conn)

  let assert Ok(queried) =
    "SELECT COUNT(*) FROM tx_test"
    |> pgl.query([], conn)

  assert 1 == queried.count

  let assert Error(pgl.RollbackError(pgl.PostgresError(code, name, ..))) = {
    use tx <- pgl.transaction(conn)

    let assert Ok(_queried) =
      "INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *"
      |> pgl.query([pg_value.int(2), pg_value.text("Transaction")], tx)

    "INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *"
    |> pgl.query([pg_value.int(1), pg_value.text("Duplicate")], tx)
  }

  assert "23505" == code
  assert "unique_violation" == name

  let assert Ok(queried) =
    "SELECT COUNT(*) FROM tx_test"
    |> pgl.query([], conn)

  assert 1 == queried.count
}

pub fn savepoint_test() {
  use conn <- connect()

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
  use conn <- connect()

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

            Error("Nah")
          })

        Ok(id2)
      })
      |> result.map(fn(id2) { #(id1, id2) })
    })

  let assert Ok(queried) =
    "SELECT * FROM users WHERE name IN ('one', 'two', 'three')"
    |> pgl.query([], conn)

  assert 2 == queried.count
}

// Transaction helper functions

fn setup_users_table(conn: pgl.Connection) {
  let assert Ok(_) = pgl.execute("truncate table users", conn)

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
