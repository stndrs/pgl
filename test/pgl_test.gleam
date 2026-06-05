import gleam/bool
import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/order
import gleam/result
import gleam/time/calendar
import gleam/time/timestamp
import gleeunit
import neon/ssl
import pg_value
import pg_value/interval
import pgl
import pgl/internal
import pgl/support/db
import pgl/support/sql
import pgl/support/user

pub fn main() {
  let assert Ok(_) = ssl.start()

  gleeunit.main()
}

pub fn parse_url_test() {
  let assert Ok(conf) =
    "postgres://postgres:supersecretpassword@localhost:5433/pgl_test"
    |> pgl.from_url

  let expected =
    pgl.config
    |> pgl.host("localhost")
    |> pgl.port(5433)
    |> pgl.database("pgl_test")
    |> pgl.username("postgres")
    |> pgl.password("supersecretpassword")
    |> pgl.ssl(pgl.SslVerified)

  assert expected == conf
}

pub fn parse_url_alternative_schema_test() {
  let assert Ok(conf) =
    "postgresql://postgres:supersecretpassword@localhost:5433/pgl_test"
    |> pgl.from_url

  let expected =
    pgl.config
    |> pgl.host("localhost")
    |> pgl.port(5433)
    |> pgl.database("pgl_test")
    |> pgl.username("postgres")
    |> pgl.password("supersecretpassword")
    |> pgl.ssl(pgl.SslVerified)

  assert expected == conf
}

pub fn parse_url_invalid_protocol_test() {
  let assert Error(Nil) =
    pgl.from_url("mysql://u:supersecretpassword@localhost:5432/pgl_test")
}

pub fn parse_url_invalid_path_test() {
  let assert Error(Nil) =
    pgl.from_url("postgres://username:pass@db:5432/some/path")
}

pub fn parse_url_ssl_mode_require_test() {
  let assert Ok(conf) =
    "postgres://username:pass@localhost:5432/pgl_test?sslmode=require"
    |> pgl.from_url

  let expected =
    pgl.config
    |> pgl.host("localhost")
    |> pgl.port(5432)
    |> pgl.database("pgl_test")
    |> pgl.username("username")
    |> pgl.password("pass")
    |> pgl.ssl(pgl.SslUnverified)

  assert expected == conf
}

pub fn parse_url_ssl_mode_verify_test() {
  let assert Ok(conf) =
    "postgres://username:pass@localhost:5432/pgl_test?sslmode=verify-ca"
    |> pgl.from_url

  let expected =
    pgl.config
    |> pgl.host("localhost")
    |> pgl.port(5432)
    |> pgl.database("pgl_test")
    |> pgl.username("username")
    |> pgl.password("pass")
    |> pgl.ssl(pgl.SslVerified)

  assert expected == conf

  let assert Ok(conf) =
    "postgres://username:pass@localhost:5432/pgl_test?sslmode=verify-full"
    |> pgl.from_url

  assert expected == conf
}

pub fn query_params_test() {
  let query =
    pgl.sql("SELECT * FROM users WHERE id=$1")
    |> pgl.values([pg_value.int(10)])

  let assert [pg_value.Int(10)] = query.values
}

// ---------- Query Tests ---------- //

fn inserting_new_rows(conn: pgl.Connection) {
  let assert Ok(returned) =
    sql.insert_into_users([
      "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
      "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
    ])
    |> sql.returning(["*"])
    |> pgl.sql
    |> pgl.query(conn)

  assert 2 == returned.count

  let assert Ok([william, stephen]) =
    returned.rows
    |> list.try_map(fn(row) { decode.run(row, user.decoder()) })

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
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  inserting_new_rows(conn)
}

pub fn ipv6_test() {
  let db =
    pgl.config
    |> pgl.database("pgl_test")
    |> pgl.username("postgres")
    |> pgl.password("postgres")
    |> pgl.host("::1")
    |> pgl.ip_version(pgl.Ipv6)
    |> pgl.ssl(pgl.SslUnverified)
    |> pgl.new

  let assert Ok(_) = pgl.start(db)
}

pub fn inserting_new_rows_and_returning_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(returned) =
    sql.insert_into_users([
      "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
      "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
    ])
    |> sql.returning(["name"])
    |> pgl.sql
    |> pgl.query(conn)

  assert 2 == returned.count
  assert returned.rows
    == [
      dynamic.array([dynamic.string("William")]),
      dynamic.array([dynamic.string("Stephen")]),
    ]
}

pub fn pipeline_multiple_query_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let insert1 =
    sql.insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> sql.returning(["name", "active", "nicknames"])

  let insert2 =
    sql.insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Dick', 'Robin', 'Nightwing'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> sql.returning(["name", "active", "nicknames"])

  let params1 = [pg_value.Text("Margaret"), pg_value.Bool(True)]

  let params2 = [pg_value.Text("Richard"), pg_value.Bool(False)]

  let assert Ok(_) =
    [pgl.Query(insert1, params1), pgl.Query(insert2, params2)]
    |> pgl.batch(conn)
}

pub fn pipeline_multiple_different_queries_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let insert1 =
    sql.insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> sql.returning(["name", "active", "nicknames"])

  let params1 = [pg_value.Text("Margaret"), pg_value.Bool(True)]

  let assert Ok(_) =
    [pgl.Query(insert1, params1), pgl.Query("SELECT 1", [])]
    |> pgl.batch(conn)
}

pub fn pipeline_batch_partial_failure_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let insert1 =
    sql.insert_into_users([
      "900, 'William', false, ARRAY['Will'], '1990-02-09', now()",
    ])

  let insert2 =
    sql.insert_into_users([
      "901, 'William', false, ARRAY['Will'], '1990-02-09', now()",
    ])

  let insert3 =
    sql.insert_into_users([
      "902, 'William', false, ARRAY['Will'], '1990-02-09', now()",
    ])

  let assert Error(pgl.PostgresError(code:, name:, message:, fields:)) =
    [
      pgl.Query(insert1, []),
      pgl.Query(insert2, []),
      pgl.Query(insert3, []),
      pgl.Query(insert1, []),
    ]
    |> pgl.batch(conn)

  let assert "23505" = code
  let assert "unique_violation" = name
  let assert "duplicate key value violates unique constraint \"users_pkey\"" =
    message

  let assert Ok("users_pkey") = dict.get(fields, pgl.Constraint)
  let assert Ok("Key (id)=(900) already exists.") = dict.get(fields, pgl.Detail)
}

pub fn pipeline_dependent_queries_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  // Tables already exist (created by schema.setup_all), truncated by with_conn

  // create users
  let create_user_sql = "INSERT INTO new_users (name) VALUES ($1) RETURNING id"

  let q1 = pgl.Query(create_user_sql, values: [pg_value.text("Jim")])
  let q2 = pgl.Query(create_user_sql, values: [pg_value.text("Will")])
  let q3 = pgl.Query(create_user_sql, values: [pg_value.text("Jean-Luc")])

  let assert Ok(queried) = pgl.batch([q1, q2, q3], conn)

  let assert Ok(user_ids) =
    queried
    |> list.try_map(fn(queried) {
      queried.rows
      |> list.try_map(decode.run(_, db.id_decoder()))
    })

  let user_ids = list.flatten(user_ids)

  // create posts
  let create_post_sql =
    "INSERT INTO posts (user_id, title, content) VALUES ($1, $2, $3) RETURNING id"

  let post_queries =
    list.flat_map(user_ids, fn(user_id) {
      [
        pgl.Query(create_post_sql, values: [
          pg_value.int(user_id),
          pg_value.text("Unique title 1"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, values: [
          pg_value.int(user_id),
          pg_value.text("Unique title 2"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, values: [
          pg_value.int(user_id),
          pg_value.text("Unique title 3"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, values: [
          pg_value.int(user_id),
          pg_value.text("Unique title 4"),
          pg_value.text("Unique content"),
        ]),
        pgl.Query(create_post_sql, values: [
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
      |> list.try_map(decode.run(_, db.id_decoder()))
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
        pgl.Query(create_comment_sql, values: [
          pg_value.int(post_id),
          pg_value.text("Unique comment 1"),
        ]),
        pgl.Query(create_comment_sql, values: [
          pg_value.int(post_id),
          pg_value.text("Unique comment 2"),
        ]),
        pgl.Query(create_tag_sql, values: [
          pg_value.int(post_id),
          pg_value.text("blog"),
        ]),
        pgl.Query(create_tag_sql, values: [
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

pub fn rows_as_maps_test() {
  use db <- db.with_db_rows_as_maps()
  use conn <- db.with_conn(db)

  let sql =
    sql.insert_into_users([
      "DEFAULT, 'James', true, ARRAY['Jim'], '2233-04-22', '2263-01-09 11:30:22'",
      "DEFAULT, 'William', false, ARRAY['William', 'Will'], '1990-02-09', '2025-09-30 09:17:30.100'",
      "DEFAULT, 'Stephen', true, ARRAY['Steve'], '1993-01-01', '2025-01-06 20:01:06.000'",
    ])

  let assert Ok(count) = pgl.execute(sql, conn)
  assert 3 == count

  let assert Ok(queried) =
    pgl.sql("SELECT * FROM users")
    |> pgl.query(conn)

  assert 3 == queried.count

  let assert Ok([james, william, steve]) =
    queried.rows
    |> list.try_map(decode.run(_, user.fields_decoder()))

  assert 1 == james.id
  assert 2 == william.id
  assert 3 == steve.id
}

pub fn selecting_rows_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql =
    sql.insert_into_users([
      "DEFAULT, 'James', true, ARRAY['Jim'], '2233-04-22', '2263-01-09 11:30:22'",
    ])

  let assert Ok(count) = pgl.execute(sql, conn)

  assert 1 == count

  let assert Ok(returned) =
    pgl.sql("SELECT * FROM users WHERE name = $1")
    |> pgl.values([pg_value.Text("James")])
    |> pgl.query(conn)

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
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql = "SELECT $1::VARCHAR, $2::VARCHAR, $3::VARCHAR"
  let params = [
    pg_value.Text("howdy"),
    pg_value.Text(""),
    pg_value.Text("postgres"),
  ]

  let assert Ok(result) =
    pgl.sql(sql)
    |> pgl.values(params)
    |> pgl.query(conn)

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
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql = "SELECT $1::TEXT, $1 IS NULL, $2::INT"
  let params = [pg_value.null, pg_value.int(42)]

  let assert Ok(result) =
    pgl.sql(sql)
    |> pgl.values(params)
    |> pgl.query(conn)

  assert 1 == result.count

  assert result.rows
    == [
      dynamic.array([dynamic.nil(), dynamic.bool(True), dynamic.int(42)]),
    ]
}

pub fn uuid_v4_encoding_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let v4_uuid = 0x85eab1c37acc4d8288e45fc1a9daa9d8

  let sql = "SELECT $1::UUID"
  let params = [pg_value.uuid(<<v4_uuid:big-int-size(128)>>)]

  let assert Ok(result) =
    pgl.sql(sql)
    |> pgl.values(params)
    |> pgl.query(conn)

  assert 1 == result.count
}

pub fn uuid_v4_decoding_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql = "SELECT gen_random_uuid()"

  let assert Ok(result) =
    pgl.sql(sql)
    |> pgl.query(conn)

  assert 1 == result.count
}

// Postgres 18 provides native uuid v7 support
pub fn uuid_v7_decoding_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let pg_version = server_version(conn)

  // Only run if pg_version is 18 or higher
  use <- bool.guard(when: pg_version < 180_000, return: Nil)

  let assert Ok(result) =
    "SELECT uuidv7()"
    |> pgl.sql
    |> pgl.query(conn)

  assert 1 == result.count
}

fn server_version(conn: pgl.Connection) -> Int {
  let assert Ok(queried) =
    "SHOW server_version_num"
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(version) =
    decode.run(row, {
      use ver <- decode.field(0, decode.string)
      decode.success(ver)
    })

  let assert Ok(version) = int.parse(version)

  version
}

pub fn uuid_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let v4_uuid = 0x85eab1c37acc4d8288e45fc1a9daa9d8

  let assert Ok(result) =
    "INSERT INTO uuid_test (identifier) VALUES ($1)"
    |> pgl.sql
    |> pgl.values([pg_value.uuid(<<v4_uuid:big-int-size(128)>>)])
    |> pgl.query(conn)

  assert 1 == result.count

  let assert Ok(result) =
    "SELECT * FROM uuid_test"
    |> pgl.sql
    |> pgl.query(conn)

  assert 1 == result.count
}

pub fn hstore_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let data =
    dict.new()
    |> dict.insert("first", Some("foo"))
    |> dict.insert("second", Some("bar"))
    |> dict.insert("third", None)

  let hstore = pg_value.hstore(data)

  let assert Ok(queried) =
    "INSERT INTO hstore_test (data) VALUES ($1) RETURNING id"
    |> pgl.sql
    |> pgl.values([hstore])
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM hstore_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use hstore <- decode.field(1, {
        decode.dict(decode.string, decode.optional(decode.string))
      })

      decode.success(hstore)
    })

  assert data == decoded
}

pub fn hstore_string_constant_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let data =
    dict.new()
    |> dict.insert("first", Some("foo"))
    |> dict.insert("second", Some("bar"))
    |> dict.insert("third", None)

  let hstore = pg_value.hstore(data)

  let val = pg_value.to_string(hstore)

  assert "'\"first\"=>\"foo\", \"second\"=>\"bar\", \"third\"=>NULL'" == val

  let assert Ok(queried) =
    { "INSERT INTO hstore_test (data) VALUES (" <> val <> ") RETURNING id" }
    |> pgl.sql
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM hstore_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use hstore <- decode.field(1, {
        decode.dict(decode.string, decode.optional(decode.string))
      })

      decode.success(hstore)
    })

  assert data == decoded
}

pub fn hstore_string_constant_escape_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let data =
    dict.new()
    |> dict.insert("firs't", Some("foo"))
    |> dict.insert("sec\\ond", Some("b\"ar"))
    |> dict.insert("third", None)

  let hstore = pg_value.hstore(data)

  let val = pg_value.to_string(hstore)

  assert "'\"firs''t\"=>\"foo\", \"sec\\\\ond\"=>\"b\\\"ar\", \"third\"=>NULL'"
    == val

  let assert Ok(queried) =
    { "INSERT INTO hstore_test (data) VALUES (" <> val <> ") RETURNING id" }
    |> pgl.sql
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM hstore_test WHERE id =" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use hstore <- decode.field(1, {
        decode.dict(decode.string, decode.optional(decode.string))
      })

      decode.success(hstore)
    })

  assert data == decoded
}

pub fn interval_encoding_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql = "SELECT 'P14MT86430S'::INTERVAL"

  let assert Ok(result) = pgl.sql(sql) |> pgl.query(conn)

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
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql = "SELECT $1::INTERVAL"

  let interval =
    interval.months(4)
    |> interval.add(interval.days(3))
    |> interval.add(interval.seconds(30))
    |> interval.add(interval.microseconds(500_000))

  let assert Ok(queried) =
    pgl.sql(sql)
    |> pgl.values([pg_value.interval(interval)])
    |> pgl.query(conn)

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
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql =
    "SELECT ARRAY['howdy', 'postgres']::TEXT[], ARRAY[1, 2, 3]::INT[], ARRAY[]::TEXT[]"

  let assert Ok(result) = pgl.sql(sql) |> pgl.query(conn)

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
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql =
    sql.insert_into_users([
      "DEFAULT, $1, $2, ARRAY['Peggy'], '1993-08-27', '2025-06-16 00:00:00.100'",
    ])
    |> sql.returning(["name", "active", "nicknames"])

  let params = [pg_value.Text("Margaret"), pg_value.Bool(True)]

  let assert Ok(result) =
    pgl.sql(sql)
    |> pgl.values(params)
    |> pgl.query(conn)

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
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql = "SELECT * FROM non_existent_table"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.sql(sql) |> pgl.query(conn)

  assert "42P01" == code
  assert "undefined_table" == name
  assert "relation \"non_existent_table\" does not exist" == message
}

pub fn invalid_sql_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)
  let sql = "select       select"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.execute(sql, conn)

  assert "42601" == code
  assert "syntax_error" == name
  assert "syntax error at or near \"select\"" == message
}

pub fn insert_constraint_error_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Error(pgl.PostgresError(code:, name:, message:, fields:)) =
    sql.insert_into_users([
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
  use db <- db.with_db()
  use conn <- db.with_conn(db)
  let sql = "SELECT * FROM unknown"

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    pgl.execute(sql, conn)

  assert "42P01" == code
  assert "undefined_table" == name
  assert "relation \"unknown\" does not exist" == message
}

pub fn insert_with_incorrect_type_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Error(pgl.PostgresError(code:, name:, message:, fields: _)) =
    sql.insert_into_users(["true, true, true, true"])
    |> pgl.execute(conn)

  assert "42804" == code
  assert "datatype_mismatch" == name
  assert "column \"id\" is of type integer but expression is of type boolean"
    == message
}

pub fn execute_with_wrong_number_of_arguments_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let sql = "SELECT * FROM users WHERE id = $1"

  let assert Error(pgl.ProtocolError(
    "[ProcessingError] Failed to describe statement parameters",
  )) = pgl.execute(sql, conn)
}

pub fn insert_with_values_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let query =
    "INSERT INTO users (name, nicknames, birthday, created_at) VALUES ($1, $2, $3, $4)"
    |> pgl.sql
    |> pgl.values([
      pg_value.text("Richard"),
      pg_value.array(["Dick", "Robin", "Nightwing"], of: pg_value.text),
      pg_value.date(calendar.Date(2011, calendar.March, 20)),
      pg_value.timestamp(timestamp.system_time()),
    ])

  let assert Ok(_) = pgl.query(query, conn)
}

// ---------- Transaction Tests ---------- //

pub fn begin_commit_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(conn) = pgl.begin(conn)

  let assert Ok(_conn) = pgl.commit(conn)
}

pub fn begin_rollback_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(conn) = pgl.begin(conn)

  let assert Ok(_conn) = pgl.rollback(conn)
}

pub fn commit_error_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Error(pgl.NotInTransaction) = pgl.commit(conn)
}

pub fn rollback_error_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Error(pgl.NotInTransaction) = pgl.rollback(conn)
}

pub fn transaction_commit_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(#(id1, id2)) = {
    use tx <- result.map(pgl.begin(conn))

    let id1 = insert_into_users_table(tx, "one")
    let id2 = insert_into_users_table(tx, "two")

    let assert Ok(_) = pgl.commit(tx)

    #(id1, id2)
  }

  let assert Ok(queried) =
    pgl.sql("SELECT id FROM users ORDER BY id") |> pgl.query(conn)

  let assert Ok([got1, got2]) =
    queried.rows
    |> list.try_map(decode.run(_, db.id_decoder()))

  assert id1 == got1
  assert id2 == got2
}

pub fn transaction_rollback_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(tx) = pgl.begin(conn)

  let _id1 = insert_into_users_table(tx, "two")
  let _id2 = insert_into_users_table(tx, "three")

  let assert Ok(conn) = pgl.rollback(tx)

  let assert Ok(queried) =
    pgl.sql("SELECT * FROM users")
    |> pgl.query(conn)

  assert 0 == queried.count
}

pub fn transaction_exception_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Error(_) = {
    use <- internal.with_rescue()
    use tx <- pgl.transaction(conn)

    let _id1 = insert_into_users_table(tx, "two")
    let _id2 = insert_into_users_table(tx, "three")

    panic as "transaction failure!"
  }

  let assert Ok(queried) =
    pgl.sql("SELECT * FROM users")
    |> pgl.query(conn)

  assert 0 == queried.count
}

pub fn transaction_error_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(_queried) =
    pgl.sql("INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *")
    |> pgl.values([pg_value.int(1), pg_value.text("Before")])
    |> pgl.query(conn)

  let assert Ok(queried) =
    pgl.sql("SELECT COUNT(*) FROM tx_test")
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Error(pgl.RollbackError(pgl.PostgresError(code, name, ..))) = {
    use tx <- pgl.transaction(conn)

    let assert Ok(_queried) =
      pgl.sql("INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *")
      |> pgl.values([pg_value.int(2), pg_value.text("Transaction")])
      |> pgl.query(tx)

    pgl.sql("INSERT INTO tx_test (id, name) VALUES ($1, $2) RETURNING *")
    |> pgl.values([pg_value.int(1), pg_value.text("Duplicate")])
    |> pgl.query(tx)
  }

  assert "23505" == code
  assert "unique_violation" == name

  let assert Ok(queried) =
    pgl.sql("SELECT COUNT(*) FROM tx_test")
    |> pgl.query(conn)

  assert 1 == queried.count
}

pub fn savepoint_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(_) =
    pgl.transaction(conn, fn(tx) {
      let id1 = insert_into_users_table(tx, "one")

      let assert Ok(_) = pgl.sql("SELECT 1") |> pgl.query(tx)

      pgl.savepoint(tx, fn(tx2) {
        let id2 = insert_into_users_table(tx2, "two")

        let assert Ok(_) = pgl.sql("SELECT 1") |> pgl.query(tx2)

        Ok(id2)
      })
      |> result.map(fn(id2) { #(id1, id2) })
    })
}

pub fn savepoint_release_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(_) =
    pgl.transaction(conn, fn(tx) {
      let id1 = insert_into_users_table(tx, "one")

      let assert Ok(_) = pgl.sql("SELECT 1") |> pgl.query(tx)

      pgl.savepoint(tx, fn(tx2) {
        let id2 = insert_into_users_table(tx2, "two")

        let assert Ok(_) = pgl.sql("SELECT 2") |> pgl.query(tx2)

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
    pgl.sql("SELECT * FROM users WHERE name IN ('one', 'two', 'three')")
    |> pgl.query(conn)

  assert 2 == queried.count
}

pub fn rollback_savepoint_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(_) =
    pgl.transaction(conn, fn(tx) {
      let id1 = insert_into_users_table(tx, "one")

      let assert Ok(_) = pgl.sql("SELECT 1") |> pgl.query(tx)

      pgl.savepoint(tx, fn(tx2) {
        let id2 = insert_into_users_table(tx2, "two")

        let assert Ok(_) = pgl.sql("SELECT 2") |> pgl.query(tx2)

        let assert Ok(_) =
          pgl.savepoint(tx2, fn(tx3) {
            let id3 = insert_into_users_table(tx3, "three")

            let assert order.Gt = int.compare(id3, id2)

            pgl.rollback(tx3)
          })

        Ok(id2)
      })
      |> result.map(fn(id2) { #(id1, id2) })
    })

  let assert Ok(queried) =
    pgl.sql("SELECT * FROM users WHERE name IN ('one', 'two', 'three')")
    |> pgl.query(conn)

  assert 2 == queried.count
}

pub fn savepoint_exception_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(_) =
    pgl.transaction(conn, fn(tx) {
      let id1 = insert_into_users_table(tx, "one")

      let assert Ok(_) = pgl.sql("SELECT 1") |> pgl.query(tx)

      pgl.savepoint(tx, fn(tx2) {
        let id2 = insert_into_users_table(tx2, "two")

        let assert Ok(_) = pgl.sql("SELECT 2") |> pgl.query(tx2)

        let assert Error(_) = {
          use <- internal.with_rescue()

          pgl.savepoint(tx2, fn(tx3) {
            let id3 = insert_into_users_table(tx3, "three")

            let assert order.Gt = int.compare(id3, id2)

            panic as "savepoint failure!"
          })
        }

        Ok(id2)
      })
      |> result.map(fn(id2) { #(id1, id2) })
    })

  let assert Ok(queried) =
    pgl.sql("SELECT * FROM users WHERE name IN ('one', 'two', 'three')")
    |> pgl.query(conn)

  assert 2 == queried.count
}

// Transaction helper
fn insert_into_users_table(conn: pgl.Connection, name: String) {
  let assert Ok(returned) =
    sql.insert_into_users([
      "DEFAULT, '" <> name <> "', true, ARRAY[''], '2025-03-04', now()",
    ])
    |> sql.returning(["id"])
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) =
    returned.rows
    |> list.first

  let assert Ok(ids) = decode.run(row, decode.list(of: decode.int))
  let assert Ok(id) = list.first(ids)
  id
}

// ---------- Error String Tests ---------- //

pub fn error_to_string_query_error_test() {
  let err = pgl.QueryError("Failed to process queried rows")
  let result = pgl.error_to_string(err)

  assert "(QueryError) Failed to process queried rows" == result
}

pub fn error_to_string_connection_error_test() {
  let err = pgl.ConnectionError("unable to connect to database")
  let result = pgl.error_to_string(err)

  assert "(ConnectionError) unable to connect to database" == result
}

pub fn error_to_string_connection_timeout_test() {
  let err = pgl.ConnectionTimeout
  let result = pgl.error_to_string(err)

  assert "(ConnectionTimeout)" == result
}

pub fn error_to_string_authentication_error_test() {
  let err = pgl.AuthenticationError("invalid password")
  let result = pgl.error_to_string(err)

  assert "(AuthenticationError) invalid password" == result
}

pub fn error_to_string_protocol_error_test() {
  let err = pgl.ProtocolError("unexpected message received")
  let result = pgl.error_to_string(err)

  assert "(ProtocolError) unexpected message received" == result
}

pub fn error_to_string_socket_error_test() {
  let err = pgl.SocketError("connection reset by peer")
  let result = pgl.error_to_string(err)

  assert "(SocketError) connection reset by peer" == result
}

pub fn error_to_string_postgres_error_test() {
  let err =
    pgl.PostgresError(
      code: "42P01",
      name: "undefined_table",
      message: "relation \"foo\" does not exist",
      fields: dict.new(),
    )
  let result = pgl.error_to_string(err)

  assert "(PostgresError), code: 42P01, name: undefined_table, message: relation \"foo\" does not exist"
    == result
}

pub fn error_to_string_empty_message_test() {
  let err = pgl.QueryError("")
  let result = pgl.error_to_string(err)

  assert "(QueryError)" == result
}

// ---------- Enum Tests ---------- //

pub fn enum_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(queried) =
    "INSERT INTO enum_test (current_mood) VALUES ($1) RETURNING id"
    |> pgl.sql
    |> pgl.values([pg_value.enum("happy")])
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM enum_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use mood <- decode.field(1, decode.string)
      decode.success(mood)
    })

  assert "happy" == decoded
}

pub fn enum_string_constant_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let val = pg_value.to_string(pg_value.enum("sad"))

  assert "'sad'" == val

  let assert Ok(queried) =
    {
      "INSERT INTO enum_test (current_mood) VALUES (" <> val <> ") RETURNING id"
    }
    |> pgl.sql
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM enum_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use mood <- decode.field(1, decode.string)
      decode.success(mood)
    })

  assert "sad" == decoded
}

pub fn enum_array_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let assert Ok(queried) =
    "INSERT INTO enum_array_test (colors) VALUES ($1) RETURNING id"
    |> pgl.sql
    |> pgl.values([pg_value.array(["red", "blue", "green"], of: pg_value.enum)])
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM enum_array_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use colors <- decode.field(1, decode.list(of: decode.string))
      decode.success(colors)
    })

  assert ["red", "blue", "green"] == decoded
}

// ---------- Json Tests ---------- //

pub fn json_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let data =
    json.object([
      #("name", json.string("Alice")),
      #("age", json.int(30)),
      #("active", json.bool(True)),
    ])

  let assert Ok(queried) =
    "INSERT INTO json_test (data) VALUES ($1) RETURNING id"
    |> pgl.sql
    |> pgl.values([pg_value.json(data)])
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM json_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use data <- decode.field(1, decode.string)
      decode.success(data)
    })

  let assert Ok(parsed) =
    json.parse(decoded, {
      use name <- decode.field("name", decode.string)
      use age <- decode.field("age", decode.int)
      use active <- decode.field("active", decode.bool)
      decode.success(#(name, age, active))
    })

  assert #("Alice", 30, True) == parsed
}

pub fn jsonb_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let data =
    json.object([
      #("name", json.string("Bob")),
      #("score", json.float(99.5)),
      #("tags", json.array(["gleam", "erlang"], of: json.string)),
    ])

  let assert Ok(queried) =
    "INSERT INTO jsonb_test (data) VALUES ($1) RETURNING id"
    |> pgl.sql
    |> pgl.values([pg_value.json(data)])
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM jsonb_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use data <- decode.field(1, decode.string)
      decode.success(data)
    })

  let assert Ok(parsed) =
    json.parse(decoded, {
      use name <- decode.field("name", decode.string)
      use score <- decode.field("score", decode.float)
      use tags <- decode.field("tags", decode.list(of: decode.string))
      decode.success(#(name, score, tags))
    })

  assert #("Bob", 99.5, ["gleam", "erlang"]) == parsed
}

pub fn json_array_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let items = [
    json.object([#("id", json.int(1)), #("name", json.string("first"))]),
    json.object([#("id", json.int(2)), #("name", json.string("second"))]),
  ]

  let assert Ok(queried) =
    "INSERT INTO json_array_test (items) VALUES ($1) RETURNING id"
    |> pgl.sql
    |> pgl.values([pg_value.array(items, of: pg_value.json)])
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM json_array_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use items <- decode.field(1, decode.list(of: decode.string))
      decode.success(items)
    })

  let item_decoder = {
    use id <- decode.field("id", decode.int)
    use name <- decode.field("name", decode.string)
    decode.success(#(id, name))
  }

  let assert Ok(parsed) = list.try_map(decoded, json.parse(_, item_decoder))

  assert [#(1, "first"), #(2, "second")] == parsed
}

pub fn json_nested_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let data =
    json.object([
      #(
        "user",
        json.object([
          #("name", json.string("Charlie")),
          #(
            "addresses",
            json.preprocessed_array([
              json.object([
                #("city", json.string("New York")),
                #("zip", json.string("10001")),
              ]),
              json.object([
                #("city", json.string("Boston")),
                #("zip", json.string("02101")),
              ]),
            ]),
          ),
        ]),
      ),
      #(
        "metadata",
        json.object([
          #("version", json.int(2)),
          #("nullable_field", json.null()),
        ]),
      ),
    ])

  let assert Ok(queried) =
    "INSERT INTO json_nested_test (data) VALUES ($1) RETURNING id"
    |> pgl.sql
    |> pgl.values([pg_value.json(data)])
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM json_nested_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use data <- decode.field(1, decode.string)
      decode.success(data)
    })

  let address_decoder = {
    use city <- decode.field("city", decode.string)
    use zip <- decode.field("zip", decode.string)
    decode.success(#(city, zip))
  }

  let user_decoder = {
    use name <- decode.field("name", decode.string)
    use addresses <- decode.field("addresses", decode.list(of: address_decoder))
    decode.success(#(name, addresses))
  }

  let metadata_decoder = {
    use version <- decode.field("version", decode.int)
    use nullable_field <- decode.field(
      "nullable_field",
      decode.optional(decode.string),
    )
    decode.success(#(version, nullable_field))
  }

  let decoder = {
    use user <- decode.field("user", user_decoder)
    use metadata <- decode.field("metadata", metadata_decoder)
    decode.success(#(user, metadata))
  }

  let assert Ok(parsed) = json.parse(decoded, decoder)

  let #(#(name, addresses), #(version, nullable_field)) = parsed

  assert "Charlie" == name
  assert [#("New York", "10001"), #("Boston", "02101")] == addresses
  assert 2 == version
  assert None == nullable_field
}

pub fn json_string_constant_test() {
  use db <- db.with_db()
  use conn <- db.with_conn(db)

  let data =
    json.object([
      #("key", json.string("value")),
      #("num", json.int(42)),
    ])

  let val = pg_value.to_string(pg_value.json(data))

  assert "'{\"key\":\"value\",\"num\":42}'" == val

  let assert Ok(queried) =
    { "INSERT INTO json_const_test (data) VALUES (" <> val <> ") RETURNING id" }
    |> pgl.sql
    |> pgl.query(conn)

  assert 1 == queried.count

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(id) = decode.run(row, db.id_decoder())

  let assert Ok(queried) =
    { "SELECT * FROM json_const_test WHERE id=" <> int.to_string(id) }
    |> pgl.sql
    |> pgl.query(conn)

  let assert Ok(row) = list.first(queried.rows)

  let assert Ok(decoded) =
    decode.run(row, {
      use _id <- decode.field(0, decode.int)
      use data <- decode.field(1, decode.string)
      decode.success(data)
    })

  let assert Ok(parsed) =
    json.parse(decoded, {
      use key <- decode.field("key", decode.string)
      use num <- decode.field("num", decode.int)
      decode.success(#(key, num))
    })

  assert #("value", 42) == parsed
}

// ---------- Auth Tests ---------- //

pub fn md5_auth_query_test() {
  let db =
    pgl.config
    |> pgl.host("127.0.0.1")
    |> pgl.port(5432)
    |> pgl.database("pgl_test")
    |> pgl.username("md5_user")
    |> pgl.password("md5_pass")
    |> pgl.ssl(pgl.SslUnverified)
    |> pgl.new

  let assert Ok(_) = pgl.start(db)

  use conn <- db.with_conn_raw(db)

  let assert Ok(result) =
    pgl.sql("SELECT 1 AS val, 'md5_auth' AS auth_type")
    |> pgl.query(conn)

  assert 1 == result.count

  let row_decoder = {
    use val <- decode.field(0, decode.int)
    use auth_type <- decode.field(1, decode.string)
    decode.success(#(val, auth_type))
  }

  let assert Ok([#(1, "md5_auth")]) =
    result.rows
    |> list.try_map(decode.run(_, row_decoder))
}

pub fn cleartext_auth_query_test() {
  let db =
    pgl.config
    |> pgl.host("127.0.0.1")
    |> pgl.port(5432)
    |> pgl.database("pgl_test")
    |> pgl.username("cleartext_user")
    |> pgl.password("cleartext_pass")
    |> pgl.ssl(pgl.SslUnverified)
    |> pgl.new

  let assert Ok(_) = pgl.start(db)

  use conn <- db.with_conn_raw(db)

  let assert Ok(result) =
    pgl.sql("SELECT 1 AS val, 'cleartext_auth' AS auth_type")
    |> pgl.query(conn)

  assert 1 == result.count

  let row_decoder = {
    use val <- decode.field(0, decode.int)
    use auth_type <- decode.field(1, decode.string)
    decode.success(#(val, auth_type))
  }

  let assert Ok([#(1, "cleartext_auth")]) =
    result.rows
    |> list.try_map(decode.run(_, row_decoder))
}
