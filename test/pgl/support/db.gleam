import gleam/dynamic/decode.{type Decoder}
import global_value
import pgl
import pgl/support/schema

/// Common single-int-column decoder.
pub fn id_decoder() -> Decoder(Int) {
  use id <- decode.field(0, decode.int)
  decode.success(id)
}

/// Get or create a shared database pool. Schema is set up once on first call.
pub fn with_db(next: fn(pgl.Db) -> t) -> t {
  let db = {
    use <- global_value.create_with_unique_name("pgl_pools")

    let db =
      pgl.config
      |> pgl.username("postgres")
      |> pgl.password("postgres")
      |> pgl.ssl(pgl.SslUnverified)
      |> pgl.new

    let assert Ok(_) = pgl.start(db)

    // Setup schema once when pool is first created
    let conn = pgl.connection(db)
    schema.setup_all(conn)

    db
  }

  next(db)
}

/// Get a connection and truncate all tables for test isolation.
pub fn with_conn(db: pgl.Db, next: fn(pgl.Connection) -> t) -> t {
  let conn = pgl.connection(db)
  schema.truncate_all(conn)
  next(conn)
}

/// Get a connection without truncating (for tests that manage their own state).
pub fn with_conn_raw(db: pgl.Db, next: fn(pgl.Connection) -> t) -> t {
  db
  |> pgl.connection
  |> next
}

/// Pool configured with rows_as_dict mode.
/// Note: connects to 'postgres' database, schema setup done on first use.
pub fn with_db_rows_as_maps(next: fn(pgl.Db) -> t) -> t {
  let db = {
    use <- global_value.create_with_unique_name("pgl_pool_rows_as_maps_test")

    let assert Ok(conf) =
      "postgres://postgres:postgres@127.0.0.1/postgres"
      |> pgl.from_url

    let conf =
      conf
      |> pgl.ssl(pgl.SslUnverified)
      |> pgl.rows_as_dict(True)

    let db = pgl.new(conf)

    let assert Ok(_) = pgl.start(db)

    // Setup schema for this pool too (different database)
    let conn = pgl.connection(db)
    schema.setup_all(conn)

    db
  }

  next(db)
}
