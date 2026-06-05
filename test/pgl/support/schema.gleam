import gleam/string
import pgl

// ---------- Extensions ---------- //

const extensions_sql = "CREATE EXTENSION IF NOT EXISTS hstore;"

// ---------- Types ---------- //

const mood_type_sql = "
DO $$ BEGIN
  CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
"

const color_type_sql = "
DO $$ BEGIN
  CREATE TYPE color AS ENUM ('red', 'green', 'blue');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
"

// ---------- Tables ---------- //

const users_sql = "
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  active boolean NOT NULL DEFAULT true,
  nicknames VARCHAR(50)[] NOT NULL,
  birthday DATE NOT NULL,
  created_at TIMESTAMP NOT NULL
);
"

const hstore_test_sql = "
CREATE TABLE IF NOT EXISTS hstore_test (
  id SERIAL PRIMARY KEY,
  data hstore
);
"

const enum_test_sql = "
CREATE TABLE IF NOT EXISTS enum_test (
  id SERIAL PRIMARY KEY,
  current_mood mood
);
"

const enum_array_test_sql = "
CREATE TABLE IF NOT EXISTS enum_array_test (
  id SERIAL PRIMARY KEY,
  colors color[]
);
"

const uuid_test_sql = "
CREATE TABLE IF NOT EXISTS uuid_test (
  id SERIAL PRIMARY KEY,
  identifier UUID
);
"

const json_test_sql = "
CREATE TABLE IF NOT EXISTS json_test (
  id SERIAL PRIMARY KEY,
  data json
);
"

const jsonb_test_sql = "
CREATE TABLE IF NOT EXISTS jsonb_test (
  id SERIAL PRIMARY KEY,
  data jsonb
);
"

const json_array_test_sql = "
CREATE TABLE IF NOT EXISTS json_array_test (
  id SERIAL PRIMARY KEY,
  items jsonb[]
);
"

const json_nested_test_sql = "
CREATE TABLE IF NOT EXISTS json_nested_test (
  id SERIAL PRIMARY KEY,
  data jsonb
);
"

const json_const_test_sql = "
CREATE TABLE IF NOT EXISTS json_const_test (
  id SERIAL PRIMARY KEY,
  data jsonb
);
"

const new_users_sql = "
CREATE TABLE IF NOT EXISTS new_users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50) NOT NULL
);
"

const posts_sql = "
CREATE TABLE IF NOT EXISTS posts (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL
);
"

const comments_sql = "
CREATE TABLE IF NOT EXISTS comments (
  id SERIAL PRIMARY KEY,
  post_id INTEGER NOT NULL,
  content TEXT NOT NULL
);
"

const tags_sql = "
CREATE TABLE IF NOT EXISTS tags (
  id SERIAL PRIMARY KEY,
  post_id INTEGER NOT NULL,
  name VARCHAR(20) NOT NULL
);
"

const tx_test_sql = "
CREATE TABLE IF NOT EXISTS tx_test (
  id INTEGER PRIMARY KEY,
  name TEXT
);
"

/// All table names for truncation
const all_tables = [
  "users", "hstore_test", "enum_test", "enum_array_test", "uuid_test",
  "json_test", "jsonb_test", "json_array_test", "json_nested_test",
  "json_const_test", "new_users", "posts", "comments", "tags", "tx_test",
]

/// Create all extensions, types, and tables. Idempotent.
/// Call once at test suite start.
pub fn setup_all(conn: pgl.Connection) -> Nil {
  // Extensions first
  let assert Ok(_) = pgl.execute(extensions_sql, conn)

  // Types second
  let assert Ok(_) = pgl.execute(mood_type_sql, conn)
  let assert Ok(_) = pgl.execute(color_type_sql, conn)

  // Tables last (some depend on types/extensions above)
  let assert Ok(_) = pgl.execute(users_sql, conn)
  let assert Ok(_) = pgl.execute(hstore_test_sql, conn)
  let assert Ok(_) = pgl.execute(enum_test_sql, conn)
  let assert Ok(_) = pgl.execute(enum_array_test_sql, conn)
  let assert Ok(_) = pgl.execute(uuid_test_sql, conn)
  let assert Ok(_) = pgl.execute(json_test_sql, conn)
  let assert Ok(_) = pgl.execute(jsonb_test_sql, conn)
  let assert Ok(_) = pgl.execute(json_array_test_sql, conn)
  let assert Ok(_) = pgl.execute(json_nested_test_sql, conn)
  let assert Ok(_) = pgl.execute(json_const_test_sql, conn)
  let assert Ok(_) = pgl.execute(new_users_sql, conn)
  let assert Ok(_) = pgl.execute(posts_sql, conn)
  let assert Ok(_) = pgl.execute(comments_sql, conn)
  let assert Ok(_) = pgl.execute(tags_sql, conn)
  let assert Ok(_) = pgl.execute(tx_test_sql, conn)

  Nil
}

/// Truncate specific tables. Use for per-test isolation.
pub fn truncate(conn: pgl.Connection, tables: List(String)) -> Nil {
  let sql =
    "TRUNCATE " <> string.join(tables, ", ") <> " RESTART IDENTITY CASCADE"
  let assert Ok(_) = pgl.execute(sql, conn)
  Nil
}

/// Truncate all test tables.
pub fn truncate_all(conn: pgl.Connection) -> Nil {
  truncate(conn, all_tables)
}
