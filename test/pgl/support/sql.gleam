import gleam/string

/// Build an INSERT INTO users VALUES (...) statement.
pub fn insert_into_users(values: List(String)) -> String {
  let values_str = string.join(values, "), (")
  "INSERT INTO users VALUES (" <> values_str <> ")"
}

/// Append a RETURNING clause to a SQL statement.
pub fn returning(sql: String, columns: List(String)) -> String {
  sql <> " RETURNING " <> string.join(columns, ", ")
}
