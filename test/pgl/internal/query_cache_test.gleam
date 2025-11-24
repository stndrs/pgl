import exception
import pgl/internal
import pgl/internal/query_cache

pub fn lookup_empty_test() {
  let qc = query_cache.new()

  let assert Ok(_) = query_cache.start(qc)

  let assert Error(internal.QueryCacheError(
    kind: internal.NotFoundError,
    message: "SQL query not found in cache",
  )) = query_cache.lookup(qc, "SELECT * FROM users")

  query_cache.shutdown(qc)
}

pub fn insert_lookup_test() {
  let qc = query_cache.new()

  let assert Ok(_) = query_cache.start(qc)

  query_cache.insert(qc, "SELECT id FROM users", [23])

  let assert Ok([23]) = query_cache.lookup(qc, "SELECT id FROM users")

  query_cache.shutdown(qc)
}

pub fn insert_error_test() {
  let qc = query_cache.new()

  let assert Ok(_) = query_cache.start(qc)

  query_cache.shutdown(qc)

  // Exception raised because the process is DOWN
  let assert Error(_) =
    exception.rescue(fn() {
      query_cache.insert(qc, "SELECT id FROM users", [23])
    })
}

pub fn reset_test() {
  let qc = query_cache.new()

  let assert Ok(_) = query_cache.start(qc)

  query_cache.insert(qc, "SELECT id FROM users", [23])

  let assert Ok([23]) = query_cache.lookup(qc, "SELECT id FROM users")

  query_cache.reset(qc)

  let assert Error(_) = query_cache.lookup(qc, "SELECT id FROM users")

  query_cache.shutdown(qc)
}
