# Changelog

## v1.1.0

- Added internal `conn` module for handling single connections
- `Connection` type can now be a `Pool` or `Connection` that `conn.Conn`.
- `db_pool` now manages `Socket`s rather than `Connection`s.
- `query`, `execute`, `batch`, and `transaction` automatically check out single connections if given a `Pool`.
- `transaction` checks in connections on success or failure.
- `begin` checks out a single connection.
- `commit` and `rollback` check in single connections.
- Added `connection` function to create a `Connection` from the provided `Db`.
- Deprecated `release_savepoint`
- Added `pgl.error_to_string` function
