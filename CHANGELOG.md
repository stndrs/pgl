# Changelog

## Unreleased

- `Connection` type can now be a `Pool` or `Connection` that holds a `SingleConnection`.
- `db_pool` now manages `Socket`s rather than `Connection`s.
- `query`, `execute`, `batch`, and `transaction` automatically check out single connections if given a `Pool`.
- `transaction` checks in connections on success or failure.
- `begin` checks out a single connection.
- `commit` and `rollback` check in single connections.
- Added `connection` function to create a `Connection` from the provided `Db`.
