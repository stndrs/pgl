# Changelog

## Unreleased

### Changed

- Default SSL mode changed from `SslDisabled` to `SslVerified`
- Replaced internal `store` module with `rasa`
- Refactored internal `socket` module to use `neon` for managing tcp and ssl sockets
- Idle connection pings now run inside the Socket actor, eliminating a race condition where pings could interleave with query I/O on the same connection
- `from_url` now accepts `sslmode=prefer` and `sslmode=allow` (mapped to `SslUnverified`), and no longer fails when query parameters are present without an `sslmode` key

### Added

- `ConnectionUnavailable` error variant in `PglError`
- Socket sends PostgreSQL `Terminate` message before closing connections
- Added `Enum` and `Json` type tests

### Fixed

- Fixed SCRAM nonce generation using only 16 bits of randomness instead of 128 bits
- Fixed SCRAM client nonce prefix not compared against server nonce
- Fixed SASLprep private use character range (`0xF000` corrected to `0xF0000`)
- Fixed type cache not notifying caller on bootstrap SQL failure, causing a timeout instead of a clean error
- Fixed `parse_server_final` crashing with a panic on unexpected SASL payloads instead of returning an error
- Fixed `data_row_values` returning unreversed partial row on truncated data
- Fixed `transaction_rollback_test` silently passing if `begin` fails
- Fixed `format_error_with_values` test not running due to missing `_test` suffix
- Fixed typo in internal SASLprep function name (`plan_text` → `plain_text`)

### Removed

- Removed dead `PosixError` type and `posix_error_to_string` function

## v2.0.0

- Update [`pg_value`](https://hexdocs.pm/pg_value/pg_value.html) to v2.0.0
- Removed deprecated function `release_savepoint`
- Removed deprecated function `with_connection`

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
- Deprecated `with_connection`
- Added `pgl.error_to_string` function
