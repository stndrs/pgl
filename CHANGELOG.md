# Changelog

## v3.0.1

- Fixed calls to `pgl.batch` timing out when one of the queries causes the server to issue an `ErrorResponse`.

## v3.0.0

### Changed

- Renamed `pgl.params` to `pgl.values`
- `Config` is now opaque
- Password is now optional
- Renamed `pgl.default` to `pgl.config`
- Default SSL mode changed from `SslDisabled` to `SslVerified`
- Replaced internal `store` module with `rasa`
- Refactored internal `socket` module to use `neon` for managing tcp and ssl sockets
- Idle connection pings now run inside the Socket actor, eliminating a race condition where pings could interleave with query I/O on the same connection
- SCRAM nonce generation simplified to use only `crypto.strong_random_bytes`
- SCRAM `client_first` now returns `Result` to handle username escaping errors

### Added

- `ConnectionUnavailable` error variant in `PglError`
- Socket sends PostgreSQL `Terminate` message before closing connections
- Added `Enum` and `Json` type tests
- Added cleartext and MD5 password authentication support
- SCRAM username escaping per RFC 5802 (`=` to `=3D`, `,` to `=2C`)
- SCRAM server iteration count bounds (4096 to 100,000)
- Constant-time server signature comparison using `crypto.secure_compare`

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
- Fixed `begin` leaking pool connection when `BEGIN` query fails
- Fixed `setup` discarding server parameter status updates received during authentication
- Removed dead code path in `row_description_fields` that could discard fields if payload bytes coincidentally matched a literal string
- Fixed `data_row_values` silently accepting truncated rows when payload runs out before all declared columns are decoded

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
