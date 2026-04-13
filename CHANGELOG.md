# Changelog

## v2.0.1

### Changed

- Default SSL mode is now `SslVerified` (was `SslDisabled`)
- `from_url` now accepts `prefer` and `allow` as `sslmode` values (mapped to `SslUnverified`)
- `from_url` without an `sslmode` query parameter now preserves the default SSL setting

### Fixed

- Fixed SSL configuration being silently ignored — the `ssl` setting was never passed to the connection protocol
- Fixed `from_url` failing when query parameters are present but `sslmode` is not specified
- Fixed SCRAM nonce generation using only 16 bits of randomness instead of 128 bits
- Fixed SCRAM client nonce prefix not compared against server nonce
- Fixed `parse_server_final` crashing with a panic on unexpected SASL payloads instead of returning an error
- Fixed SASLprep private use character range (`0xF000` corrected to `0xF0000`)
- Fixed typo in internal SASLprep function name (`plan_text` → `plain_text`)

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
