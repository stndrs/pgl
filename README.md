# PGL

[![test](https://github.com/stndrs/pgl/actions/workflows/test.yml/badge.svg)](https://github.com/stndrs/pgl/actions/workflows/test.yml)
[![Package Version](https://img.shields.io/hexpm/v/pgl)](https://hex.pm/packages/pgl)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/pgl/)

`pgl` is a PostgreSQL client written in Gleam.

## Features

- Implementation of PostgreSQL wire protocol
- SSL support
- [`SCRAM-SHA-256`](https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256), [Cleartext](https://www.postgresql.org/docs/current/auth-password.html), and [MD5](https://www.postgresql.org/docs/current/auth-password.html) password authentication
- Connection pooling provided by [`db_pool`](https://github.com/stndrs/db_pool)
- PostgreSQL data types provided by [`pg_value`](https://github.com/stndrs/pg_value)
- Transaction support
- Savepoint support
- [Pipelining](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING)

## Example

```gleam
import gleam/dynamic/decode
import gleam/list
import pgl
import pg_value

pub fn main() {
  let conf =
    pgl.config
    |> pgl.host("localhost")
    |> pgl.port(5432)
    |> pgl.database("my_db")
    |> pgl.username("postgres")
    |> pgl.password("postgres")

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  let conn = pgl.connection(db)

  let assert Ok(queried) =
    "SELECT id, name FROM users WHERE id = $1"
    |> pgl.sql
    |> pgl.values([pg_value.int(1)])
    |> pgl.query(conn)

  let assert Ok(users) =
    queried.rows
    |> list.try_map(decode.run(_, {
      use id <- decode.field(0, decode.int)
      use name <- decode.field(1, decode.string)
      decode.success(#(id, name))
    }))

  let assert Ok(_) = pgl.shutdown(db)
}
```

## URL Configuration

You can also configure a connection from a URL:

```gleam
let assert Ok(conf) =
  "postgres://user:pass@localhost:5432/my_db?sslmode=verify-ca"
  |> pgl.from_url
```

Supported `sslmode` values: `disable`, `require`, `verify-ca`, `verify-full`.

## Rows as Dicts

By default, rows are returned as tuples. To return rows as `Dict`s where columns are keyed by name:

```gleam
let conf =
  pgl.config
  |> pgl.rows_as_dict(True)
```

## Pipelining

Use `pgl.batch` to send multiple queries without waiting for each to complete, reducing network round trips:

```gleam
let queries = [
  pgl.Query("INSERT INTO users (name) VALUES ($1)", [pg_value.text("Alice")]),
  pgl.Query("INSERT INTO users (name) VALUES ($1)", [pg_value.text("Bob")]),
]

let assert Ok(results) = pgl.batch(queries, conn)
```

## Transactions and Savepoints

Use `pgl.transaction` for automatic commit/rollback:

```gleam
let assert Ok(result) =
  pgl.transaction(conn, fn(tx) {
    let assert Ok(_) =
      "INSERT INTO users (name) VALUES ($1)"
      |> pgl.sql
      |> pgl.values([pg_value.text("Alice")])
      |> pgl.query(tx)

    Ok("done")
  })
```

If the callback returns `Error` or raises an exception, the transaction is rolled back. Nested savepoints are also supported:

```gleam
pgl.transaction(conn, fn(tx) {
  pgl.savepoint(tx, fn(sp) {
    // If this fails, only the savepoint is rolled back
    Ok(Nil)
  })
})
```

## Supported Versions

- PostgreSQL 15, 16, 17, 18
- Erlang/OTP 28+

## Installation

```
gleam add pgl
```

Further documentation can be found at <https://hexdocs.pm/pgl>.

## Development

Tests require a running PostgreSQL instance:

```sh
docker compose up      # Start PostgreSQL
gleam test             # Run the tests
```

### Acknowledgements

Early iterations of this package were based on [pgo](https://github.com/erleans/pgo) and its influence remains. [pog](https://github.com/lpil/pog) and [postgrex](https://github.com/elixir-ecto/postgrex) were also helpful in writing this package.
