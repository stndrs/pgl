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
import pgl
import pg_value as value

pub fn main() {
  let assert Ok(conf) =
    "postgres://user:pass@db:5432/pgl_db"
    |> pgl.from_url

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  let conn = pgl.connection(db)

  let assert Ok(queried) =
    {

      "SELECT * FROM users WHERE id=$1"
      |> pgl.sql
      |> pgl.params([value.int(1000)])
      |> pgl.query(conn)
    }

  pgl.shutdown(db)
}
```

## Installation

```
gleam add pgl
```

Further documentation can be found at <https://hexdocs.pm/valet>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
```

### Acknowledgements

Early iterations of this package were based on [pgo](https://github.com/erleans/pgo) and its influence remains. [pog](https://github.com/lpil/pog) and [postgrex](https://github.com/elixir-ecto/postgrex) were also helpful in writing this package.
