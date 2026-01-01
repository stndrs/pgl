# PGL

`pgl` is a PostgreSQL client written in Gleam.

## Features

- Implementation of PostgreSQL wire protocol
- SSL support
- [`SCRAM-SHA-256` Authentication](https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256)
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

  let assert Ok(queried) =
    {
      use conn <- pgl.with_connection(db)

      "SELECT * FROM users WHERE id=$1"
      |> pgl.query([value.int(1000)], conn)
    }

  pgl.shutdown(db)
}
```

## Installation

```
gleam add pgl
```

### Acknowledgements

Early iterations of this package were based on [pgo](https://github.com/erleans/pgo) and its influence remains. [pog](https://github.com/lpil/pog) and [postgrex](https://github.com/elixir-ecto/postgrex) were also helpful in writing this package.
