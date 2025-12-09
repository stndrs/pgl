# PGL

`pgl` is a PostgreSQL client written in Gleam.

**Note:** This package is still in early development and has limitations and rough edges.

## Features

- Implementation of PostgreSQL wire protocol
- SSL support
- [`SCRAM-SHA-256` Authentication](https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256)
- Connection pooling provided by [`db_pool`](https://github.com/stndrs/db_pool)
- PostgreSQL types and parameter values provided by [`pgl_types`](https://github.com/stndrs/pgl_types)
- Transaction support
- Savepoint support
- [Pipelining](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING)

## Limitations

- [`db_pool`](https://github.com/stndrs/db_pool)'s connection pool algorithm has room for improvement. It works but is fairly simple at the moment.
- [`pgl_types`](https://github.com/stndrs/pgl_types) supports commonly used types but does not yet support as many types as other PostgreSQL libraries.
  - The `array` type also does not yet support nested arrays.

## TODO
- [ ] Instrumentation support
- [ ] Improve error handling
- [ ] Documentation
- [ ] Benchmarks
- [ ] `LISTEN/NOTIFY`

## Example

```gleam
import pgl
import pgl/value

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

TODO

### Acknowledgements

Early iterations of this package were based heavily on [pgo](https://github.com/erleans/pgo), and its influence is still very present. [pog](https://github.com/lpil/pog) and [postgrex](https://github.com/elixir-ecto/postgrex) were also very helpful in writing this package.
