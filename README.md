# PGL

## [WIP] A PostgreSQL database driver for Gleam

This package implements the PostgreSQL wire protocol.

### Done

- [x] Common PG type encoding/decoding
- [x] Binary protocol
- [x] Extended query flow
- [x] Type cache
- [x] Query cache

### In progress

- [ ] Pipelining

### Todo

- [ ] More PG types
- [ ] Logging

### Inspired by

[pgo]: https://github.com/erleans/pgo
[pog]: https://github.com/lpil/pog
[postgrex]: https://github.com/elixir-ecto/postgrex

```gleam
import pgl
import pgl/config

pub fn main() {
  let assert Ok(conf) =
    "postgres://user:pass@db:5432/pgl_db"
    |> config.from_url

  let db = pgl.new(conf)

  let assert Ok(_) = pgl.start(db)

  use conn <- pgl.with_connection(db)

  let assert Ok(queried) =
    "SELECT * FROM users WHERE id=$1"
    |> pgl.query([pgl.int(1000)], conn)

  pgl.shutdown(db)
}
```

## Installation

```sh
gleam add pgl
```
