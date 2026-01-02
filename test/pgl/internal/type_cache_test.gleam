import gleam/list
import gleam/result
import pgl/internal
import pgl/internal/protocol
import pgl/internal/socket
import pgl/internal/socket_test
import pgl/internal/type_cache.{type TypeCache}

fn conf() {
  protocol.config
  |> protocol.database("gleam_pgl_test")
  |> protocol.username("postgres")
  |> protocol.password("postgres")
}

pub fn load_test() {
  use tc <- with_type_cache()

  let assert Ok(_) = type_cache.load(tc)
}

pub fn lookup_test() {
  use tc <- with_type_cache()

  let assert Ok(_) = type_cache.load(tc)

  let assert Ok(result) = type_cache.lookup(tc, [23])
  let assert Ok(ti) = list.first(result)

  let assert 23 = ti.oid
  let assert "int4" = ti.name
}

pub fn lookup_many_test() {
  use tc <- with_type_cache()

  let assert Ok(_) = type_cache.load(tc)

  let oids = [
    23,
    25,
    1043,
    16,
  ]

  let assert Ok(result) = type_cache.lookup(tc, oids)

  let assert 4 = result |> list.length
}

fn with_type_cache(next: fn(TypeCache) -> t) {
  let sockets =
    socket.new()
    |> socket.host(internal.default_host)
    |> socket.port(internal.default_port)
    |> socket.factory

  let assert Ok(_) = socket_test.supervise(sockets)

  let tc =
    type_cache.new()
    |> type_cache.on_connect(fn() {
      socket.connect(sockets)
      |> result.map_error(fn(_) { Nil })
      |> result.try(fn(sock) {
        protocol.auth(sock, conf())
        |> result.map_error(fn(_) { Nil })
      })
    })

  let assert Ok(_) = type_cache.start(tc)

  let res = next(tc)

  type_cache.shutdown(tc)

  res
}
