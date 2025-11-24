import gleam/list
import pgl/config
import pgl/internal/type_cache.{type TypeCache}

fn conf() {
  config.default
  |> config.set_database("gleam_pgl_test")
  |> config.set_username("postgres")
  |> config.set_password("postgres")
}

pub fn load_test() {
  let conf = conf()

  use tc <- with_type_cache()

  let assert Ok(_) = type_cache.load(tc, conf)
}

pub fn lookup_test() {
  let conf = conf()

  use tc <- with_type_cache()

  let assert Ok(_) = type_cache.load(tc, conf)

  let assert Ok(result) = type_cache.lookup(tc, [23], conf)
  let assert Ok(ti) = list.first(result)

  let assert 23 = ti.oid
  let assert "int4" = ti.name
}

pub fn lookup_many_test() {
  let conf = conf()

  use tc <- with_type_cache()

  let assert Ok(_) = type_cache.load(tc, conf)

  let oids = [
    23,
    25,
    1043,
    16,
  ]

  let assert Ok(result) = type_cache.lookup(tc, oids, conf)

  let assert 4 = result |> list.length
}

fn with_type_cache(next: fn(TypeCache) -> t) -> t {
  let tc = type_cache.new()

  let assert Ok(_) = type_cache.start(tc)

  let res = next(tc)

  type_cache.shutdown(tc)

  res
}
