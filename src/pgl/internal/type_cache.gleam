import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import pgl/internal
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/socket.{type Socket}
import pgl/internal/store.{type Store}
import pgl/types

pub opaque type TypeCache {
  TypeCache(np: process.Name(Message), host: String, port: Int)
}

pub opaque type Message {
  Load(client: process.Subject(Result(Nil, internal.PglError)), sock: Socket)
  Lookup(
    client: process.Subject(Result(List(types.TypeInfo), internal.PglError)),
    oids: List(Int),
  )
  Shutdown
}

const name = "pgl_type_cache"

pub fn new() -> TypeCache {
  let np = process.new_name(name)

  TypeCache(np:, host: internal.default_host, port: internal.default_port)
}

pub fn host(tc: TypeCache, host: String) -> TypeCache {
  TypeCache(..tc, host:)
}

pub fn port(tc: TypeCache, port: Int) -> TypeCache {
  TypeCache(..tc, port:)
}

const table_name = "pgl_type_cache_table"

pub fn start(tc: TypeCache) -> actor.StartResult(Nil) {
  actor.new_with_initialiser(1000, fn(subj) {
    let selector = process.new_selector() |> process.select(subj)

    store.new(table_name)
    |> actor.initialised
    |> actor.selecting(selector)
    |> Ok
  })
  |> actor.named(tc.np)
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn supervised(tc: TypeCache) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start(tc) })
  |> supervision.timeout(1000)
  |> supervision.restart(supervision.Transient)
}

pub fn load(
  tc: TypeCache,
  conf: protocol.Config,
) -> Result(Nil, internal.PglError) {
  use sock <- result.try(socket.connect(tc.host, tc.port))
  use sock <- result.try(protocol.auth(sock, conf))

  let res = process.named_subject(tc.np) |> actor.call(1000, Load(_, sock))

  let _ = socket.shutdown(sock)

  res
}

pub fn lookup(
  tc: TypeCache,
  oids: List(Int),
  config: protocol.Config,
) -> Result(List(types.TypeInfo), internal.PglError) {
  use <- result.lazy_or(do_lookup(tc, oids))
  use _ <- result.try(load(tc, config))

  do_lookup(tc, oids)
}

fn do_lookup(
  tc: TypeCache,
  oids: List(Int),
) -> Result(List(types.TypeInfo), internal.PglError) {
  process.named_subject(tc.np) |> actor.call(1000, Lookup(_, oids))
}

pub fn shutdown(tc: TypeCache) -> Nil {
  process.named_subject(tc.np) |> actor.send(Shutdown)
}

fn handle_message(
  store: Store(Int, types.TypeInfo),
  message: Message,
) -> actor.Next(Store(Int, types.TypeInfo), a) {
  case message {
    Load(client, sock) -> handle_load(store, sock, client)
    Lookup(client, oids) -> handle_lookup(store, oids, client)
    Shutdown -> actor.stop()
  }
}

fn handle_load(
  store: Store(Int, types.TypeInfo),
  sock: Socket,
  client: process.Subject(Result(Nil, internal.PglError)),
) -> actor.Next(Store(Int, types.TypeInfo), a) {
  {
    let packet = encode.query(bootstrap_sql)

    use rows <- result.try(protocol.simple(packet, sock))

    use infos <- result.map(list.try_map(rows, parse_type_info))

    actor.send(client, Ok(Nil))

    parse_type_infos(store, infos)
  }
  |> result.unwrap(store)
  |> actor.continue
}

fn handle_lookup(
  store: Store(Int, types.TypeInfo),
  oids: List(Int),
  client: process.Subject(Result(List(types.TypeInfo), internal.PglError)),
) -> actor.Next(Store(Int, types.TypeInfo), a) {
  list.try_map(oids, store.lookup(store, _))
  |> result.replace_error(internal.TypeCacheError(
    kind: internal.NotFoundError,
    message: "Failed to find type info for OIDs",
  ))
  |> actor.send(client, _)

  actor.continue(store)
}

fn parse_type_infos(
  store: Store(Int, types.TypeInfo),
  infos: List(types.TypeInfo),
) -> Store(Int, types.TypeInfo) {
  let oid_to_info =
    infos
    |> list.map(fn(ti) { #(ti.oid, ti) })
    |> dict.from_list

  oid_to_info
  |> dict.fold(from: store, with: fn(store, oid, info) {
    info.comp_oids
    |> list.try_map(dict.get(oid_to_info, _))
    |> result.try(fn(comp_types) {
      use elem_type <- result.map(dict.get(oid_to_info, info.elem_oid))

      info
      |> types.elem_type(Some(elem_type))
      |> types.comp_types(Some(comp_types))
    })
    |> result.unwrap(info)
    |> store.insert(store, oid, _)

    store
  })
}

fn parse_type_info(
  row: List(BitArray),
) -> Result(types.TypeInfo, internal.PglError) {
  case row {
    [
      <<oid:bits>>,
      <<name:bits>>,
      <<typesend:bits>>,
      <<typereceive:bits>>,
      <<typelen:bits>>,
      <<output:bits>>,
      <<input:bits>>,
      <<elem_oid:bits>>,
      <<base_oid:bits>>,
      <<comp_oids:bits>>,
    ] -> {
      {
        use oid <- result.try(bits_to_oid(oid))
        use name <- result.try(bit_array.to_string(name))
        use typesend <- result.try(bit_array.to_string(typesend))
        use typereceive <- result.try(bit_array.to_string(typereceive))
        use typelen <- result.try(bits_to_int(typelen))
        use output <- result.try(bit_array.to_string(output))
        use input <- result.try(bit_array.to_string(input))
        use elem_oid <- result.try(bits_to_oid(elem_oid))
        use base_oid <- result.try(bits_to_oid(base_oid))
        use comp_oids <- result.map(parse_comp_oids(comp_oids))

        types.info(oid)
        |> types.name(name)
        |> types.typesend(typesend)
        |> types.typereceive(typereceive)
        |> types.typelen(typelen)
        |> types.output(output)
        |> types.input(input)
        |> types.elem_oid(elem_oid)
        |> types.base_oid(base_oid)
        |> types.comp_oids(comp_oids)
      }
      |> result.replace_error(internal.TypeCacheError(
        kind: internal.LoadError,
        message: "Failed to parse type info",
      ))
    }
    _ ->
      Error(internal.TypeCacheError(
        kind: internal.LoadError,
        message: "Unexpected type info format",
      ))
  }
}

fn parse_comp_oids(bits: BitArray) -> Result(List(Int), Nil) {
  case bits {
    <<0>> -> Ok([])
    <<>> -> Ok([])
    <<"{}":utf8>> -> Ok([])
    <<"{":utf8, rest:bits>> -> {
      bit_array.to_string(rest)
      |> result.try(do_parse_comp_oids)
    }
    _ -> Error(Nil)
  }
}

fn do_parse_comp_oids(oids: String) -> Result(List(Int), Nil) {
  string.split(oids, on: "}")
  |> list.first
  |> result.try(fn(oids1) {
    string.split(oids1, on: ",")
    |> list.try_fold(from: [], with: fn(acc, oid) {
      int.parse(oid)
      |> result.map(list.prepend(acc, _))
    })
  })
}

fn bits_to_int(bits: BitArray) -> Result(Int, Nil) {
  bit_array.to_string(bits) |> result.try(int.parse)
}

fn bits_to_oid(bits: BitArray) -> Result(Int, Nil) {
  bits_to_int(bits)
}

pub const bootstrap_sql = "SELECT t.oid AS oid, t.typname AS name, t.typsend AS typesend, t.typreceive AS typereceive, t.typlen AS typelen, t.typoutput AS output, t.typinput AS input, t.typelem AS elem_oid, coalesce(r.rngsubtype, 0) AS base_oid, ARRAY (SELECT a.atttypid FROM pg_attribute AS a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum) AS comp_oids FROM pg_type AS t LEFT JOIN pg_range AS r ON r.rngtypid = t.oid OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype) ORDER BY t.oid"
