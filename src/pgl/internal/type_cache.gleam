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
import pg_value/type_info.{type TypeInfo}
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/socket.{type Socket}
import rasa/table

pub opaque type TypeCache {
  TypeCache(
    name: process.Name(Message),
    handle_connect: fn() -> Result(Socket, Nil),
  )
}

pub opaque type Message {
  Load(client: process.Subject(Result(Nil, Nil)), sock: Socket)
  Lookup(client: process.Subject(Result(List(TypeInfo), Nil)), oids: List(Int))
  Shutdown
}

const type_cache_name = "pgl_type_cache"

pub fn new() -> TypeCache {
  let handle_connect = fn() { panic as "TypeCache connect not configured" }

  type_cache_name
  |> process.new_name
  |> TypeCache(handle_connect:)
}

pub fn on_connect(
  type_cache: TypeCache,
  handle_connect: fn() -> Result(Socket, Nil),
) {
  TypeCache(..type_cache, handle_connect:)
}

pub fn start(type_cache: TypeCache) -> actor.StartResult(Nil) {
  actor.new_with_initialiser(1000, fn(subj) {
    let selector = process.new_selector() |> process.select(subj)

    table.new()
    |> table.with_access(table.Private)
    |> table.build
    |> actor.initialised
    |> actor.selecting(selector)
    |> Ok
  })
  |> actor.named(type_cache.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn supervised(type_cache: TypeCache) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start(type_cache) })
  |> supervision.timeout(1000)
  |> supervision.restart(supervision.Transient)
}

pub fn load(type_cache: TypeCache) -> Result(Nil, Nil) {
  type_cache.handle_connect()
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(sock) {
    let res =
      type_cache.name
      |> process.named_subject
      |> actor.call(1000, Load(_, sock))

    let _ = socket.shutdown(sock)
    res
  })
}

pub fn lookup(
  type_cache: TypeCache,
  oids: List(Int),
) -> Result(List(TypeInfo), Nil) {
  let do_lookup = fn(oids) {
    type_cache.name
    |> process.named_subject
    |> actor.call(1000, Lookup(_, oids))
  }

  do_lookup(oids)
  |> result.lazy_or(fn() {
    load(type_cache)
    |> result.try(fn(_) { do_lookup(oids) })
  })
}

pub fn shutdown(type_cache: TypeCache) -> Nil {
  process.named_subject(type_cache.name) |> actor.send(Shutdown)
}

fn handle_message(
  table: table.Table(Int, TypeInfo),
  message: Message,
) -> actor.Next(table.Table(Int, TypeInfo), a) {
  case message {
    Load(client, sock) -> handle_load(table, sock, client)
    Lookup(client, oids) -> handle_lookup(table, oids, client)
    Shutdown -> actor.stop()
  }
}

fn handle_load(
  table: table.Table(Int, TypeInfo),
  sock: Socket,
  client: process.Subject(Result(Nil, Nil)),
) -> actor.Next(table.Table(Int, TypeInfo), a) {
  bootstrap_sql
  |> encode.query
  |> protocol.simple(sock)
  |> result.map(fn(rows) {
    case list.try_map(rows, parse_type_info) {
      Ok(infos) -> {
        actor.send(client, Ok(Nil))

        parse_type_infos(table, infos)
      }
      Error(_) -> {
        actor.send(client, Error(Nil))

        table
      }
    }
  })
  |> result.lazy_unwrap(fn() {
    actor.send(client, Error(Nil))

    table
  })
  |> actor.continue
}

fn handle_lookup(
  table: table.Table(Int, TypeInfo),
  oids: List(Int),
  client: process.Subject(Result(List(TypeInfo), Nil)),
) -> actor.Next(table.Table(Int, TypeInfo), a) {
  oids
  |> list.try_map(table.lookup(table, _))
  |> actor.send(client, _)

  actor.continue(table)
}

fn parse_type_infos(
  table: table.Table(Int, TypeInfo),
  infos: List(TypeInfo),
) -> table.Table(Int, TypeInfo) {
  let oid_to_info =
    infos
    |> list.map(fn(ti) { #(ti.oid, ti) })
    |> dict.from_list

  oid_to_info
  |> dict.fold(from: table, with: fn(table, oid, info) {
    let _ =
      info.comp_oids
      |> list.try_map(dict.get(oid_to_info, _))
      |> result.try(fn(comp_types) {
        use elem_type <- result.map(dict.get(oid_to_info, info.elem_oid))

        info
        |> type_info.elem_type(Some(elem_type))
        |> type_info.comp_types(Some(comp_types))
      })
      |> result.unwrap(info)
      |> table.insert(table, oid, _)

    table
  })
}

fn parse_type_info(row: List(BitArray)) -> Result(TypeInfo, Nil) {
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

        type_info.new(oid)
        |> type_info.name(name)
        |> type_info.typesend(typesend)
        |> type_info.typereceive(typereceive)
        |> type_info.typelen(typelen)
        |> type_info.output(output)
        |> type_info.input(input)
        |> type_info.elem_oid(elem_oid)
        |> type_info.base_oid(base_oid)
        |> type_info.comp_oids(comp_oids)
      }
    }
    _ -> Error(Nil)
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
    |> result.map(list.reverse)
  })
}

fn bits_to_int(bits: BitArray) -> Result(Int, Nil) {
  bit_array.to_string(bits) |> result.try(int.parse)
}

fn bits_to_oid(bits: BitArray) -> Result(Int, Nil) {
  bits_to_int(bits)
}

pub const bootstrap_sql = "SELECT t.oid AS oid, t.typname AS name, t.typsend AS typesend, t.typreceive AS typereceive, t.typlen AS typelen, t.typoutput AS output, t.typinput AS input, t.typelem AS elem_oid, coalesce(r.rngsubtype, 0) AS base_oid, ARRAY (SELECT a.atttypid FROM pg_attribute AS a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum) AS comp_oids FROM pg_type AS t LEFT JOIN pg_range AS r ON r.rngtypid = t.oid OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype) ORDER BY t.oid"
