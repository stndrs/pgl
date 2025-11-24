import gleam/bit_array
import gleam/bytes_tree
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

pub fn bind(
  portal_name: BitArray,
  statement_name: BitArray,
  params: List(v),
  type_infos: List(t),
  with encoder: fn(v, t) -> Result(BitArray, String),
) -> Result(BitArray, String) {
  let param_count = list.length(params)
  let param_count_bin = <<param_count:int-size(16)>>

  list.strict_zip(params, type_infos)
  |> result.replace_error(
    "Parameter list and cached types list are different lengths",
  )
  |> result.try(fn(zipped) {
    list.try_map(zipped, fn(zip) {
      let #(param, type_info) = zip

      encoder(param, type_info)
    })
  })
  |> result.map(fn(param_values) {
    let param_formats_bin =
      list.map(param_values, with: fn(_) { <<1:int-size(16)>> })
      |> list.prepend(param_count_bin)
      |> bit_array.concat

    let results = <<1:int-size(16), 1:int-size(16)>>

    let packet =
      bytes_tree.from_bit_array(portal_name)
      |> bytes_tree.append(<<0>>)
      |> bytes_tree.append(statement_name)
      |> bytes_tree.append(<<0>>)
      |> bytes_tree.append(param_formats_bin)
      |> bytes_tree.append(param_count_bin)
      |> bytes_tree.append(bit_array.concat(param_values))
      |> bytes_tree.append(results)
      |> bytes_tree.to_bit_array

    let packet_len = bit_array.byte_size(packet) + 4

    bytes_tree.from_string("B")
    |> bytes_tree.append(<<packet_len:int-size(32)>>)
    |> bytes_tree.append(packet)
    |> bytes_tree.to_bit_array
  })
}

pub opaque type Final {
  Flush
  Sync
}

pub type Query(v, t) {
  Query(
    sql: String,
    params: List(v),
    type_infos: List(t),
    encoder: Option(fn(v, t) -> Result(BitArray, String)),
    describe: Describe,
    execute: Bool,
    final: Option(Final),
  )
}

pub fn needs_sync(msg: Query(v, t)) -> Bool {
  case msg.final {
    Some(Sync) -> False
    _ -> True
  }
}

pub fn cached(
  sql: String,
  params: List(v),
  type_infos: List(t),
  encoder: fn(v, t) -> Result(BitArray, String),
) -> Query(v, t) {
  Query(
    sql:,
    params:,
    type_infos:,
    encoder: Some(encoder),
    describe: Portal,
    execute: True,
    final: None,
  )
}

pub fn uncached(sql: String, params: List(v)) -> Query(v, t) {
  Query(
    sql:,
    params:,
    type_infos: [],
    encoder: None,
    describe: Statement,
    execute: False,
    final: Some(Flush),
  )
}

pub fn with_sync(msg: Query(v, t)) -> Query(v, t) {
  Query(..msg, final: Some(Sync))
}

pub fn to_bit_array(msg: Query(v, t)) -> BitArray {
  let parse = parse("", msg.sql, [])

  let bind = case msg.encoder {
    None -> <<>>
    Some(encoder) -> {
      bind(<<>>, <<>>, msg.params, msg.type_infos, encoder)
      |> result.unwrap(<<>>)
    }
  }

  let describe = describe(msg.describe, "")
  let exec = case msg.execute {
    True -> execute("", 0)
    False -> <<>>
  }

  let final = case msg.final {
    Some(Flush) -> flush()
    Some(Sync) -> sync()
    None -> <<>>
  }

  bit_array.concat([parse, bind, describe, exec, final])
}

// Query encoding

pub fn ssl_request() -> BitArray {
  <<8:int-size(32), 1234:int-size(16), 5679:int-size(16)>>
}

pub fn query(query: String) -> BitArray {
  encode_string("Q", query)
}

pub fn password(password: String) -> BitArray {
  encode_string("p", password)
}

fn encode_string(identifier: String, message: String) -> BitArray {
  let msg_bin = bit_array.from_string(message)
  let msg_len = bit_array.byte_size(msg_bin) + 5

  bytes_tree.from_string(identifier)
  |> bytes_tree.append(<<msg_len:int-size(32)>>)
  |> bytes_tree.append(msg_bin)
  |> bytes_tree.append(<<0>>)
  |> bytes_tree.to_bit_array
}

pub fn parse(name: String, query: String, data_types: List(Int)) -> BitArray {
  let dt_bin = list.map(data_types, fn(dt) { <<dt:int-size(32)>> })

  let dt_count = list.length(data_types)

  let start =
    bytes_tree.from_string(name)
    |> bytes_tree.append(<<0>>)
    |> bytes_tree.append_string(query)
    |> bytes_tree.append(<<0>>)
    |> bytes_tree.append(<<dt_count:int-size(16)>>)
    |> bytes_tree.to_bit_array

  let packet = bit_array.concat([start, ..dt_bin])

  let packet_len = bit_array.byte_size(packet) + 4

  bytes_tree.from_string("P")
  |> bytes_tree.append(<<packet_len:int-size(32)>>)
  |> bytes_tree.append(packet)
  |> bytes_tree.to_bit_array
}

pub type Describe {
  Portal
  Statement
}

pub fn describe(pos: Describe, name: String) -> BitArray {
  let name_bits = bit_array.from_string(name)
  let msg_len = bit_array.byte_size(name_bits) + 6
  let what_byte = case pos {
    Portal -> "P"
    Statement -> "S"
  }

  bytes_tree.from_string("D")
  |> bytes_tree.append(<<msg_len:int-size(32)>>)
  |> bytes_tree.append_string(what_byte)
  |> bytes_tree.append(name_bits)
  |> bytes_tree.append(<<0>>)
  |> bytes_tree.to_bit_array
}

pub fn flush() -> BitArray {
  <<"H":utf8, 4:int-size(32)>>
}

pub fn sync() -> BitArray {
  <<"S":utf8, 4:int-size(32)>>
}

pub fn execute(name: String, num: Int) -> BitArray {
  let msg_len = { bit_array.from_string(name) |> bit_array.byte_size } + 9

  <<"E":utf8, msg_len:int-size(32), name:utf8, 0, num:int-size(32)>>
}
