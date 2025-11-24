import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import gleam/crypto
import gleam/erlang/atom.{type Atom}
import gleam/list
import gleam/result
import gleam/string
import pgl/internal
import pgl/internal/sasl

// Message encoding

const protocol_version_major = <<3:int-size(16)>>

const protocol_version_minor = <<0:int-size(16)>>

pub fn encode_auth_scram_client_first(client_first: BitArray) -> BitArray {
  let size = bit_array.byte_size(client_first)

  let initial_response =
    bytes_tree.from_string("SCRAM-SHA-256")
    |> bytes_tree.append(<<0>>)
    |> bytes_tree.append(<<size:int-size(32)>>)
    |> bytes_tree.append(client_first)

  let sasl_size = bytes_tree.byte_size(initial_response) + 4

  bytes_tree.from_string("p")
  |> bytes_tree.append(<<sasl_size:int-size(32)>>)
  |> bytes_tree.append_tree(initial_response)
  |> bytes_tree.to_bit_array
}

pub fn encode_scram_response(client_final: BitArray) -> BitArray {
  let size = bit_array.byte_size(client_final) + 4

  bytes_tree.from_string("p")
  |> bytes_tree.append(<<size:int-size(32)>>)
  |> bytes_tree.append(client_final)
  |> bytes_tree.to_bit_array
}

pub fn encode_startup(params: List(#(String, String))) -> BitArray {
  let encoded_params =
    list.fold(over: params, from: bytes_tree.new(), with: fn(acc, key_val) {
      acc
      |> bytes_tree.append_string(key_val.0)
      |> bytes_tree.append(<<0>>)
      |> bytes_tree.append_string(key_val.1)
      |> bytes_tree.append(<<0>>)
    })
    |> bytes_tree.to_bit_array

  let packet =
    bit_array.concat([
      protocol_version_major,
      protocol_version_minor,
      encoded_params,
      <<0>>,
    ])

  let size = bit_array.byte_size(packet) + 4

  <<size:int-size(32), packet:bits>>
}

// ---------- Scram ---------- //

pub type ServerFirst {
  ServerFirst(nonce: BitArray, salt: BitArray, iterations: Int, raw: BitArray)
}

pub fn server_first() -> ServerFirst {
  ServerFirst(nonce: <<>>, salt: <<>>, iterations: 0, raw: <<>>)
}

pub fn set_nonce(sf: ServerFirst, nonce: BitArray) -> ServerFirst {
  ServerFirst(..sf, nonce:)
}

pub fn set_salt(sf: ServerFirst, salt: BitArray) -> ServerFirst {
  ServerFirst(..sf, salt:)
}

pub fn set_iterations(sf: ServerFirst, iterations: Int) -> ServerFirst {
  ServerFirst(..sf, iterations:)
}

pub fn set_raw(sf: ServerFirst, raw: BitArray) -> ServerFirst {
  ServerFirst(..sf, raw:)
}

pub fn client_first(username: BitArray, nonce: BitArray) -> BitArray {
  [<<"n,,":utf8>>, ..client_first_bare(username, nonce)] |> bit_array.concat
}

fn client_first_bare(username: BitArray, nonce: BitArray) -> List(BitArray) {
  [<<"n=":utf8>>, username, <<",r=":utf8>>, nonce]
}

pub fn get_nonce(num_random_bytes: Int) -> BitArray {
  let random = crypto.strong_random_bytes(num_random_bytes)
  let unique = <<unique_int()>>
  let nonce_bin = <<
    num_random_bytes,
    random:bits-size(num_random_bytes),
    unique:bits,
  >>
  bit_array.base64_encode(nonce_bin, True)
  |> bit_array.from_string
}

pub fn client_final(
  server_first: ServerFirst,
  client_nonce: BitArray,
  username: BitArray,
  password: BitArray,
) -> #(BitArray, BitArray) {
  let channel_binding = <<"c=biws":utf8>>
  let nonce = [<<"r=":utf8>>, server_first.nonce]

  let salted_password =
    sasl.validate(password)
    |> result.unwrap(<<>>)
    |> hi(server_first.salt, server_first.iterations)
  let client_key = hmac(salted_password, <<"Client Key":utf8>>)

  let stored_key = h(client_key)
  let client_first_bare = client_first_bare(username, client_nonce)

  let auth_message =
    collect_bits(bytes_tree.new(), client_first_bare)
    |> bytes_tree.append(<<",":utf8>>)
    |> bytes_tree.append(server_first.raw)
    |> bytes_tree.append(<<",":utf8>>)
    |> bytes_tree.append(channel_binding)
    |> bytes_tree.append(<<",":utf8>>)
    |> collect_bits(nonce)
    |> bytes_tree.to_bit_array

  let client_signature = hmac(stored_key, auth_message)
  let client_proof = bin_xor(client_key, client_signature)
  let encoded_client_proof =
    client_proof
    |> bit_array.base64_encode(True)
    |> bit_array.from_string

  let server_signature =
    hmac(salted_password, <<"Server Key":utf8>>)
    |> hmac(auth_message)

  let encoded_client_final =
    bytes_tree.new()
    |> bytes_tree.append(channel_binding)
    |> bytes_tree.append(<<",":utf8>>)
    |> collect_bits(nonce)
    |> bytes_tree.append(<<",p=":utf8>>)
    |> bytes_tree.append(encoded_client_proof)
    |> bytes_tree.to_bit_array

  #(encoded_client_final, server_signature)
}

fn collect_bits(collector: BytesTree, bits_list: List(BitArray)) -> BytesTree {
  bits_list
  |> list.fold(collector, fn(acc, bits) { acc |> bytes_tree.append(bits) })
}

pub fn parse_server_first(
  server_first: BitArray,
  client_nonce: BitArray,
) -> Result(ServerFirst, internal.PglError) {
  let parts =
    binary_split_global(server_first, <<",":utf8>>) |> result.unwrap([])

  let parts = case list.length(parts) == 3 {
    True -> parse_parts(parts)
    False -> Error(internal.server_first_error("parts parsing error"))
  }

  use sf <- result.try(parts)

  case check_nonce(client_nonce, sf.nonce) {
    True -> sf |> set_raw(server_first) |> Ok
    False -> Error(internal.server_first_error("Check nonce error"))
  }
}

fn parse_parts(parts: List(BitArray)) -> Result(ServerFirst, internal.PglError) {
  parts
  |> list.try_fold(server_first(), with: fn(sf, part) {
    case part {
      <<"r=":utf8, nonce:bits>> -> sf |> set_nonce(nonce) |> Ok
      <<"s=":utf8, rest:bits>> -> {
        let parsed =
          rest
          |> bit_array.to_string
          |> result.try(bit_array.base64_decode)
          |> result.map(fn(salt) { sf |> set_salt(salt) })

        case parsed {
          Ok(parsed) -> Ok(parsed)
          Error(_) -> Error(internal.server_first_error("salt parsing error"))
        }
      }
      <<"i=":utf8, rest:bits>> -> sf |> set_iterations(bits_to_int(rest)) |> Ok
      _ -> Ok(sf)
    }
  })
}

pub fn parse_server_final(
  server_final: BitArray,
) -> Result(BitArray, internal.PglError) {
  case server_final {
    <<"v=":utf8, final:bits>> -> {
      let parsed =
        bit_array.to_string(final)
        |> result.map(string.split(_, ","))
        |> result.try(list.first)
        |> result.try(bit_array.base64_decode)

      case parsed {
        Ok(parsed) -> Ok(parsed)
        _ -> Error(internal.server_final_error("payload error"))
      }
    }
    <<"e=":utf8, _error:bits>> ->
      Error(internal.server_final_error("payload error"))
    _bits -> Error(internal.server_final_error("unexpected payload"))
  }
}

fn check_nonce(client_nonce: BitArray, server_nonce: BitArray) -> Bool {
  let size = bit_array.byte_size(client_nonce)

  case server_nonce {
    <<_client_nonce:bits-size(size), _rest:bits>> -> True
    _ -> False
  }
}

fn hi(str: BitArray, salt: BitArray, i: Int) {
  let u1 = hmac(str, <<salt:bits, 1:int-big-size(32)>>)
  do_hi(str, u1, u1, i - 1)
}

fn do_hi(str: BitArray, u: BitArray, hi: BitArray, i: Int) -> BitArray {
  case i > 0 {
    False -> hi
    True -> {
      let u2 = hmac(str, u)
      let hi1 = bin_xor(hi, u2)
      do_hi(str, u2, hi1, i - 1)
    }
  }
}

fn hmac(key: BitArray, data: BitArray) {
  crypto.hmac(data, crypto.Sha256, key)
}

fn h(str: BitArray) {
  crypto.hash(crypto.Sha256, str)
}

fn bin_xor(b1: BitArray, b2: BitArray) -> BitArray {
  crypto_exor(b1, b2)
}

@external(erlang, "crypto", "exor")
fn crypto_exor(b1: BitArray, b2: BitArray) -> BitArray

@external(erlang, "pgl_ffi", "unique_int")
fn unique_int() -> Int

@external(erlang, "erlang", "binary_to_integer")
fn bits_to_int(data: BitArray) -> Int

fn binary_split_global(
  data: BitArray,
  pattern: BitArray,
) -> Result(List(BitArray), Nil) {
  let global = atom.create("global")

  erlang_binary_split(data, pattern, [global]) |> Ok
}

@external(erlang, "binary", "split")
fn erlang_binary_split(
  data: BitArray,
  pattern: BitArray,
  options: List(Atom),
) -> List(BitArray)
// ---------- Scram ---------- //
