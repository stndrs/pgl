import gleam/bit_array
import gleam/bytes_tree
import gleam/crypto
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import pgl/internal
import pgl/internal/sasl

// Message encoding

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
      internal.protocol_version_major,
      internal.protocol_version_minor,
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

pub fn client_first(user: BitArray, nonce: BitArray) -> BitArray {
  [<<"n,,":utf8>>, <<"n=":utf8>>, user, <<",r=":utf8>>, nonce]
  |> bit_array.concat
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

const sha_256 = crypto.Sha256

pub fn client_final(
  server_first: ServerFirst,
  client_nonce: BitArray,
  username: BitArray,
  password: BitArray,
) -> #(BitArray, BitArray) {
  let channel_binding = <<"c=biws":utf8>>
  let nonce = [<<"r=":utf8>>, server_first.nonce]

  let salted_password =
    password
    |> sasl.validate
    |> result.unwrap(<<>>)
    |> hi(server_first.salt, server_first.iterations)

  let client_key = crypto.hmac(<<"Client Key":utf8>>, sha_256, salted_password)

  let auth_message =
    [<<"n=":utf8>>, username, <<",r=":utf8>>, client_nonce]
    |> list.fold(bytes_tree.new(), bytes_tree.append)
    |> bytes_tree.append(<<",":utf8>>)
    |> bytes_tree.append(server_first.raw)
    |> bytes_tree.append(<<",":utf8>>)
    |> bytes_tree.append(channel_binding)
    |> bytes_tree.append(<<",":utf8>>)
    |> list.fold(nonce, _, bytes_tree.append)
    |> bytes_tree.to_bit_array

  let client_signature =
    sha_256
    |> crypto.hash(client_key)
    |> crypto.hmac(auth_message, sha_256, _)

  let encoded_client_proof =
    client_key
    |> bin_xor(client_signature)
    |> bit_array.base64_encode(True)
    |> bit_array.from_string

  let server_signature =
    salted_password
    |> crypto.hmac(<<"Server Key":utf8>>, sha_256, _)
    |> crypto.hmac(auth_message, sha_256, _)

  let encoded_client_final =
    bytes_tree.new()
    |> bytes_tree.append(channel_binding)
    |> bytes_tree.append(<<",":utf8>>)
    |> list.fold(nonce, _, bytes_tree.append)
    |> bytes_tree.append(<<",p=":utf8>>)
    |> bytes_tree.append(encoded_client_proof)
    |> bytes_tree.to_bit_array

  #(encoded_client_final, server_signature)
}

pub fn parse_server_first(
  server_first: BitArray,
  client_nonce: BitArray,
) -> Result(ServerFirst, internal.PglError) {
  let parts =
    bit_array.to_string(server_first)
    |> result.map(string.split(_, on: ","))
    |> result.unwrap([])

  case parts {
    ["r=" <> nonce, "s=" <> salt, "i=" <> iters] -> {
      let nonce = bit_array.from_string(nonce)
      use salt <- result.try(bit_array.base64_decode(salt))
      use iterations <- result.try(int.parse(iters))

      let size = bit_array.byte_size(client_nonce)

      case nonce {
        <<_:bits-size(size), _:bits>> -> {
          Ok(ServerFirst(nonce:, salt:, iterations:, raw: server_first))
        }
        _ -> Error(Nil)
      }
    }
    _ -> Error(Nil)
  }
  |> result.map_error(fn(_) {
    internal.server_first_error("server_first parse error")
  })
}

pub fn parse_server_final(
  server_final: BitArray,
) -> Result(BitArray, internal.PglError) {
  case server_final {
    <<"v=":utf8, final:bits>> -> {
      bit_array.to_string(final)
      |> result.map(string.split(_, ","))
      |> result.try(list.first)
      |> result.try(bit_array.base64_decode)
      |> result.map_error(fn(_) { internal.server_final_error("payload error") })
    }
    <<"e=":utf8, _error:bits>> ->
      Error(internal.server_final_error("payload error"))
    _bits -> Error(internal.server_final_error("unexpected payload"))
  }
}

fn hi(str: BitArray, salt: BitArray, i: Int) {
  let u1 = crypto.hmac(<<salt:bits, 1:int-big-size(32)>>, crypto.Sha256, str)

  do_hi(str, u1, u1, i - 1)
}

fn do_hi(str: BitArray, u: BitArray, hi: BitArray, i: Int) -> BitArray {
  case i > 0 {
    False -> hi
    True -> {
      let u2 = crypto.hmac(u, crypto.Sha256, str)
      let hi1 = bin_xor(hi, u2)
      do_hi(str, u2, hi1, i - 1)
    }
  }
}

@external(erlang, "crypto", "exor")
fn bin_xor(b1: BitArray, b2: BitArray) -> BitArray

@external(erlang, "pgl_ffi", "unique_int")
fn unique_int() -> Int
