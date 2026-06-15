import gleam/bit_array
import gleam/bool
import gleam/bytes_tree
import gleam/crypto
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import pgl/internal
import pgl/internal/sasl

pub type ServerFirst {
  ServerFirst(nonce: BitArray, salt: BitArray, iterations: Int, raw: BitArray)
}

pub fn client_first(
  user: BitArray,
  nonce: BitArray,
) -> Result(BitArray, internal.InternalError) {
  use escaped <- result.map(escape_username(user))

  <<"n,,n=":utf8, escaped:bits, ",r=":utf8, nonce:bits>>
}

// https://datatracker.ietf.org/doc/html/rfc5802#section-5.1
fn escape_username(user: BitArray) -> Result(BitArray, internal.InternalError) {
  user
  |> bit_array.to_string
  |> result.replace_error({
    internal.SaslClientFirst
    |> internal.ProtocolError("Invalid username")
  })
  |> result.map(fn(user) {
    user
    |> string.replace("=", "=3D")
    |> string.replace(",", "=2C")
    |> bit_array.from_string
  })
}

pub fn get_nonce(num_random_bytes: Int) -> BitArray {
  crypto.strong_random_bytes(num_random_bytes)
  |> bit_array.base64_encode(True)
  |> bit_array.from_string
}

pub fn client_final(
  server_first: ServerFirst,
  client_nonce: BitArray,
  username: BitArray,
  password: BitArray,
) -> Result(#(BitArray, BitArray), internal.InternalError) {
  let channel_binding = <<"c=biws":utf8>>
  let nonce = [<<"r=":utf8>>, server_first.nonce]

  password
  |> sasl.validate
  |> result.replace_error({
    internal.SaslClientFinal
    |> internal.ProtocolError("Invalid password")
  })
  |> result.try(fn(valid_password) {
    let salted_password =
      valid_password
      |> hi(server_first.salt, server_first.iterations)

    let client_key =
      crypto.hmac(<<"Client Key":utf8>>, crypto.Sha256, salted_password)

    use escaped_username <- result.map(escape_username(username))

    let auth_message =
      <<"n=":utf8, escaped_username:bits, ",r=":utf8, client_nonce:bits>>
      |> bytes_tree.from_bit_array
      |> bytes_tree.append(<<",":utf8>>)
      |> bytes_tree.append(server_first.raw)
      |> bytes_tree.append(<<",":utf8>>)
      |> bytes_tree.append(channel_binding)
      |> bytes_tree.append(<<",":utf8>>)
      |> list.fold(nonce, _, bytes_tree.append)
      |> bytes_tree.to_bit_array

    let client_signature =
      crypto.Sha256
      |> crypto.hash(client_key)
      |> crypto.hmac(auth_message, crypto.Sha256, _)

    let encoded_client_proof =
      client_key
      |> bin_xor(client_signature)
      |> bit_array.base64_encode(True)
      |> bit_array.from_string

    let server_signature =
      salted_password
      |> crypto.hmac(<<"Server Key":utf8>>, crypto.Sha256, _)
      |> crypto.hmac(auth_message, crypto.Sha256, _)

    let encoded_client_final =
      bytes_tree.new()
      |> bytes_tree.append(channel_binding)
      |> bytes_tree.append(<<",":utf8>>)
      |> list.fold(nonce, _, bytes_tree.append)
      |> bytes_tree.append(<<",p=":utf8>>)
      |> bytes_tree.append(encoded_client_proof)
      |> bytes_tree.to_bit_array

    #(encoded_client_final, server_signature)
  })
}

pub fn parse_server_first(
  server_first: BitArray,
  client_nonce: BitArray,
) -> Result(ServerFirst, internal.InternalError) {
  let parts =
    bit_array.to_string(server_first)
    |> result.map(string.split(_, on: ","))
    |> result.unwrap([])

  case parts {
    ["r=" <> nonce, "s=" <> salt, "i=" <> iters] -> {
      let nonce = bit_array.from_string(nonce)
      use salt <- result.try(bit_array.base64_decode(salt))
      use iterations <- result.try(int.parse(iters))

      use <- bool.guard(iterations < 4096, Error(Nil))

      let size = bit_array.bit_size(client_nonce)

      case nonce {
        <<prefix:bits-size(size), _:bits>> -> {
          case prefix == client_nonce {
            True ->
              Ok(ServerFirst(nonce:, salt:, iterations:, raw: server_first))
            False -> Error(Nil)
          }
        }
        _ -> Error(Nil)
      }
    }
    _ -> Error(Nil)
  }
  |> result.map_error(fn(_) {
    internal.ProtocolError(
      kind: internal.SaslServerFirst,
      message: "Failed to parse server_first",
    )
  })
}

pub fn parse_server_final(
  server_final: BitArray,
) -> Result(BitArray, internal.InternalError) {
  case server_final {
    // https://datatracker.ietf.org/doc/html/rfc5802 - p. 17
    <<"v=":utf8, final:bits>> -> {
      bit_array.to_string(final)
      |> result.map(string.split(_, ","))
      |> result.try(list.first)
      |> result.try(bit_array.base64_decode)
      |> result.map_error(fn(_) {
        internal.ProtocolError(
          kind: internal.SaslServerFinal,
          message: "Failed to parse server_final",
        )
      })
    }
    // https://datatracker.ietf.org/doc/html/rfc5802 - p. 18
    <<"e=":utf8, error:bits>> -> {
      let error_value =
        bit_array.to_string(error)
        |> result.unwrap("")

      internal.ProtocolError(
        kind: internal.SaslServerError,
        message: "Server error: '" <> error_value <> "'",
      )
      |> Error
    }
    _bits ->
      internal.ProtocolError(
        kind: internal.SaslServerFinal,
        message: "Unexpected SASL server final payload",
      )
      |> Error
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
