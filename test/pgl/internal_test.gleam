import gleam/int
import neon/net
import pgl/internal
import rasa/table

pub fn format_error_test() {
  assert "(Name)" == internal.format_error("Name", "")

  assert "(Name) message" == internal.format_error("Name", "message")
}

pub fn format_error_with_values_test() {
  assert "(Name) message, first: 10, second: 20"
    == internal.format_error_with_values(
      "Name",
      "message",
      [#("first", 10), #("second", 20)],
      int.to_string,
    )
}

pub fn auth_failed_test() {
  assert "(AuthenticationError[AuthenticationFailed]) message"
    == internal.AuthenticationError(internal.AuthenticationFailed, "message")
    |> internal.error_to_string
}

pub fn method_not_implemented_error_to_string_test() {
  assert "(AuthenticationError[MethodNotImplemented]) message"
    == internal.AuthenticationError(internal.MethodNotImplemented, "message")
    |> internal.error_to_string
}

pub fn socket_error_to_string_test() {
  assert "(SocketError[econnreset]) message"
    == internal.SocketError(internal.Posix(net.Econnreset), "message")
    |> internal.error_to_string
}

pub fn sasl_server_error_to_string_test() {
  assert "(ProtocolError[SaslServerError]) message"
    == internal.ProtocolError(internal.SaslServerError, "message")
    |> internal.error_to_string
}

pub fn sasl_server_final_to_string_test() {
  assert "(ProtocolError[SaslServerFinal]) message"
    == internal.ProtocolError(internal.SaslServerFinal, "message")
    |> internal.error_to_string
}

pub fn sasl_server_first_to_string_test() {
  assert "(ProtocolError[SaslServerFirst]) message"
    == internal.ProtocolError(internal.SaslServerFirst, "message")
    |> internal.error_to_string
}

pub fn decoding_error_to_string_test() {
  assert "(ProtocolError[DecodingError]) message"
    == internal.ProtocolError(internal.DecodingError, "message")
    |> internal.error_to_string
}

pub fn message_error_to_string_test() {
  assert "(ProtocolError[MessageError]) message"
    == internal.ProtocolError(internal.MessageError, "message")
    |> internal.error_to_string
}

pub fn ssl_error_to_string_test() {
  assert "(ProtocolError[SSLError]) message"
    == internal.ProtocolError(internal.SslError, "message")
    |> internal.error_to_string
}

pub fn with_rescue_test() {
  let assert Error(Nil) = {
    use <- internal.with_rescue()

    panic as "Failure"
  }
}

pub fn on_crash_test() {
  let data = table.new() |> table.build

  let assert Error(Nil) = {
    use <- internal.with_rescue()

    use <- internal.on_crash(fn() { table.insert(data, "key", 10) })

    panic as "Failure"
  }

  let assert Ok(10) = table.lookup(data, "key")
}

pub fn assert_on_crash_ok_test() {
  let data = table.new() |> table.build

  let assert Error(Nil) = {
    use <- internal.with_rescue()

    use <- internal.assert_on_crash(
      fn() { table.insert(data, "key", 10) },
      "message",
    )

    panic as "Failure"
  }

  let assert Ok(10) = table.lookup(data, "key")
}

pub fn assert_on_crash_error_test() {
  let assert Error(Nil) = {
    use <- internal.with_rescue()

    use <- internal.assert_on_crash(fn() { Error(Nil) }, "message")

    panic as "Failure"
  }
}
