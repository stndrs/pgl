import pgl/internal

pub fn auth_failed_test() {
  assert "(AuthenticationFailed) message"
    == internal.AuthenticationError(internal.AuthenticationFailed, "message")
    |> internal.error_to_string
}

pub fn method_not_implemented_error_to_string_test() {
  assert "(MethodNotImplemented) message"
    == internal.AuthenticationError(internal.MethodNotImplemented, "message")
    |> internal.error_to_string
}

pub fn socket_error_to_string_test() {
  assert "(SocketError[econnreset]) message"
    == internal.SocketError(internal.Econnreset, "message")
    |> internal.error_to_string
}

pub fn sasl_server_error_to_string_test() {
  assert "(SaslServerError) message"
    == internal.ProtocolError(internal.SaslServerError, "message")
    |> internal.error_to_string
}

pub fn sasl_server_final_to_string_test() {
  assert "(SaslServerFinal) message"
    == internal.ProtocolError(internal.SaslServerFinal, "message")
    |> internal.error_to_string
}

pub fn sasl_server_first_to_string_test() {
  assert "(SaslServerFirst) message"
    == internal.ProtocolError(internal.SaslServerFirst, "message")
    |> internal.error_to_string
}

pub fn decoding_error_to_string_test() {
  assert "(DecodingError) message"
    == internal.ProtocolError(internal.DecodingError, "message")
    |> internal.error_to_string
}

pub fn message_error_to_string_test() {
  assert "(MessageError) message"
    == internal.ProtocolError(internal.MessageError, "message")
    |> internal.error_to_string
}

pub fn ssl_error_to_string_test() {
  assert "(SSLError) message"
    == internal.ProtocolError(internal.SslError, "message")
    |> internal.error_to_string
}
