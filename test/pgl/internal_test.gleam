import pgl/internal

pub fn decoding_error_test() {
  let assert internal.ProtocolError(
    kind: internal.DecodingError,
    message: "test message",
  ) = internal.decode_error("test message")
}

pub fn encoding_error_test() {
  let assert internal.ProtocolError(
    kind: internal.EncodingError,
    message: "test message",
  ) = internal.encode_error("test message")
}

pub fn message_error_test() {
  let assert internal.ProtocolError(
    kind: internal.MessageError,
    message: "test message",
  ) = internal.message_error("test message")
}

pub fn rollback_test() {
  let assert internal.RollbackError(cause: Nil) = internal.rollback(Nil)
}

pub fn not_in_transaction_test() {
  let assert internal.NotInTransaction(message: "test message") =
    internal.not_in_transaction("test message")
}

pub fn type_encode_error_test() {
  let assert internal.TypeError(
    kind: internal.TypeEncodeError,
    message: "test message",
  ) = internal.type_encode_error("test message")
}

pub fn type_decode_error_test() {
  let assert internal.TypeError(
    kind: internal.TypeDecodeError,
    message: "test message",
  ) = internal.type_decode_error("test message")
}
