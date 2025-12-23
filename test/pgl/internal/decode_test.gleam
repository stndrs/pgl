import gleam/dict
import gleam/list
import gleam/result
import pgl/internal
import pgl/internal/decode

// Message decode tests

pub fn decode_close_complete_test() {
  let assert Ok(internal.CloseComplete) = decode.message(<<"3":utf8>>, <<>>)
}

pub fn decode_close_complete_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for CloseComplete",
  )) = decode.message(<<"3":utf8>>, <<"unexpected":utf8>>)
}

pub fn decode_empty_query_response_test() {
  let assert Ok(internal.EmptyQueryResponse) =
    decode.message(<<"I":utf8>>, <<>>)
}

pub fn decode_empty_query_response_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for EmptyQueryResponse",
  )) = decode.message(<<"I":utf8>>, <<"unexpected":utf8>>)
}

pub fn decode_copy_done_test() {
  let assert Ok(internal.CopyDone) = decode.message(<<"c":utf8>>, <<>>)
}

pub fn decode_copy_done_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for CopyDone",
  )) = decode.message(<<"c":utf8>>, <<"unexpected":utf8>>)
}

pub fn decode_portal_suspended_test() {
  let assert Ok(internal.PortalSuspended) = decode.message(<<"s":utf8>>, <<>>)
}

pub fn decode_portal_suspended_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for PortalSuspended",
  )) = decode.message(<<"s":utf8>>, <<"unexpected":utf8>>)
}

pub fn decode_copy_data_test() {
  let assert Ok(internal.CopyData(data: <<>>)) =
    decode.message(<<"d":utf8>>, <<>>)
}

pub fn decode_data_row_test() {
  let assert Ok(internal.DataRow([<<100:int-size(64)>>])) =
    decode.message(<<"D":utf8>>, <<
      2:int-size(16),
      8:int-size(32),
      100:big-int-size(64),
    >>)
}

pub fn decode_data_row_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for DataRow",
  )) = decode.message(<<"D":utf8>>, <<>>)
}

pub fn decode_data_row_values_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Invalid data row",
  )) = decode.message(<<"D":utf8>>, <<2:int-size(16), 8:int-size(32)>>)
}

pub fn decode_backend_key_data_test() {
  let assert Ok(internal.BackendKeyData(proc_id: 3352, secret: 2_173_604_095)) =
    decode.message(<<"K":utf8>>, <<
      3352:int-size(32),
      2_173_604_095:int-size(32),
    >>)
}

pub fn decode_backend_key_data_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for BackendKeyData",
  )) = decode.message(<<"K":utf8>>, <<>>)
}

pub fn decode_parameter_status_test() {
  let assert Ok(internal.ParameterStatus(name: "TimeZone", value: "Etc/UTC")) =
    decode.message(<<"S":utf8>>, <<"TimeZone":utf8, 0, "Etc/UTC":utf8, 0>>)
}

pub fn decode_authentication_test() {
  [
    #(<<0:int-size(32)>>, internal.AuthenticationOk),
    #(<<2:int-size(32)>>, internal.AuthenticationKerberosV5),
    #(<<3:int-size(32)>>, internal.AuthenticationCleartextPassword),
    #(
      <<5:int-size(32), "salt":utf8>>,
      internal.AuthenticationMD5Password(salt: <<"salt":utf8>>),
    ),
    #(<<6:int-size(32)>>, internal.AuthenticationSCM),
    #(<<7:int-size(32)>>, internal.AuthenticationGSS),
    #(
      <<8:int-size(32), "data":utf8>>,
      internal.AuthenticationGSSContinue(data: <<"data":utf8>>),
    ),
    #(<<9:int-size(32)>>, internal.AuthenticationSSPI),
    #(
      <<10:int-size(32), "SCRAM-SHA-256":utf8>>,
      internal.AuthenticationSASL(methods: ["SCRAM-SHA-256"]),
    ),
    #(
      <<11:int-size(32), "server_first":utf8>>,
      internal.AuthenticationSASLContinue(server_first: <<"server_first":utf8>>),
    ),
    #(
      <<12:int-size(32), "server_final":utf8>>,
      internal.AuthenticationSASLFinal(server_final: <<"server_final":utf8>>),
    ),
  ]
  |> list.try_each(fn(payload_expected) {
    let #(payload, expected) = payload_expected

    decode.message(<<"R":utf8>>, payload)
    |> result.map(fn(msg) {
      let assert True = msg == expected
    })
  })
}

pub fn decode_authentication_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected authentication payload",
  )) = decode.message(<<"R":utf8>>, <<-1:int-size(32)>>)
}

pub fn decode_authentication_sasl_methods_error_test() {
  let assert Error(internal.AuthenticationError(
    kind: internal.MethodNotImplemented,
    message: "Supported methods: [SCRAM-SHA-256]",
  )) = decode.message(<<"R":utf8>>, <<10:int-size(32), "NOPE":utf8>>)
}

pub fn decode_bind_complete_test() {
  let assert Ok(internal.BindComplete) = decode.message(<<"2":utf8>>, <<>>)
}

pub fn decode_bind_complete_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for BindComplete",
  )) = decode.message(<<"2":utf8>>, <<"unexpected":utf8>>)
}

pub fn decode_error_response_test() {
  [
    #(<<0>>, dict.new()),
    #(
      <<
        "C":utf8, "25001":utf8, 0, "M":utf8,
        "there is already a transaction in progress":utf8, 0, 0,
      >>,
      dict.from_list([
        #(internal.Code, "25001"),
        #(internal.Message, "there is already a transaction in progress"),
      ]),
    ),
  ]
  |> list.each(fn(payload_expected) {
    let #(payload, expected_fields) = payload_expected

    let assert Ok(internal.ErrorResponse(fields:)) =
      decode.message(<<"E":utf8>>, payload)

    let assert True = expected_fields == fields
  })

  decode.message(<<"E":utf8>>, <<>>)
}

pub fn decode_ready_for_query_test() {
  let assert Ok(internal.ReadyForQuery(status: internal.Idle)) =
    decode.message(<<"Z":utf8>>, <<"I":utf8>>)

  let assert Ok(internal.ReadyForQuery(status: internal.Transaction)) =
    decode.message(<<"Z":utf8>>, <<"T":utf8>>)

  let assert Ok(internal.ReadyForQuery(status: internal.Err)) =
    decode.message(<<"Z":utf8>>, <<"E":utf8>>)
}

pub fn decode_ready_for_query_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.MessageError,
    message: "Unexpected payload for ReadyForQuery",
  )) = decode.message(<<"Z":utf8>>, <<"X":utf8>>)
}

pub fn decode_no_data_test() {
  let assert Ok(internal.NoData) = decode.message(<<"n":utf8>>, <<>>)
}

pub fn decode_no_data_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for NoData",
  )) = decode.message(<<"n":utf8>>, <<"X":utf8>>)
}

pub fn decode_parse_complete_test() {
  let assert Ok(internal.ParseComplete) = decode.message(<<"1":utf8>>, <<>>)
}

pub fn decode_parse_complete_error_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for ParseComplete",
  )) = decode.message(<<"1":utf8>>, <<"X":utf8>>)
}

pub fn decode_command_complete_test() {
  [
    #(<<"SELECT 1":utf8, 0>>, 1, internal.Select(1)),
    #(<<"INSERT 0 1":utf8, 0>>, 1, internal.Insert(1)),
    #(<<"UPDATE 1":utf8, 0>>, 1, internal.Update(1)),
    #(<<"DELETE 1":utf8, 0>>, 1, internal.Delete(1)),
    #(<<"FETCH 1":utf8, 0>>, 1, internal.Fetch(1)),
    #(<<"MOVE 1":utf8, 0>>, 1, internal.Move(1)),
    #(<<"COPY 1":utf8, 0>>, 1, internal.Copy(1)),
    #(<<"BEGIN":utf8, 0>>, 0, internal.Begin),
    #(<<"COMMIT":utf8, 0>>, 0, internal.Commit),
    #(<<"ROLLBACK":utf8, 0>>, 0, internal.Rollback),
  ]
  |> list.each(fn(bits_rows_command) {
    let #(bits, expected_rows, expected_command) = bits_rows_command

    let assert Ok(internal.CommandComplete(command:, rows:)) =
      decode.message(<<"C":utf8>>, bits)

    let assert True = expected_rows == rows
    let assert True = expected_command == command
  })
}

pub fn decode_command_complete_invalid_command_test() {
  let commands = [
    #(<<"SELECT *":utf8, 0>>, "SELECT"),
    #(<<"INSERT 0 *":utf8, 0>>, "INSERT"),
    #(<<"UPDATE *":utf8, 0>>, "UPDATE"),
    #(<<"DELETE *":utf8, 0>>, "DELETE"),
    #(<<"FETCH *":utf8, 0>>, "FETCH"),
    #(<<"MOVE *":utf8, 0>>, "MOVE"),
    #(<<"COPY *":utf8, 0>>, "COPY"),
  ]

  use #(invalid_bits, name) <- list.each(commands)

  let assert Error(internal.ProtocolError(internal.DecodingError, msg)) =
    decode.message(<<"C":utf8>>, invalid_bits)

  let assert True = "Invalid " <> name <> " row count" == msg
}

pub fn decode_command_complete_invalid_payload_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.DecodingError,
    message: "Unexpected payload for CommandComplete",
  )) = decode.message(<<"C":utf8>>, <<0:int-size(1)>>)
}
// pub fn decode_error_and_mention_field_type_test() {
//   [
//     #("S", internal.Severity),
//     #("C", internal.Code),
//     #("M", internal.Message),
//     #("D", internal.Detail),
//     #("H", internal.Hint),
//     #("P", internal.Position),
//     #("p", internal.InternalPosition),
//     #("q", internal.InternalQuery),
//     #("W", internal.Where),
//     #("s", internal.Schema),
//     #("t", internal.Table),
//     #("c", internal.Column),
//     #("d", internal.DataType),
//     #("n", internal.Constraint),
//     #("F", internal.File),
//     #("L", internal.Line),
//     #("R", internal.Routine),
//     #("other", internal.Unknown(<<"other":utf8>>)),
//   ]
//   |> list.each(fn(field_type_expected) {
//     let #(field_type, expected) = field_type_expected
// 
//     let assert Ok(internal.ErrorResponse(fields:)) =
//       decode.message(<<"E":utf8>>, <<field_type:utf8>>)
// 
//     let assert Ok(field) = dict.get(fields, expected)
//     echo field
//   })
// }
