import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import pgl/internal

pub fn message(
  code: BitArray,
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case code {
    <<"1":utf8>> -> parse_complete(payload)
    <<"2":utf8>> -> bind_complete(payload)
    <<"3":utf8>> -> close_complete(payload)
    <<"C":utf8>> -> command_complete(payload)
    <<"D":utf8>> -> data_row(payload)
    <<"E":utf8>> -> error_response(payload)
    <<"I":utf8>> -> empty_query_response(payload)
    <<"K":utf8>> -> backend_key_data(payload)
    <<"N":utf8>> -> notice_response(payload)
    <<"R":utf8>> -> authentication(payload)
    <<"S":utf8>> -> parameter_status(payload)
    <<"T":utf8>> -> row_description(payload)
    <<"Z":utf8>> -> ready_for_query(payload)
    <<"c":utf8>> -> copy_done(payload)
    <<"d":utf8>> -> copy_data(payload)
    <<"n":utf8>> -> no_data(payload)
    <<"s":utf8>> -> portal_suspended(payload)
    <<"t":utf8>> -> parameter_description(payload)
    _ -> {
      let message =
        "message type " <> bit_array.to_string(code) |> result.unwrap("unknown")

      internal.DecodingError
      |> internal.ProtocolError(message:)
      |> Error
    }
  }
}

fn close_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<>> -> Ok(internal.CloseComplete)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected payload for CloseComplete")
      |> Error
    }
  }
}

fn empty_query_response(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<>> -> Ok(internal.EmptyQueryResponse)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(
        message: "Unexpected payload for EmptyQueryResponse",
      )
      |> Error
    }
  }
}

fn copy_done(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<>> -> Ok(internal.CopyDone)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected payload for CopyDone")
      |> Error
    }
  }
}

fn portal_suspended(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<>> -> Ok(internal.PortalSuspended)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(
        message: "Unexpected payload for PortalSuspended",
      )
      |> Error
    }
  }
}

fn copy_data(data: BitArray) -> Result(internal.Message, internal.InternalError) {
  Ok(internal.CopyData(data:))
}

fn data_row(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<columns:int-size(16), rest:bits>> -> {
      data_row_values(rest, columns, [])
      |> result.map(internal.DataRow)
    }
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected payload for DataRow")
      |> Error
    }
  }
}

fn data_row_values(
  payload: BitArray,
  columns: Int,
  acc: List(BitArray),
) -> Result(List(BitArray), internal.InternalError) {
  case columns > 0 {
    False -> Ok(list.reverse(acc))
    True -> {
      case payload {
        <<>> -> Ok(acc)
        <<-1:signed-int-size(32), rest:bits>> -> {
          data_row_values(rest, columns - 1, [<<>>, ..acc])
        }
        <<value_len:int-size(32), value:bytes-size(value_len), rest:bits>> -> {
          data_row_values(rest, columns - 1, [value, ..acc])
        }
        _bits -> {
          internal.DecodingError
          |> internal.ProtocolError(message: "Invalid data row")
          |> Error
        }
      }
    }
  }
}

fn backend_key_data(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<proc_id:int-size(32), secret:int-size(32)>> ->
      Ok(internal.BackendKeyData(proc_id:, secret:))
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(
        message: "Unexpected payload for BackendKeyData",
      )
      |> Error
    }
  }
}

fn parameter_status(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  payload
  |> bit_array.to_string
  |> result.map_error(fn(_) {
    internal.DecodingError
    |> internal.ProtocolError(message: "Unexpected payload for ParameterStatus")
  })
  |> result.try(fn(str) {
    case string.split(str, on: "\u{0000}") {
      [name, value, _] -> Ok(internal.ParameterStatus(name:, value:))
      _bits -> {
        internal.DecodingError
        |> internal.ProtocolError(
          message: "Unexpected payload for ParameterStatus",
        )
        |> Error
      }
    }
  })
}

fn authentication(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<0:int-size(32)>> -> Ok(internal.AuthenticationOk)
    <<2:int-size(32)>> -> Ok(internal.AuthenticationKerberosV5)
    <<3:int-size(32)>> -> Ok(internal.AuthenticationCleartextPassword)
    <<5:int-size(32), salt:bits-size(32)>> ->
      Ok(internal.AuthenticationMD5Password(salt:))
    <<6:int-size(32)>> -> Ok(internal.AuthenticationSCM)
    <<7:int-size(32)>> -> Ok(internal.AuthenticationGSS)
    <<8:int-size(32), data:bits>> ->
      Ok(internal.AuthenticationGSSContinue(data:))
    <<9:int-size(32)>> -> Ok(internal.AuthenticationSSPI)
    <<10:int-size(32), methods_bin:bits>> -> {
      sasl_methods(methods_bin)
      |> result.map(fn(methods) { internal.AuthenticationSASL(methods:) })
    }
    <<11:int-size(32), server_first:bits>> ->
      Ok(internal.AuthenticationSASLContinue(server_first:))
    <<12:int-size(32), server_final:bits>> ->
      Ok(internal.AuthenticationSASLFinal(server_final:))
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected authentication payload")
      |> Error
    }
  }
}

fn sasl_methods(
  methods_bin: BitArray,
) -> Result(List(String), internal.InternalError) {
  case bit_array.byte_size(methods_bin) {
    0 -> Ok([])
    _ -> sasl_methods_inner(methods_bin)
  }
}

fn sasl_methods_inner(
  binary: BitArray,
) -> Result(List(String), internal.InternalError) {
  case binary {
    <<"SCRAM-SHA-256":utf8, _rest:bits>> -> Ok(["SCRAM-SHA-256"])
    _ ->
      Error(internal.AuthenticationError(
        kind: internal.MethodNotImplemented,
        message: "Supported methods: [SCRAM-SHA-256]",
      ))
  }
}

fn bind_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<>> -> Ok(internal.BindComplete)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected payload for BindComplete")
      |> Error
    }
  }
}

fn error_response(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  error_and_notice_message_fields(payload, dict.new())
  |> result.map(internal.ErrorResponse)
}

fn notice_response(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  error_and_notice_message_fields(payload, dict.new())
  |> result.map(internal.NoticeResponse)
}

fn command_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  let len = bit_array.byte_size(payload) - 1

  bit_array.slice(payload, at: 0, take: len)
  |> result.try(bit_array.to_string)
  |> result.map_error(fn(_) {
    internal.DecodingError
    |> internal.ProtocolError(message: "Unexpected payload for CommandComplete")
  })
  |> result.try(to_tag)
  |> result.map(fn(command) {
    let rows = num_rows_from_command(command)

    internal.CommandComplete(command:, rows:)
  })
}

fn num_rows_from_command(cmd: internal.Command) -> Int {
  case cmd {
    internal.Select(num) -> num
    internal.Insert(num) -> num
    internal.Update(num) -> num
    internal.Delete(num) -> num
    internal.Fetch(num) -> num
    internal.Move(num) -> num
    internal.Copy(num) -> num
    internal.Begin -> 0
    internal.Commit -> 0
    internal.Rollback -> 0
    internal.Other(_) -> 0
  }
}

fn to_tag(value: String) -> Result(internal.Command, internal.InternalError) {
  case value {
    "SELECT " <> num -> parse_num_rows("SELECT", num, into: internal.Select)
    // https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
    //
    // > For an INSERT command, the tag is INSERT oid rows, where rows is the
    // > number of rows inserted. oid used to be the object ID of the
    // > inserted row if rows was 1 and the target table had OIDs, but OIDs
    // > system columns are not supported anymore; therefore oid is always 0.
    "INSERT 0 " <> num -> parse_num_rows("INSERT", num, into: internal.Insert)
    "UPDATE " <> num -> parse_num_rows("UPDATE", num, into: internal.Update)
    "DELETE " <> num -> parse_num_rows("DELETE", num, into: internal.Delete)
    "FETCH " <> num -> parse_num_rows("FETCH", num, into: internal.Fetch)
    "MOVE " <> num -> parse_num_rows("MOVE", num, into: internal.Move)
    "COPY " <> num -> parse_num_rows("COPY", num, into: internal.Copy)
    "BEGIN" -> Ok(internal.Begin)
    "COMMIT" -> Ok(internal.Commit)
    "ROLLBACK" -> Ok(internal.Rollback)
    other -> Ok(internal.Other(other))
  }
}

fn parse_num_rows(
  name: String,
  num: String,
  into command: fn(Int) -> internal.Command,
) -> Result(internal.Command, internal.InternalError) {
  int.parse(num)
  |> result.map_error(fn(_) {
    internal.DecodingError
    |> internal.ProtocolError(message: "Invalid " <> name <> " row count")
  })
  |> result.map(command)
}

fn ready_for_query(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<"I":utf8>> -> Ok(internal.ReadyForQuery(status: internal.Idle))
    <<"T":utf8>> -> Ok(internal.ReadyForQuery(status: internal.Transaction))
    <<"E":utf8>> -> Ok(internal.ReadyForQuery(status: internal.Err))
    _bits -> {
      internal.MessageError
      |> internal.ProtocolError(message: "Unexpected payload for ReadyForQuery")
      |> Error
    }
  }
}

fn error_and_notice_message_fields(
  payload: BitArray,
  acc: Dict(BitArray, String),
) -> Result(Dict(BitArray, String), internal.InternalError) {
  case payload {
    <<0>> -> Ok(acc)
    <<field:bits-size(8), rest:bits>> -> {
      case decode_string(rest) {
        Ok(#(field_string, rest1)) -> {
          let acc1 = dict.insert(acc, field, field_string)

          error_and_notice_message_fields(rest1, acc1)
        }
        Error(err) -> Error(err)
      }
    }
    _ -> {
      internal.MessageError
      |> internal.ProtocolError("Unexpected message format")
      |> Error
    }
  }
}

fn decode_string(
  bits: BitArray,
) -> Result(#(String, BitArray), internal.InternalError) {
  case binary_match(bits, <<0>>) {
    Some(#(start, _length)) -> {
      case split_binary(bits, start) {
        #(<<str:bits>>, <<0, rest:bits>>) -> {
          bit_array.to_string(str)
          |> result.map(fn(str1) { #(str1, rest) })
          |> result.map_error(fn(_) {
            internal.DecodingError
            |> internal.ProtocolError(message: "Failed to parse binary")
          })
        }
        _bits -> {
          internal.DecodingError
          |> internal.ProtocolError(message: "Failed to parse binary")
          |> Error
        }
      }
    }
    None -> {
      echo bits
      internal.DecodingError
      |> internal.ProtocolError(message: "Failed to decode string")
      |> Error
    }
  }
}

@external(erlang, "pgl_ffi", "binary_match")
fn binary_match(bits: BitArray, pattern: BitArray) -> Option(#(Int, Int))

@external(erlang, "erlang", "split_binary")
fn split_binary(bits: BitArray, position: Int) -> #(BitArray, BitArray)

fn no_data(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<>> -> Ok(internal.NoData)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected payload for NoData")
      |> Error
    }
  }
}

fn row_description(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<count:int-size(16), rest:bits>> -> {
      case row_description_fields(count, rest, []) {
        Ok(fields) -> Ok(internal.RowDescription(count:, fields:))
        Error(err) -> Error(err)
      }
    }
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(
        message: "Unexpected payload for RowDescription",
      )
      |> Error
    }
  }
}

fn row_description_fields(
  count: Int,
  binary: BitArray,
  acc: List(internal.RowDescriptionField),
) -> Result(List(internal.RowDescriptionField), internal.InternalError) {
  case count, binary {
    0, <<>> -> Ok(list.reverse(acc))
    count, binary -> {
      decode_string(binary)
      |> result.try(fn(decoded) {
        let name = decoded.0

        case decoded.1 {
          <<"?column?":utf8>> -> row_description_fields(count - 1, <<>>, [])
          <<
            table_oid:int-size(32),
            attr_number:int-size(16),
            data_type_oid:int-size(32),
            data_type_size:int-size(16),
            type_modifier:int-size(32),
            format_code:int-size(16),
            tail:bits,
          >> -> {
            decode_format_code(format_code)
            |> result.try(fn(format) {
              let field =
                internal.RowDescriptionField(
                  name:,
                  table_oid:,
                  attr_number:,
                  data_type_oid:,
                  data_type_size:,
                  type_modifier:,
                  format:,
                )
              row_description_fields(count - 1, tail, [field, ..acc])
            })
          }
          _bits -> {
            internal.DecodingError
            |> internal.ProtocolError(
              message: "Unexpected payload for RowDescriptionField",
            )
            |> Error
          }
        }
      })
    }
  }
}

fn decode_format_code(
  code: Int,
) -> Result(internal.PgSqlFormat, internal.InternalError) {
  case code {
    0 -> Ok(internal.Text)
    1 -> Ok(internal.Binary)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected format code")
      |> Error
    }
  }
}

fn parse_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<>> -> Ok(internal.ParseComplete)
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected payload for ParseComplete")
      |> Error
    }
  }
}

fn parameter_description(
  payload: BitArray,
) -> Result(internal.Message, internal.InternalError) {
  case payload {
    <<count:int-size(16), rest:bits>> -> {
      let data_types = parameter_data_types(rest, [])
      case count == list.length(data_types) {
        True -> Ok(internal.ParameterDescription(count:, data_types:))
        _bits -> {
          internal.DecodingError
          |> internal.ProtocolError(message: "ParameterDescription")
          |> Error
        }
      }
    }
    _bits -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "ParameterDescription")
      |> Error
    }
  }
}

fn parameter_data_types(payload: BitArray, acc: List(Int)) -> List(Int) {
  case payload {
    <<>> -> list.reverse(acc)
    <<oid:int-size(32), rest:bits>> -> {
      parameter_data_types(rest, [oid, ..acc])
    }
    _ -> acc
  }
}
