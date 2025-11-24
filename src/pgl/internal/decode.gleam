import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import pgl/internal

pub fn message(
  code: BitArray,
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
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
    _ ->
      Error(error(
        "message type" <> bit_array.to_string(code) |> result.unwrap("unknown"),
      ))
  }
}

pub fn error(message: String) -> internal.PglError {
  internal.ProtocolError(kind: internal.DecodingError, message:)
}

pub fn close_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<>> -> Ok(internal.CloseComplete)
    _bits -> Error(error("CloseComplete"))
  }
}

pub fn empty_query_response(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<>> -> Ok(internal.EmptyQueryResponse)
    _bits -> Error(error("EmptyQueryResponse"))
  }
}

pub fn copy_done(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<>> -> Ok(internal.CopyDone)
    _bits -> Error(error("CopyDone"))
  }
}

pub fn portal_suspended(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<>> -> Ok(internal.PortalSuspended)
    _bits -> Error(error("PortalSuspended"))
  }
}

pub fn copy_data(data: BitArray) -> Result(internal.Message, internal.PglError) {
  Ok(internal.CopyData(data:))
}

pub fn data_row(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<columns:int-size(16), rest:bits>> -> {
      data_row_values(rest, columns, [])
      |> result.map(internal.DataRow)
    }
    _ -> Error(error("DataRow"))
  }
}

fn data_row_values(
  payload: BitArray,
  columns: Int,
  acc: List(BitArray),
) -> Result(List(BitArray), internal.PglError) {
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
        _ -> Error(error("Invalid data row"))
      }
    }
  }
}

pub fn backend_key_data(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<proc_id:int-size(32), secret:int-size(32)>> ->
      Ok(internal.BackendKeyData(proc_id:, secret:))
    _ -> Error(error("BackendKeyData"))
  }
}

pub fn parameter_status(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  decode_string(payload)
  |> result.try(fn(decoded) {
    let #(name, rest) = decoded

    decode_string(rest)
    |> result.try(fn(decoded1) {
      case decoded1 {
        #(value, <<>>) -> Ok(internal.ParameterStatus(name:, value:))
        _ -> Error(error("ParameterStatus"))
      }
    })
  })
}

pub fn authentication(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
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
    _ -> Error(error("authentication"))
  }
}

fn sasl_methods(
  methods_bin: BitArray,
) -> Result(List(String), internal.PglError) {
  case bit_array.byte_size(methods_bin) {
    0 -> Ok([])
    _ -> sasl_methods_inner(methods_bin)
  }
}

fn sasl_methods_inner(
  binary: BitArray,
) -> Result(List(String), internal.PglError) {
  case binary {
    <<"SCRAM-SHA-256":utf8, _rest:bits>> -> Ok(["SCRAM-SHA-256"])
    _ ->
      Error(internal.AuthenticationError(
        kind: internal.UnsupportedSASLMethod(method: binary),
        message: "",
      ))
  }
}

pub fn bind_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<>> -> Ok(internal.BindComplete)
    _ -> Error(error("BindComplete"))
  }
}

pub fn error_response(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  error_and_notice_message_fields(payload, dict.new())
  |> result.map(internal.ErrorResponse)
}

pub fn notice_response(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  error_and_notice_message_fields(payload, dict.new())
  |> result.map(internal.NoticeResponse)
}

pub fn command_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case decode_string(payload) {
    Ok(#(command_tag, <<>>)) -> {
      tag(bit_array.from_string(command_tag))
      |> result.map(fn(command) {
        let rows = num_rows_from_command(command)

        internal.CommandComplete(command:, rows:)
      })
    }
    _ -> Error(error("CommandComplete"))
  }
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

fn tag(bits: BitArray) -> Result(internal.Command, internal.PglError) {
  case bits {
    <<"SELECT ":utf8, num:bits>> -> {
      bit_array.to_string(num)
      |> result.try(int.parse)
      |> result.unwrap(0)
      |> internal.Select
      |> Ok
    }
    <<"INSERT ":utf8, rest:bits>> -> {
      case binary_split(rest, <<" ">>) {
        [_oid, <<num_rows:bits>>] -> {
          let num_rows = bits_to_int(num_rows)
          Ok(internal.Insert(num_rows))
        }
        _ -> Error(error("Invalid INSERT tag"))
      }
    }
    <<"UPDATE ", num:int>> -> Ok(internal.Update(num))
    <<"DELETE ", num:int>> -> Ok(internal.Delete(num))
    <<"FETCH ", num:int>> -> Ok(internal.Fetch(num))
    <<"MOVE ", num:int>> -> Ok(internal.Move(num))
    <<"COPY ", num:int>> -> Ok(internal.Copy(num))
    <<"BEGIN">> -> Ok(internal.Begin)
    <<"COMMIT">> -> Ok(internal.Commit)
    <<"ROLLBACK">> -> Ok(internal.Rollback)
    other -> {
      bit_array.to_string(other)
      |> result.unwrap("")
      |> internal.Other
      |> Ok
    }
  }
}

@external(erlang, "erlang", "binary_to_integer")
fn bits_to_int(data: BitArray) -> Int

pub fn ready_for_query(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<"I":utf8>> -> Ok(internal.ReadyForQuery(status: internal.Idle))
    <<"T":utf8>> -> Ok(internal.ReadyForQuery(status: internal.Transaction))
    <<"E":utf8>> -> Ok(internal.ReadyForQuery(status: internal.Err))
    _ -> Error(error("ReadyForQuery"))
  }
}

fn error_and_notice_message_fields(
  payload: BitArray,
  acc: Dict(internal.Field, String),
) -> Result(Dict(internal.Field, String), internal.PglError) {
  case payload {
    <<0>> -> Ok(acc)
    <<field_type:bits-size(8), rest:bits>> -> {
      case decode_string(rest) {
        Ok(#(field_string, rest1)) -> {
          let field = error_and_mention_field_type(field_type)
          let acc1 = dict.insert(acc, field, field_string)

          error_and_notice_message_fields(rest1, acc1)
        }
        Error(err) -> Error(err)
      }
    }
    _ -> Error(internal.message_error("Unexpected message format"))
  }
}

fn decode_string(
  bits: BitArray,
) -> Result(#(String, BitArray), internal.PglError) {
  case binary_match(bits, <<0>>) {
    Some(#(start, _length)) -> {
      case split_binary(bits, start) {
        #(<<str:bits>>, <<0, rest:bits>>) -> {
          bit_array.to_string(str)
          |> result.map(fn(str1) { #(str1, rest) })
          |> result.replace_error(error("Faild to parse string"))
        }
        _ -> Error(error("Failed to parse binary"))
      }
    }
    None -> Error(error("Not null terminated"))
  }
}

@external(erlang, "pgl_ffi", "binary_match")
fn binary_match(bits: BitArray, pattern: BitArray) -> Option(#(Int, Int))

@external(erlang, "erlang", "split_binary")
fn split_binary(bits: BitArray, position: Int) -> #(BitArray, BitArray)

@external(erlang, "binary", "split")
fn binary_split(input: BitArray, pattern: BitArray) -> List(BitArray)

pub fn no_data(payload: BitArray) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<>> -> Ok(internal.NoData)
    _ -> Error(error("NoData"))
  }
}

pub fn row_description(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<count:int-size(16), rest:bits>> -> {
      case row_description_fields(count, rest, []) {
        Ok(fields) -> Ok(internal.RowDescription(count:, fields:))
        Error(err) -> Error(err)
      }
    }
    _ -> Error(error("row description"))
  }
}

fn row_description_fields(
  count: Int,
  binary: BitArray,
  acc: List(internal.RowDescriptionField),
) -> Result(List(internal.RowDescriptionField), internal.PglError) {
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
          _ -> Error(error("Row description fields"))
        }
      })
    }
  }
}

fn decode_format_code(
  code: Int,
) -> Result(internal.PgSqlFormat, internal.PglError) {
  case code {
    0 -> Ok(internal.Text)
    1 -> Ok(internal.Binary)
    _ -> Error(error("format code"))
  }
}

pub fn parse_complete(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<>> -> Ok(internal.ParseComplete)
    _ -> Error(error("ParseComplete"))
  }
}

pub fn parameter_description(
  payload: BitArray,
) -> Result(internal.Message, internal.PglError) {
  case payload {
    <<count:int-size(16), rest:bits>> -> {
      let data_types = parameter_data_types(rest, [])
      case count == list.length(data_types) {
        True -> Ok(internal.ParameterDescription(count:, data_types:))
        _ -> Error(error("ParameterDescription"))
      }
    }
    _ -> Error(error("Parameter description"))
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

pub fn error_and_mention_field_type(field_type: BitArray) -> internal.Field {
  case field_type {
    <<"S":utf8>> -> internal.Severity
    <<"C":utf8>> -> internal.Code
    <<"M":utf8>> -> internal.Message
    <<"D":utf8>> -> internal.Detail
    <<"H":utf8>> -> internal.Hint
    <<"P":utf8>> -> internal.Position
    <<"p":utf8>> -> internal.InternalPosition
    <<"q":utf8>> -> internal.InternalQuery
    <<"W":utf8>> -> internal.Where
    <<"s":utf8>> -> internal.Schema
    <<"t":utf8>> -> internal.Table
    <<"c":utf8>> -> internal.Column
    <<"d":utf8>> -> internal.DataType
    <<"n":utf8>> -> internal.Constraint
    <<"F":utf8>> -> internal.File
    <<"L":utf8>> -> internal.Line
    <<"R":utf8>> -> internal.Routine
    other -> internal.Unknown(other)
  }
}
