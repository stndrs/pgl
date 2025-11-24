import gleam/dynamic
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/result
import gleam/time/calendar
import gleam/time/timestamp
import pgl/internal
import pgl/internal/interval
import pgl/types

pub fn decode_timestamp_test() {
  let ts_value =
    { 1 - internal.postgres_gs_epoch + internal.gs_to_unix_epoch } * 1_000_000

  let in = <<ts_value:big-int-size(64)>>
  let out = dynamic.int(1_000_000)

  let assert Ok(ts) = types.decode(in, timestamp())

  equal(out, ts)
}

pub fn decode_timestamp_pos_infinity_test() {
  let in = <<internal.int8_max:big-int-size(64)>>
  let out = dynamic.string("infinity")

  let assert Ok(ts) = types.decode(in, timestamp())

  equal(out, ts)
}

pub fn decode_timestamp_neg_infinity_test() {
  let in = <<-internal.int8_min:big-int-size(64)>>
  let out = dynamic.string("-infinity")

  let assert Ok(ts) = types.decode(in, timestamp())

  equal(out, ts)
}

pub fn decode_oid_test() {
  use valid <- list.map([23, 1042, 0])

  let in = <<valid:big-int-size(32)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, oid())

  equal(out, result)
}

pub fn decode_bool_test() {
  use #(byte, expected) <- list.map([#(1, True), #(0, False)])

  let in = <<byte:big-int-size(8)>>
  let out = dynamic.bool(expected)

  let assert Ok(result) = types.decode(in, bool())

  equal(out, result)
}

pub fn decode_int2_test() {
  use valid <- list.map([32_767, 0, -32_768])

  let in = <<valid:big-int-size(16)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, int2())

  equal(out, result)
}

pub fn decode_int2_error_test() {
  let in = <<1:big-int-size(8)>>
  let out = "invalid int2"

  let assert Error(msg) = types.decode(in, int2())

  equal(out, msg)
}

pub fn decode_int4_test() {
  use valid <- list.map([2_147_483_647, 0, -2_147_483_648])

  let in = <<valid:big-int-size(32)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, int4())

  equal(out, result)
}

pub fn decode_int4_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid int4"

  let assert Error(msg) = types.decode(in, int4())

  equal(out, msg)
}

pub fn decode_int8_test() {
  use valid <- list.map([
    9_223_372_036_854_775_807,
    0,
    -9_223_372_036_854_775_808,
  ])

  let in = <<valid:big-int-size(64)>>
  let out = dynamic.int(valid)

  let assert Ok(result) = types.decode(in, int8())

  equal(out, result)
}

pub fn decode_int8_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid int8"

  let assert Error(msg) = types.decode(in, int8())

  equal(out, msg)
}

pub fn decode_float4_test() {
  use valid <- list.map([0.0, 3.14, -42.5])

  let in = <<valid:big-float-size(32)>>
  let out = dynamic.float(valid)

  let assert Ok(result) = types.decode(in, float4())

  equal(out, result)
}

pub fn decode_float4_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid float4"

  let assert Error(msg) = types.decode(in, float4())

  equal(out, msg)
}

pub fn decode_float8_test() {
  use valid <- list.map([0.0, 3.14159, -42.989283])

  let in = <<valid:big-float-size(64)>>
  let out = dynamic.float(valid)

  let assert Ok(result) = types.decode(in, float8())

  equal(out, result)
}

pub fn decode_float8_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid float8"

  let assert Error(msg) = types.decode(in, float8())

  equal(out, msg)
}

pub fn decode_varchar_test() {
  use valid <- list.map(["hello", "", "PostgreSQL"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, varchar())

  equal(out, result)
}

pub fn decode_varchar_error_test() {
  let in = <<255, 255, 255, 255>>
  let out = "invalid varchar"

  let assert Error(msg) = types.decode(in, varchar())

  equal(out, msg)
}

pub fn decode_text_test() {
  use valid <- list.map(["hello world", "", "PostgreSQL Database"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, text())

  equal(out, result)
}

pub fn decode_text_error_test() {
  let in = <<255, 255, 255, 255>>
  let out = "invalid text"

  let assert Error(msg) = types.decode(in, text())

  equal(out, msg)
}

pub fn decode_bytea_test() {
  use valid <- list.map([<<1, 2, 3, 4, 5>>, <<>>, <<255, 0, 128>>])

  let in = valid
  let out = dynamic.bit_array(valid)

  let assert Ok(result) = types.decode(in, bytea())

  equal(out, result)
}

pub fn decode_char_test() {
  use valid <- list.map(["A", "x"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, char())

  equal(out, result)
}

pub fn decode_name_test() {
  use valid <- list.map(["table_name", "", "column_name"])

  let in = <<valid:utf8>>
  let out = dynamic.string(valid)

  let assert Ok(result) = types.decode(in, name())

  equal(out, result)
}

pub fn decode_time_test() {
  use #(microseconds, expected) <- list.map([
    #(
      79_000_000,
      dynamic.array([
        dynamic.int(0),
        dynamic.int(1),
        dynamic.int(19),
        dynamic.int(0),
      ]),
    ),
    #(
      0,
      dynamic.array([
        dynamic.int(0),
        dynamic.int(0),
        dynamic.int(0),
        dynamic.int(0),
      ]),
    ),
    #(
      86_399_000_000,
      dynamic.array([
        dynamic.int(23),
        dynamic.int(59),
        dynamic.int(59),
        dynamic.int(0),
      ]),
    ),
  ])

  let in = <<microseconds:big-int-size(64)>>

  let assert Ok(result) = types.decode(in, time())

  equal(expected, result)
}

pub fn decode_time_error_test() {
  let in = <<1:big-int-size(32)>>
  let out = "invalid time"

  let assert Error(msg) = types.decode(in, time())

  equal(out, msg)
}

pub fn decode_date_test() {
  use #(days, expected) <- list.map([
    #(-10_957, [1970, 1, 1]),
    #(0, [2000, 1, 1]),
    #(366, [2001, 1, 1]),
  ])

  let in = <<days:big-int-size(32)>>
  let out = dynamic.array(list.map(expected, dynamic.int))

  let assert Ok(result) = types.decode(in, date())

  equal(out, result)
}

pub fn decode_date_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid date"

  let assert Error(msg) = types.decode(in, date())

  equal(out, msg)
}

pub fn array_error_test() {
  let in = <<1:big-int-size(16)>>
  let out = "invalid array"

  let assert Error(msg) = types.decode(in, array(int2()))

  equal(out, msg)
}

// Encode tests //

pub fn encode_bool_test() {
  use valid <- list.map([#(True, 1), #(False, 0)])

  let in = valid.0
  let expected = <<1:big-int-size(32), valid.1:big-int-size(8)>>

  let assert Ok(out) = types.encode(in, bool(), with: types.bool)

  equal(expected, out)
}

pub fn encode_int2_test() {
  use valid <- list.map([32_767, 0, -32_768])

  let in = valid
  let expected = <<2:big-int-size(32), valid:big-int-size(16)>>

  let assert Ok(out) = types.encode(in, int2(), with: types.int2)

  equal(expected, out)
}

pub fn encode_int2_error_test() {
  use invalid <- list.map([-100_000, 100_000, 32_768, -32_769])

  let in = invalid
  let expected = "Out of range for int2"

  let assert Error(msg) = types.encode(in, int2(), with: types.int2)

  equal(expected, msg)
}

pub fn encode_int4_test() {
  use valid <- list.map([2_147_483_647, 0, -2_147_483_648])

  let in = valid
  let expected = <<4:big-int-size(32), valid:big-int-size(32)>>

  let assert Ok(out) = types.encode(in, int4(), with: types.int4)

  equal(expected, out)
}

pub fn encode_int4_error_test() {
  use invalid <- list.map([2_147_483_648, -2_147_483_649])

  let in = invalid
  let expected = "Out of range for int4"

  let assert Error(msg) = types.encode(in, int4(), with: types.int4)

  equal(expected, msg)
}

pub fn encode_int8_test() {
  use valid <- list.map([
    9_223_372_036_854_775_807,
    0,
    -9_223_372_036_854_775_808,
  ])

  let in = valid
  let expected = <<8:big-int-size(32), valid:big-int-size(64)>>

  let assert Ok(out) = types.encode(in, int8(), with: types.int8)

  equal(expected, out)
}

pub fn encode_int8_error_test() {
  use invalid <- list.map([
    9_223_372_036_854_775_807 + 1,
    -9_223_372_036_854_775_808 - 1,
  ])

  let in = invalid
  let expected = "Out of range for int8"

  let assert Error(msg) = types.encode(in, int8(), with: types.int8)

  equal(expected, msg)
}

pub fn encode_float4_test() {
  use valid <- list.map([0.0, 1.0, -1.0, 3.14, -42.5, 1.23e38])

  let in = valid
  let expected = <<4:big-int-size(32), valid:float-size(32)>>

  let assert Ok(out) = types.encode(in, float4(), with: types.float4)

  equal(expected, out)
}

pub fn encode_float8_test() {
  use valid <- list.map([0.0, 1.0, -1.0, 3.14, -42.5, 1.23e308])

  let in = valid
  let expected = <<8:big-int-size(32), valid:float-size(64)>>

  let assert Ok(out) = types.encode(in, float8(), with: types.float8)

  equal(expected, out)
}

pub fn encode_oid_test() {
  use valid <- list.map([0, 1042, 4_294_967_295])

  let in = valid
  let expected = <<4:big-int-size(32), valid:big-int-size(32)>>

  let assert Ok(out) = types.encode(in, oid(), with: types.oid)

  equal(expected, out)
}

pub fn encode_oid_error_test() {
  use invalid <- list.map([-1, 4_294_967_296])

  let in = invalid
  let expected = "Out of range for oid"

  let assert Error(msg) = types.encode(in, oid(), with: types.oid)

  equal(expected, msg)
}

pub fn encode_varchar_test() {
  use valid <- list.map([#("hello", 5), #("", 0), #("PostgreSQL", 10)])

  let in = valid.0
  let expected = <<valid.1:big-int-size(32), valid.0:utf8>>

  let assert Ok(out) = types.encode(in, varchar(), with: types.text)

  equal(expected, out)
}

pub fn encode_date_test() {
  let assert Ok(#(in, _tod)) =
    timestamp.parse_rfc3339("1970-01-01T00:00:00Z")
    |> result.map(timestamp.to_calendar(_, calendar.utc_offset))

  let expected = <<4:big-int-size(32), -10_957:big-int-size(32)>>

  let assert Ok(out) = types.encode(in, date(), with: types.date)

  equal(expected, out)
}

pub fn encode_time_test() {
  let t = calendar.TimeOfDay(hours: 0, minutes: 1, seconds: 19, nanoseconds: 0)

  let in = t
  let expected = <<8:big-int-size(32), 79_000_000:big-int-size(64)>>

  let assert Ok(out) = types.encode(in, time(), with: types.time)

  equal(expected, out)
}

pub fn encode_timestamp_test() {
  let ts = timestamp.from_unix_seconds(1)

  let in = ts
  let expected = <<8:big-int-size(32), -946_684_799_000_000:big-int-size(64)>>

  let assert Ok(out) = types.encode(in, timestamp(), with: types.timestamp)

  equal(expected, out)
}

pub fn encode_interval_test() {
  let usecs =
    interval.days(14)
    |> interval.add(interval.microseconds(79_000))

  let microseconds = interval.to_microseconds(usecs)

  let in = usecs |> interval.to_duration
  let expected = <<
    16:big-int-size(32),
    microseconds:big-int-size(64),
    0:big-int-size(32),
    0:big-int-size(32),
  >>

  let assert Ok(out) = types.encode(in, interval(), with: types.interval)

  equal(expected, out)
}

pub fn encode_timestampz_test() {
  let expected_utc_int = -946_684_799_000_000
  let ts = timestamp.from_unix_seconds(1)

  let offset = interval.hours(0)
  let expected = <<
    8:big-int-size(32),
    expected_utc_int:big-int-size(64),
  >>

  let assert Ok(out) =
    types.encode(#(ts, offset), timestampz(), with: types.timestampz)

  equal(expected, out)
}

pub fn encode_positive_offset_timestampz_test() {
  let expected_utc_int = -946_684_800_000_000
  let ts = timestamp.from_unix_seconds(1)

  let offset = interval.hours(10)
  let ten_hours =
    offset
    |> interval.add_to(ts)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)
    |> int.add(expected_utc_int)

  let expected = <<
    8:big-int-size(32),
    ten_hours:big-int-size(64),
  >>

  let assert Ok(out) =
    types.encode(#(ts, offset), timestampz(), with: types.timestampz)

  equal(expected, out)
}

pub fn encode_negative_offset_timestampz_test() {
  let expected_utc_int = -946_684_800_000_000
  let ts = timestamp.from_unix_seconds(1)

  let offset =
    interval.hours(-2)
    |> interval.add(interval.minutes(30))

  let minus_two_thirty =
    offset
    |> interval.add_to(ts)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)
    |> int.add(expected_utc_int)

  let expected = <<
    8:big-int-size(32),
    minus_two_thirty:big-int-size(64),
  >>

  let assert Ok(out) =
    types.encode(#(ts, offset), timestampz(), with: types.timestampz)

  equal(expected, out)
}

pub fn empty_array_test() {
  let expected = <<
    12:big-int-size(32), 0:big-int-size(32), 0:big-int-size(32),
    25:big-int-size(32),
  >>

  let assert Ok(out) =
    types.encode([], array(int2()), with: fn(val, info) {
      types.array(val, info, of: types.int2)
    })

  equal(expected, out)
}

pub fn string_array_test() {
  let in = ["hello", "world"]

  let expected = <<
    38:big-int-size(32), 1:big-int-size(32), 0:big-int-size(32),
    25:big-int-size(32), 2:big-int-size(32), 1:big-int-size(32),
    5:big-int-size(32), "hello":utf8, 5:big-int-size(32), "world":utf8,
  >>

  let assert Ok(out) =
    types.encode(in, array(text()), with: fn(val, info) {
      types.array(val, info, of: types.text)
    })

  equal(expected, out)
}

pub fn int_array_test() {
  let in = [42]
  let expected = <<
    28:big-int-size(32), 1:big-int-size(32), 0:big-int-size(32),
    25:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    4:big-int-size(32), 42:big-int-size(32),
  >>

  let assert Ok(out) =
    types.encode(in, array(int4()), with: fn(val, info) {
      types.array(val, info, of: types.int4)
    })

  equal(expected, out)
}

pub fn null_array_test() {
  let in = [Nil]

  let expected = <<
    24:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    25:big-int-size(32), 1:big-int-size(32), 1:big-int-size(32),
    -1:big-int-size(32),
  >>

  let assert Ok(out) =
    types.encode(in, array(int4()), with: fn(val, info) {
      types.array(val, info, of: types.null)
    })

  equal(expected, out)
}

// pub fn nested_array_test() {
//   let in = [[12, 23]]
// 
//   let expected = <<>>
// 
//   let assert Ok(out) =
//     types.encode(in, array(array(int4())), with: fn(val, info) {
//       types.array(val, info, of: fn(val1, elem_info) {
//         types.array(val1, elem_info, of: types.int4)
//       })
//     })
// 
//   equal(expected, out)
// }

// TypeInfo helpers

fn oid() {
  types.info(25)
  |> types.set_typesend("oidsend")
  |> types.set_typereceive("oidrecv")
}

fn bool() {
  types.info(25)
  |> types.set_typesend("boolsend")
  |> types.set_typereceive("boolrecv")
}

fn int2() {
  types.info(25)
  |> types.set_typesend("int2send")
  |> types.set_typereceive("int2recv")
}

fn int4() {
  types.info(25)
  |> types.set_typesend("int4send")
  |> types.set_typereceive("int4recv")
}

fn int8() {
  types.info(25)
  |> types.set_typesend("int8send")
  |> types.set_typereceive("int8recv")
}

fn float4() {
  types.info(25)
  |> types.set_typesend("float4send")
  |> types.set_typereceive("float4recv")
}

fn float8() {
  types.info(25)
  |> types.set_typesend("float8send")
  |> types.set_typereceive("float8recv")
}

fn varchar() {
  types.info(25)
  |> types.set_typesend("varcharsend")
  |> types.set_typereceive("varcharrecv")
}

fn text() {
  types.info(25)
  |> types.set_typesend("textsend")
  |> types.set_typereceive("textrecv")
}

fn bytea() {
  types.info(25)
  |> types.set_typesend("byteasend")
  |> types.set_typereceive("bytearecv")
}

fn char() {
  types.info(25)
  |> types.set_typesend("charsend")
  |> types.set_typereceive("charrecv")
}

fn name() {
  types.info(25)
  |> types.set_typesend("namesend")
  |> types.set_typereceive("namerecv")
}

fn time() {
  types.info(25)
  |> types.set_typesend("time_send")
  |> types.set_typereceive("time_recv")
}

fn date() {
  types.info(25)
  |> types.set_typesend("date_send")
  |> types.set_typereceive("date_recv")
}

fn timestamp() {
  types.info(25)
  |> types.set_typesend("timestamp_send")
  |> types.set_typereceive("timestamp_recv")
}

fn timestampz() {
  types.info(25)
  |> types.set_typesend("timestampz_send")
  |> types.set_typereceive("timestampz_recv")
}

fn interval() {
  types.info(25)
  |> types.set_typesend("interval_send")
  |> types.set_typereceive("interval_recv")
}

fn array(ti: types.TypeInfo) -> types.TypeInfo {
  types.info(25)
  |> types.set_typesend("array_send")
  |> types.set_typereceive("array_recv")
  |> types.set_elem_type(Some(ti))
}

// test helper

pub fn equal(left: a, right: a) -> a {
  assert left == right

  right
}
