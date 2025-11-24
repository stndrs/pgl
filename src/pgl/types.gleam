import gleam/bit_array
import gleam/dynamic.{type Dynamic}
import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/calendar
import gleam/time/duration
import gleam/time/timestamp
import pgl/internal
import pgl/internal/interval.{type Interval}

pub type TypeInfo {
  TypeInfo(
    oid: Int,
    name: String,
    typesend: String,
    typereceive: String,
    typelen: Int,
    output: String,
    input: String,
    elem_oid: Int,
    elem_type: Option(TypeInfo),
    base_oid: Int,
    comp_oids: List(Int),
    comp_types: Option(List(TypeInfo)),
  )
}

pub fn info(oid: Int) -> TypeInfo {
  TypeInfo(
    oid:,
    name: "",
    typesend: "",
    typereceive: "",
    typelen: 0,
    output: "",
    input: "",
    elem_oid: 0,
    elem_type: None,
    base_oid: 0,
    comp_oids: [],
    comp_types: None,
  )
}

pub fn set_name(ti: TypeInfo, name: String) -> TypeInfo {
  TypeInfo(..ti, name:)
}

pub fn set_typesend(ti: TypeInfo, typesend: String) -> TypeInfo {
  TypeInfo(..ti, typesend:)
}

pub fn set_typereceive(ti: TypeInfo, typereceive: String) -> TypeInfo {
  TypeInfo(..ti, typereceive:)
}

pub fn set_typelen(ti: TypeInfo, typelen: Int) -> TypeInfo {
  TypeInfo(..ti, typelen:)
}

pub fn set_output(ti: TypeInfo, output: String) -> TypeInfo {
  TypeInfo(..ti, output:)
}

pub fn set_input(ti: TypeInfo, input: String) -> TypeInfo {
  TypeInfo(..ti, input:)
}

pub fn set_elem_oid(ti: TypeInfo, elem_oid: Int) -> TypeInfo {
  TypeInfo(..ti, elem_oid:)
}

pub fn set_base_oid(ti: TypeInfo, base_oid: Int) -> TypeInfo {
  TypeInfo(..ti, base_oid:)
}

pub fn set_comp_oids(ti: TypeInfo, comp_oids: List(Int)) -> TypeInfo {
  TypeInfo(..ti, comp_oids:)
}

pub fn set_elem_type(ti: TypeInfo, elem_type: Option(TypeInfo)) -> TypeInfo {
  TypeInfo(..ti, elem_type:)
}

pub fn set_comp_types(
  ti: TypeInfo,
  comp_types: Option(List(TypeInfo)),
) -> TypeInfo {
  TypeInfo(..ti, comp_types:)
}

// ---------- Encoding ---------- //

pub fn encode(
  val: a,
  ti: TypeInfo,
  with encoder: fn(a, TypeInfo) -> Result(BitArray, String),
) -> Result(BitArray, String) {
  case ti.typesend {
    "array_send" -> encoder(val, ti)
    "boolsend" -> encoder(val, ti)
    "oidsend" -> encoder(val, ti)
    "int2send" -> encoder(val, ti)
    "int4send" -> encoder(val, ti)
    "int8send" -> encoder(val, ti)
    "float4send" -> encoder(val, ti)
    "float8send" -> encoder(val, ti)
    "textsend" -> encoder(val, ti)
    "varcharsend" -> encoder(val, ti)
    "namesend" -> encoder(val, ti)
    "charsend" -> encoder(val, ti)
    "byteasend" -> encoder(val, ti)
    "time_send" -> encoder(val, ti)
    "date_send" -> encoder(val, ti)
    "timestamp_send" -> encoder(val, ti)
    "timestampz_send" -> encoder(val, ti)
    "interval_send" -> encoder(val, ti)
    _ -> Error("Unsupported type")
  }
}

pub fn array(
  val: List(a),
  ti: TypeInfo,
  of inner: fn(a, TypeInfo) -> Result(BitArray, String),
) -> Result(BitArray, String) {
  let dimensions = case val {
    [] -> []
    _ -> [list.length(val)]
  }

  case ti.elem_type {
    Some(elem_ti) -> {
      use encoded_elems <- result.try(list.try_map(val, inner(_, elem_ti)))

      let has_nulls = list.contains(encoded_elems, <<-1:big-int-size(32)>>)

      encode_array(dimensions, has_nulls, elem_ti, val, inner)
    }
    None -> Error("Missing elem type info")
  }
}

fn encode_array(
  dimensions: List(Int),
  has_nulls: Bool,
  ti: TypeInfo,
  vals: List(a),
  with encoder: fn(a, TypeInfo) -> Result(BitArray, String),
) -> Result(BitArray, String) {
  let header = array_header(dimensions, has_nulls, ti.oid)

  case vals {
    [] -> raw(header, ti)
    _ -> {
      vals
      |> list.try_map(encoder(_, ti))
      |> result.map(fn(elems) { bit_array.concat([header, ..elems]) })
      |> result.try(raw(_, ti))
    }
  }
}

fn array_header(
  dimensions: List(Int),
  has_nulls: Bool,
  elem_type_oid: Int,
) -> BitArray {
  let num_dims = list.length(dimensions)

  let flags = case has_nulls {
    True -> 1
    False -> 0
  }

  let encoded_dimensions =
    list.map(dimensions, fn(dimension) {
      <<dimension:big-int-size(32), 1:big-int-size(32)>>
    })
    |> bit_array.concat

  [
    <<num_dims:int-size(32), flags:int-size(32), elem_type_oid:int-size(32)>>,
    encoded_dimensions,
  ]
  |> bit_array.concat
}

pub fn null(_: a, _ti: TypeInfo) -> Result(BitArray, String) {
  Ok(<<-1:big-int-size(32)>>)
}

pub fn bool(bool: Bool, _ti: TypeInfo) -> Result(BitArray, String) {
  case bool {
    True -> Ok(<<1:big-int-size(32), 1:big-int-size(8)>>)
    False -> Ok(<<1:big-int-size(32), 0:big-int-size(8)>>)
  }
}

pub fn oid(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case 0 <= num && num <= internal.oid_max {
    True -> Ok(<<4:big-int-size(32), num:big-int-size(32)>>)
    False -> Error("Out of range for oid")
  }
}

pub fn int(num: Int, ti: TypeInfo) -> Result(BitArray, String) {
  case ti.typesend {
    "oidsend" -> oid(num, ti)
    "int2send" -> int2(num, ti)
    "int4send" -> int4(num, ti)
    "int8send" -> int8(num, ti)
    _ -> Error("Unsupported int type")
  }
}

pub fn int2(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case -internal.int2_min <= num && num <= internal.int2_max {
    True -> Ok(<<2:big-int-size(32), num:big-int-size(16)>>)
    False -> Error("Out of range for int2")
  }
}

pub fn int4(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case -internal.int4_min <= num && num <= internal.int4_max {
    True -> Ok(<<4:big-int-size(32), num:big-int-size(32)>>)
    False -> Error("Out of range for int4")
  }
}

pub fn int8(num: Int, _ti: TypeInfo) -> Result(BitArray, String) {
  case -internal.int8_min <= num && num <= internal.int8_max {
    True -> Ok(<<8:big-int-size(32), num:big-int-size(64)>>)
    False -> Error("Out of range for int8")
  }
}

pub fn float(num: Float, ti: TypeInfo) -> Result(BitArray, String) {
  case ti.typesend {
    "float4send" -> float4(num, ti)
    "float8send" -> float8(num, ti)
    _ -> Error("Unsupported int type")
  }
}

pub fn float4(num: Float, _ti: TypeInfo) -> Result(BitArray, String) {
  Ok(<<4:big-int-size(32), num:big-float-size(32)>>)
}

pub fn float8(num: Float, _ti: TypeInfo) -> Result(BitArray, String) {
  Ok(<<8:big-int-size(32), num:big-float-size(64)>>)
}

pub fn text(text: String, ti: TypeInfo) -> Result(BitArray, String) {
  bit_array.from_string(text) |> raw(ti)
}

pub fn raw(bits: BitArray, _ti: TypeInfo) -> Result(BitArray, String) {
  let len = bit_array.byte_size(bits)

  Ok(<<len:big-int-size(32), bits:bits>>)
}

pub fn date(date: calendar.Date, _ti: TypeInfo) -> Result(BitArray, String) {
  let gregorian_days =
    date_to_gregorian_days(
      date.year,
      calendar.month_to_int(date.month),
      date.day,
    )
  let pg_days = gregorian_days - internal.postgres_gd_epoch

  Ok(<<4:big-int-size(32), pg_days:big-int-size(32)>>)
}

pub fn time(tod: calendar.TimeOfDay, _ti: TypeInfo) -> Result(BitArray, String) {
  let usecs =
    tod
    |> interval.from_time_of_day
    |> interval.to_microseconds

  Ok(<<8:big-int-size(32), usecs:big-int-size(64)>>)
}

pub fn interval(
  dur: duration.Duration,
  _ti: TypeInfo,
) -> Result(BitArray, String) {
  let usecs =
    interval.from_duration(dur)
    |> interval.to_microseconds

  let encoded = <<
    16:big-int-size(32),
    usecs:big-int-size(64),
    0:big-int-size(32),
    0:big-int-size(32),
  >>

  Ok(encoded)
}

pub fn timestamp(
  ts: timestamp.Timestamp,
  _ti: TypeInfo,
) -> Result(BitArray, String) {
  let ts_int =
    interval.unix_seconds_before_postgres_epoch()
    |> interval.add_to(ts)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), ts_int:big-int-size(64)>>)
}

pub fn timestampz(
  tsz: #(timestamp.Timestamp, Interval),
  _ti: TypeInfo,
) -> Result(BitArray, String) {
  let #(ts, offset) = tsz

  let ts_int =
    interval.unix_seconds_before_postgres_epoch()
    |> interval.add(offset)
    |> interval.add_to(ts)
    |> internal.to_microseconds(timestamp.to_unix_seconds_and_nanoseconds)

  Ok(<<8:big-int-size(32), ts_int:big-int-size(64)>>)
}

// ---------- Decoding ---------- //

pub fn decode(val: BitArray, ti: TypeInfo) -> Result(Dynamic, String) {
  case ti.typereceive {
    "array_recv" ->
      decode_array(val, with: fn(elem) {
        case ti.elem_type {
          Some(elem_ti) -> decode(elem, elem_ti)
          None -> Error("elem type missing")
        }
      })
    "boolrecv" -> decode_bool(val)
    "oidrecv" -> decode_oid(val)
    "int2recv" -> decode_int2(val)
    "int4recv" -> decode_int4(val)
    "int8recv" -> decode_int8(val)
    "float4recv" -> decode_float4(val)
    "float8recv" -> decode_float8(val)
    "textrecv" -> decode_text(val)
    "varcharrecv" -> decode_varchar(val)
    "namerecv" -> decode_text(val)
    "charrecv" -> decode_text(val)
    "bytearecv" -> decode_bytea(val)
    "time_recv" -> decode_time(val)
    "date_recv" -> decode_date(val)
    "timestamp_recv" -> decode_timestamp(val)
    "timestampz_recv" -> decode_timestamp(val)
    "interval_recv" -> decode_interval(val)
    _ -> Error("Unsupported type")
  }
}

fn decode_array(
  bits: BitArray,
  with decoder: fn(BitArray) -> Result(Dynamic, String),
) -> Result(Dynamic, String) {
  case bits {
    <<
      dimensions:big-signed-int-size(32),
      _flags:big-signed-int-size(32),
      _elem_oid:big-signed-int-size(32),
      rest:bits,
    >> -> {
      use data <- result.try(do_decode_array(dimensions, rest, []))

      decode_array_elems(data.0, decoder, [])
      |> result.map(dynamic.array)
    }
    _ -> Error("invalid array")
  }
}

fn do_decode_array(
  count: Int,
  bits: BitArray,
  acc: List(#(Int, Int)),
) -> Result(#(BitArray, List(#(Int, Int))), String) {
  case count {
    0 -> Ok(#(bits, acc))
    idx -> {
      case bits {
        <<
          nbr:big-signed-int-size(32),
          l_bound:big-signed-int-size(32),
          rest1:bits,
        >> -> {
          let current = #(nbr, l_bound)

          let data_info1 = list.prepend(acc, current)

          do_decode_array({ idx - 1 }, rest1, data_info1)
        }
        _ -> Error("invalid array")
      }
    }
  }
}

fn decode_array_elems(
  bits: BitArray,
  decoder: fn(BitArray) -> Result(Dynamic, String),
  acc: List(Dynamic),
) -> Result(List(Dynamic), String) {
  case bits {
    <<>> -> Ok(list.reverse(acc))
    <<-1:big-signed-int-size(32), rest:bits>> -> {
      list.prepend(acc, dynamic.nil())
      |> decode_array_elems(rest, decoder, _)
    }
    <<size:big-signed-int-size(32), rest:bits>> -> {
      let elem_len = size * 8

      case rest {
        <<val_bin:bits-size(elem_len), rest1:bits>> -> {
          use decoded <- result.try(decoder(val_bin))

          list.prepend(acc, decoded)
          |> decode_array_elems(rest1, decoder, _)
        }
        _ -> Error("invalid array")
      }
    }
    _ -> Error("invalid array")
  }
}

// Types decode functions

fn decode_bool(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<1:big-signed-int-size(8)>> -> Ok(dynamic.bool(True))
    <<0:big-signed-int-size(8)>> -> Ok(dynamic.bool(False))
    _ -> Error("invalid bool")
  }
}

fn decode_int2(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-signed-int-size(16)>> -> Ok(dynamic.int(num))
    _ -> Error("invalid int2")
  }
}

fn decode_oid(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-unsigned-int-size(32)>> -> Ok(dynamic.int(num))

    _ -> Error("invalid oid")
  }
}

fn decode_int4(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-signed-int-size(32)>> -> Ok(dynamic.int(num))
    _ -> Error("invalid int4")
  }
}

fn decode_int8(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<num:big-signed-int-size(64)>> -> Ok(dynamic.int(num))
    _ -> Error("invalid int8")
  }
}

fn decode_float4(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<value:big-float-size(32)>> -> {
      float.to_precision(value, 4)
      |> dynamic.float
      |> Ok
    }
    _ -> Error("invalid float4")
  }
}

fn decode_float8(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<value:big-float-size(64)>> -> {
      float.to_precision(value, 8)
      |> dynamic.float
      |> Ok
    }
    _ -> Error("invalid float8")
  }
}

fn decode_varchar(bits: BitArray) -> Result(Dynamic, String) {
  bit_array.to_string(bits)
  |> result.map(dynamic.string)
  |> result.replace_error("invalid varchar")
}

fn decode_text(bits: BitArray) -> Result(Dynamic, String) {
  bit_array.to_string(bits)
  |> result.map(dynamic.string)
  |> result.replace_error("invalid text")
}

fn decode_bytea(bits: BitArray) -> Result(Dynamic, String) {
  Ok(dynamic.bit_array(bits))
}

fn decode_time(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<microseconds:big-int-size(64)>> -> {
      let tod = from_microseconds(microseconds)

      dynamic.array([
        dynamic.int(tod.hours),
        dynamic.int(tod.minutes),
        dynamic.int(tod.seconds),
        dynamic.int(tod.nanoseconds / 1000),
      ])
      |> Ok
    }
    _ -> Error("invalid time")
  }
}

fn decode_timestamp(bits: BitArray) -> Result(Dynamic, String) {
  let pos_infinity = internal.int8_max
  let neg_infinity = -internal.int8_min

  case bits {
    <<num:signed-big-int-size(64)>> -> {
      case num {
        _pos_inf if num == pos_infinity -> Ok(dynamic.string("infinity"))
        _neg_inf if num == neg_infinity -> Ok(dynamic.string("-infinity"))
        _ -> Ok(handle_timestamp(num))
      }
    }
    _ -> Error("invalid timestamp")
  }
}

fn handle_timestamp(microseconds: Int) -> Dynamic {
  let seconds_since_unix_epoch =
    { microseconds / 1_000_000 }
    |> int.add(internal.postgres_gs_epoch)
    |> int.subtract(internal.gs_to_unix_epoch)

  let usecs_since_unix_epoch = seconds_since_unix_epoch * 1_000_000

  usecs_since_unix_epoch
  |> int.add({ microseconds % 1_000_000 })
  |> dynamic.int
}

fn decode_date(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<days:big-signed-int-size(32)>> -> {
      days_to_date(days)
      |> result.map(fn(date) {
        let month = calendar.month_to_int(date.month)

        dynamic.array([
          dynamic.int(date.year),
          dynamic.int(month),
          dynamic.int(date.day),
        ])
      })
      |> result.replace_error("Invalid month")
    }
    _ -> Error("invalid date")
  }
}

fn decode_interval(bits: BitArray) -> Result(Dynamic, String) {
  case bits {
    <<
      microseconds:big-signed-int-size(64),
      _days:big-signed-int-size(32),
      _months:big-signed-int-size(32),
    >> -> Ok(dynamic.int(microseconds))
    _ -> Error("invalid interval")
  }
}

fn from_microseconds(usecs: Int) -> calendar.TimeOfDay {
  let seconds = usecs / internal.usecs_per_sec
  let nanoseconds = { usecs % internal.usecs_per_sec } * 1000

  let #(hours, minutes, seconds) = seconds_to_time(seconds)

  calendar.TimeOfDay(hours:, minutes:, seconds:, nanoseconds:)
}

fn days_to_date(days: Int) -> Result(calendar.Date, Nil) {
  let #(year, month, day) =
    gregorian_days_to_date(days + internal.postgres_gd_epoch)

  calendar.month_from_int(month)
  |> result.map(fn(month) { calendar.Date(year:, month:, day:) })
}

// FFI

@external(erlang, "calendar", "gregorian_days_to_date")
fn gregorian_days_to_date(days: Int) -> #(Int, Int, Int)

@external(erlang, "calendar", "seconds_to_time")
fn seconds_to_time(seconds: Int) -> #(Int, Int, Int)

@external(erlang, "calendar", "date_to_gregorian_days")
fn date_to_gregorian_days(year: Int, month: Int, day: Int) -> Int
