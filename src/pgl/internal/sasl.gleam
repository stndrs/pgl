import gleam/bit_array
import gleam/list
import gleam/result
import gleam/string

// https://github.com/epgsql/epgsql/blob/devel/src/epgsql_sasl_prep_profile.erl#L8
pub fn validate(data: BitArray) -> Result(BitArray, Nil) {
  data
  |> bit_array.to_string
  |> result.map(fn(str) {
    string.to_utf_codepoints(str)
    |> list.map(string.utf_codepoint_to_int)
  })
  |> result.try(fn(chars) {
    let fns = [
      is_non_ascii_space_char,
      is_ascii_control_char,
      is_non_ascii_control_char,
      is_private_use_char,
      is_non_char_code_points,
      is_surrogate_code_point,
      is_inappropriate_for_plan_text_char,
      is_inappropriate_for_canonical_representation_char,
      is_change_display_properties_or_deprecated_char,
      is_tagging_char,
    ]

    fns
    |> list.try_each(with: fn(validation) {
      case list.any(chars, validation) {
        True -> Error(Nil)
        False -> Ok(Nil)
      }
    })
  })
  |> result.replace(data)
}

// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.1.2
fn is_non_ascii_space_char(char: Int) -> Bool {
  case char {
    0x00A0 -> True
    0x1680 -> True
    0x2000 -> True
    0x2001 -> True
    0x2002 -> True
    0x2003 -> True
    0x2004 -> True
    0x2005 -> True
    0x2006 -> True
    0x2007 -> True
    0x2008 -> True
    0x2009 -> True
    0x200A -> True
    0x200B -> True
    0x202F -> True
    0x205F -> True
    0x3000 -> True
    _ -> False
  }
}

// https://tools.ietf.org/html/rfc3454#appendix-C.2.1
fn is_ascii_control_char(char: Int) -> Bool {
  char <= 0x001F || char == 0x007F
}

// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.2.2
fn is_non_ascii_control_char(char: Int) -> Bool {
  { 0x0080 <= char && char <= 0x009F }
  || case char {
    0x06DD -> True
    0x070F -> True
    0x180E -> True
    0x200C -> True
    0x200D -> True
    0x2028 -> True
    0x2029 -> True
    0x2060 -> True
    0x2061 -> True
    0x2062 -> True
    0x2063 -> True
    0xFEFF -> True
    _ -> False
  }
  || { 0x206A <= char && char <= 0x206F }
  || { 0xFFF9 <= char && char <= 0xFFFC }
  || { 0x1D173 <= char && char <= 0x1D17A }
}

// https://tools.ietf.org/html/rfc3454#appendix-C.3
fn is_private_use_char(char: Int) -> Bool {
  { 0xE000 <= char && char <= 0xF8FF }
  || { 0xF000 <= char && char <= 0xFFFFD }
  || { 0x100000 <= char && char <= 0x10FFFD }
}

// https://tools.ietf.org/html/rfc3454#appendix-C.4
fn is_non_char_code_points(char: Int) -> Bool {
  { 0xFDD0 <= char && char <= 0xFDEF }
  || { 0xFFFE <= char && char <= 0xFFFF }
  || { 0x1FFFE <= char && char <= 0x1FFFF }
  || { 0x2FFFE <= char && char <= 0x2FFFF }
  || { 0x3FFFE <= char && char <= 0x3FFFF }
  || { 0x4FFFE <= char && char <= 0x4FFFF }
  || { 0x5FFFE <= char && char <= 0x5FFFF }
  || { 0x6FFFE <= char && char <= 0x6FFFF }
  || { 0x7FFFE <= char && char <= 0x7FFFF }
  || { 0x8FFFE <= char && char <= 0x8FFFF }
  || { 0x9FFFE <= char && char <= 0x9FFFF }
  || { 0xAFFFE <= char && char <= 0xAFFFF }
  || { 0xBFFFE <= char && char <= 0xBFFFF }
  || { 0xCFFFE <= char && char <= 0xCFFFF }
  || { 0xDFFFE <= char && char <= 0xDFFFF }
  || { 0xEFFFE <= char && char <= 0xEFFFF }
  || { 0xFFFFE <= char && char <= 0xFFFFF }
  || { 0x10FFFE <= char && char <= 0x10FFFF }
}

// https://tools.ietf.org/html/rfc3454#appendix-C.5
fn is_surrogate_code_point(char: Int) -> Bool {
  0xD800 <= char && char <= 0xDFFF
}

// https://tools.ietf.org/html/rfc3454#appendix-C.6
fn is_inappropriate_for_plan_text_char(char: Int) -> Bool {
  0xFFF9 <= char && char <= 0xFFFD
}

// https://tools.ietf.org/html/rfc3454#appendix-C.7
fn is_inappropriate_for_canonical_representation_char(char: Int) -> Bool {
  0x2FF0 <= char && char <= 0x2FFB
}

// https://tools.ietf.org/html/rfc3454#appendix-C.8
fn is_change_display_properties_or_deprecated_char(char: Int) -> Bool {
  case char {
    0x0340 -> True
    0x0341 -> True
    0x200E -> True
    0x200F -> True
    _ -> False
  }
  || { 0x202A <= char && char <= 0x202E }
  || { 0x206A <= char && char <= 0x206F }
}

// https://tools.ietf.org/html/rfc3454#appendix-C.9
fn is_tagging_char(char: Int) -> Bool {
  char == 0xE0001 || 0xE0020 <= char && char <= 0xE007F
}
