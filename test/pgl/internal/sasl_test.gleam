import gleam/bit_array
import gleam/string
import pgl/internal/sasl

fn codepoint(cp: Int) -> BitArray {
  let assert Ok(utf_cp) = string.utf_codepoint(cp)
  string.from_utf_codepoints([utf_cp])
  |> bit_array.from_string
}

fn with_codepoint(cp: Int) -> BitArray {
  let assert Ok(utf_cp) = string.utf_codepoint(cp)
  let s = "pass" <> string.from_utf_codepoints([utf_cp]) <> "word"
  bit_array.from_string(s)
}

// ---------- Valid inputs ---------- //

pub fn validate_ascii_test() {
  assert Ok(<<"hello":utf8>>) == sasl.validate(<<"hello":utf8>>)
  assert Ok(<<"password123":utf8>>) == sasl.validate(<<"password123":utf8>>)
  assert Ok(<<"Hello World!":utf8>>) == sasl.validate(<<"Hello World!":utf8>>)
}

pub fn validate_ascii_printable_test() {
  assert Ok(<<"~!@#$%^&*()":utf8>>) == sasl.validate(<<"~!@#$%^&*()":utf8>>)
  assert Ok(<<" ":utf8>>) == sasl.validate(<<" ":utf8>>)
}

pub fn validate_valid_unicode_test() {
  assert Ok(<<"café":utf8>>) == sasl.validate(<<"café":utf8>>)
  assert Ok(<<"über":utf8>>) == sasl.validate(<<"über":utf8>>)
  assert Ok(<<"日本語":utf8>>) == sasl.validate(<<"日本語":utf8>>)
}

pub fn validate_empty_string_test() {
  assert Ok(<<"":utf8>>) == sasl.validate(<<"":utf8>>)
}

pub fn validate_invalid_utf8_test() {
  assert Error(Nil) == sasl.validate(<<0xFF, 0xFE>>)
  assert Error(Nil) == sasl.validate(<<0x80>>)
  assert Error(Nil) == sasl.validate(<<0xC0, 0x00>>)
}

// ---------- C.1.2: Non-ASCII space characters ---------- //

pub fn validate_non_ascii_space_no_break_space_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x00A0))
  assert Error(Nil) == sasl.validate(with_codepoint(0x00A0))
}

pub fn validate_non_ascii_space_ogham_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x1680))
}

pub fn validate_non_ascii_space_en_quad_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x2000))
}

pub fn validate_non_ascii_space_hair_space_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x200A))
}

pub fn validate_non_ascii_space_zero_width_space_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x200B))
}

pub fn validate_non_ascii_space_narrow_no_break_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x202F))
}

pub fn validate_non_ascii_space_medium_mathematical_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x205F))
}

pub fn validate_non_ascii_space_ideographic_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x3000))
}

// ---------- C.2.1: ASCII control characters ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.2.1

pub fn validate_ascii_control_null_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x00))
}

pub fn validate_ascii_control_bell_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x07))
}

pub fn validate_ascii_control_tab_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x09))
}

pub fn validate_ascii_control_line_feed_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x0A))
}

pub fn validate_ascii_control_unit_separator_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x1F))
}

pub fn validate_ascii_control_delete_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x7F))
}

pub fn validate_ascii_control_embedded_test() {
  assert Error(Nil) == sasl.validate(with_codepoint(0x00))
  assert Error(Nil) == sasl.validate(with_codepoint(0x1F))
}

// ---------- C.2.2: Non-ASCII control characters ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.2.2

pub fn validate_non_ascii_control_range_start_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x0080))
}

pub fn validate_non_ascii_control_range_end_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x009F))
}

pub fn validate_non_ascii_control_06dd_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x06DD))
}

pub fn validate_non_ascii_control_070f_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x070F))
}

pub fn validate_non_ascii_control_180e_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x180E))
}

pub fn validate_non_ascii_control_200c_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x200C))
}

pub fn validate_non_ascii_control_200d_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x200D))
}

pub fn validate_non_ascii_control_2028_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x2028))
}

pub fn validate_non_ascii_control_2029_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x2029))
}

pub fn validate_non_ascii_control_2060_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x2060))
}

pub fn validate_non_ascii_control_206a_range_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x206A))
  assert Error(Nil) == sasl.validate(codepoint(0x206F))
}

pub fn validate_non_ascii_control_feff_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xFEFF))
}

pub fn validate_non_ascii_control_fff9_range_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xFFF9))
  assert Error(Nil) == sasl.validate(codepoint(0xFFFC))
}

// https://datatracker.ietf.org/doc/html/rfc3454#section-5.2
pub fn validate_non_ascii_control_1d173_range_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x1D173))
  assert Error(Nil) == sasl.validate(codepoint(0x1D17A))
}

// ---------- C.3: Private use characters ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.3

pub fn validate_private_use_bmp_start_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xE000))
}

pub fn validate_private_use_bmp_end_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xF8FF))
}

pub fn validate_private_use_plane15_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xF0000))
}

pub fn validate_private_use_plane16_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x100000))
}

pub fn validate_private_use_embedded_test() {
  assert Error(Nil) == sasl.validate(with_codepoint(0xE000))
}

// ---------- C.4: Non-character code points ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.4

pub fn validate_non_char_fdd0_range_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xFDD0))
  assert Error(Nil) == sasl.validate(codepoint(0xFDEF))
}

pub fn validate_non_char_fffe_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xFFFE))
  assert Error(Nil) == sasl.validate(codepoint(0xFFFF))
}

pub fn validate_non_char_1fffe_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x1FFFE))
  assert Error(Nil) == sasl.validate(codepoint(0x1FFFF))
}

pub fn validate_non_char_10fffe_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x10FFFE))
  assert Error(Nil) == sasl.validate(codepoint(0x10FFFF))
}

// ---------- C.5: Surrogate code points ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.5

pub fn validate_surrogate_codes_test() {
  assert Error(Nil) == sasl.validate(<<0xD800:int-size(16)>>)
  assert Error(Nil) == sasl.validate(<<0xDFFF:int-size(16)>>)
}

// ---------- C.6: Inappropriate for plain text ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.6

pub fn validate_inappropriate_plain_text_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xFFF9))
  assert Error(Nil) == sasl.validate(codepoint(0xFFFA))
  assert Error(Nil) == sasl.validate(codepoint(0xFFFB))
  assert Error(Nil) == sasl.validate(codepoint(0xFFFC))
  assert Error(Nil) == sasl.validate(codepoint(0xFFFD))
}

// ---------- C.7: Inappropriate for canonical representation ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.7

pub fn validate_inappropriate_canonical_start_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x2FF0))
}

pub fn validate_inappropriate_canonical_end_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x2FFB))
}

pub fn validate_inappropriate_canonical_mid_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x2FF5))
}

// ---------- C.8: Change display properties / deprecated ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.8

pub fn validate_display_deprecated_0340_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x0340))
}

pub fn validate_display_deprecated_0341_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x0341))
}

pub fn validate_display_deprecated_200e_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x200E))
}

pub fn validate_display_deprecated_200f_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x200F))
}

pub fn validate_display_deprecated_202a_range_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x202A))
  assert Error(Nil) == sasl.validate(codepoint(0x202E))
}

pub fn validate_display_deprecated_206a_range_test() {
  assert Error(Nil) == sasl.validate(codepoint(0x206A))
  assert Error(Nil) == sasl.validate(codepoint(0x206F))
}

// ---------- C.9: Tagging characters ---------- //
// https://datatracker.ietf.org/doc/html/rfc3454#appendix-C.9

pub fn validate_tagging_e0001_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xE0001))
}

pub fn validate_tagging_e0020_range_test() {
  assert Error(Nil) == sasl.validate(codepoint(0xE0020))
  assert Error(Nil) == sasl.validate(codepoint(0xE007F))
  assert Error(Nil) == sasl.validate(codepoint(0xE0041))
}

pub fn validate_tagging_embedded_test() {
  assert Error(Nil) == sasl.validate(with_codepoint(0xE0001))
}
