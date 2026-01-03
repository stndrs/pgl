import pgl/internal/sasl

pub fn validate_ascii_test() {
  assert Ok(<<"hello":utf8>>) == sasl.validate(<<"hello":utf8>>)
  assert Ok(<<"password123":utf8>>) == sasl.validate(<<"password123":utf8>>)
  assert Ok(<<"Hello World!":utf8>>) == sasl.validate(<<"Hello World!":utf8>>)
}

pub fn validate_non_ascii_space_test() {
  assert Error(Nil) == sasl.validate(<<0x00A0:int-size(32)>>)
  assert Error(Nil) == sasl.validate(<<0x2000:int-size(32)>>)
  assert Error(Nil) == sasl.validate(<<0x3000:int-size(32)>>)
}

pub fn validate_ascii_control_char_test() {
  assert Error(Nil) == sasl.validate(<<0x00>>)
  assert Error(Nil) == sasl.validate(<<0x07>>)
  assert Error(Nil) == sasl.validate(<<0x7F>>)
}

pub fn validate_non_ascii_control_char_test() {
  assert Error(Nil) == sasl.validate(<<0x0080:int-size(32)>>)
  assert Error(Nil) == sasl.validate(<<0x009F:int-size(32)>>)
  assert Error(Nil) == sasl.validate(<<0x200C:int-size(32)>>)
  assert Error(Nil) == sasl.validate(<<0xFEFF:int-size(32)>>)
}

pub fn validate_change_display_properties_test() {
  assert Error(Nil) == sasl.validate(<<0x200E:int-size(32)>>)
  assert Error(Nil) == sasl.validate(<<0x200F:int-size(32)>>)
}

pub fn validate_inappropriate_for_canonical_representation_test() {
  assert Error(Nil) == sasl.validate(<<0x2FF0:int-size(32)>>)
  assert Error(Nil) == sasl.validate(<<0x2FFB:int-size(32)>>)
}
