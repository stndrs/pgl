import pgl/internal/encode

pub fn query_test() {
  let assert <<"Q":utf8, 13:int-size(32), "SELECT 1":utf8, 0>> =
    encode.query("SELECT 1")
}

pub fn password_test() {
  let assert <<"p":utf8, 24:int-size(32), "supersecurepassword":utf8, 0>> =
    encode.password("supersecurepassword")
}

pub fn parse_test() {
  let assert <<
    "P":utf8,
    16:int-size(32),
    "":utf8,
    0,
    "SELECT 1":utf8,
    0,
    0:int-size(16),
  >> = encode.parse("", "SELECT 1", [])
}

pub fn describe_test() {
  let assert <<"D", 6:int-size(32), "S":utf8, "":utf8, 0>> =
    encode.describe(encode.Statement, "")
}

pub fn flush_test() {
  let assert <<"H":utf8, 4:int-size(32)>> = encode.flush()
}

pub fn sync_test() {
  let assert <<"S":utf8, 4:int-size(32)>> = encode.sync()
}
