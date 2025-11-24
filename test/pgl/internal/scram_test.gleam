import pgl/internal/scram

pub fn encode_startup_test() {
  let assert <<
    62:int-size(32),
    3:int-size(16),
    0:int-size(16),
    "user":utf8,
    0,
    "postgres":utf8,
    0,
    "database":utf8,
    0,
    "postgres":utf8,
    0,
    "application_name":utf8,
    0,
    "pgl":utf8,
    0,
    0,
  >> =
    [
      #("user", "postgres"),
      #("database", "postgres"),
      #("application_name", "pgl"),
    ]
    |> scram.encode_startup
}
