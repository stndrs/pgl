import gleam/dynamic/decode.{type Decoder}
import gleam/time/calendar
import gleam/time/timestamp
import pg_value

pub type User {
  User(
    id: Int,
    name: String,
    active: Bool,
    nicknames: List(String),
    birthday: calendar.Date,
    created_at: timestamp.Timestamp,
  )
}

/// Decode user by positional index.
pub fn decoder() -> Decoder(User) {
  use id <- decode.field(0, decode.int)
  use name <- decode.field(1, decode.string)
  use active <- decode.field(2, decode.bool)
  use nicknames <- decode.field(3, decode.list(of: decode.string))
  use birthday <- decode.field(4, pg_value.date_decoder())
  use created_at <- decode.field(5, pg_value.timestamp_decoder())

  User(id:, name:, active:, nicknames:, created_at:, birthday:)
  |> decode.success
}

/// Decode user by field name (for rows_as_dict mode).
pub fn fields_decoder() -> Decoder(User) {
  use id <- decode.field("id", decode.int)
  use name <- decode.field("name", decode.string)
  use active <- decode.field("active", decode.bool)
  use nicknames <- decode.field("nicknames", decode.list(of: decode.string))
  use birthday <- decode.field("birthday", pg_value.date_decoder())
  use created_at <- decode.field("created_at", pg_value.timestamp_decoder())

  User(id:, name:, active:, nicknames:, created_at:, birthday:)
  |> decode.success
}
