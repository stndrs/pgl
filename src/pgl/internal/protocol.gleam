import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import pgl/internal
import pgl/internal/decode
import pgl/internal/encode
import pgl/internal/scram
import pgl/internal/socket.{type Socket}

// ---------- Config ---------- //

pub type Config {
  Config(
    database: String,
    username: String,
    password: String,
    application: String,
    ssl: Option(Bool),
  )
}

pub const config = Config(
  database: "",
  username: "",
  password: "",
  application: "",
  ssl: None,
)

pub fn application(conf: Config, application: String) -> Config {
  Config(..conf, application:)
}

pub fn username(conf: Config, username: String) -> Config {
  Config(..conf, username:)
}

pub fn password(conf: Config, password: String) -> Config {
  Config(..conf, password:)
}

pub fn database(conf: Config, database: String) -> Config {
  Config(..conf, database:)
}

pub fn ssl(conf: Config, ssl: Option(Bool)) -> Config {
  Config(..conf, ssl:)
}

// ---------- Auth flow ---------- //

pub fn auth(sock: Socket, conf: Config) -> Result(Socket, internal.PglError) {
  sock
  |> ssl_upgrade(conf.ssl)
  |> result.try(setup(_, conf))
}

// SSL functions

pub fn ssl_upgrade(
  sock: Socket,
  ssl: Option(Bool),
) -> Result(Socket, internal.PglError) {
  case ssl {
    Some(verified) -> do_ssl_upgrade(sock, verified:)
    None -> Ok(sock)
  }
}

fn do_ssl_upgrade(
  sock: Socket,
  verified verified: Bool,
) -> Result(Socket, internal.PglError) {
  use sock <- result.try(socket.send(sock, encode.ssl_request()))

  case socket.receive(sock, 1) {
    Ok(<<"S":utf8>>) -> socket.to_ssl(sock, verified:)
    Ok(<<"N":utf8>>) -> {
      internal.SslError
      |> internal.SocketError(message: "SSL Refused")
      |> Error
    }
    Ok(_) -> {
      internal.SslError
      |> internal.SocketError(message: "Failed to upgrade SSL")
      |> Error
    }
    Error(err) -> Error(err)
  }
}

fn setup(sock: Socket, conf: Config) -> Result(Socket, internal.PglError) {
  let message =
    [
      #("user", conf.username),
      #("database", conf.database),
      #("application_name", conf.application),
    ]
    |> encode.startup

  use sock <- result.try(socket.send(sock, message))

  sock
  |> auth_flow(conf, <<>>)
  |> result.replace(sock)
}

// https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256
fn auth_flow(
  sock: Socket,
  conf: Config,
  prev: BitArray,
) -> Result(BitArray, internal.PglError) {
  use msg <- result.try(receive_message(sock))

  case msg {
    internal.AuthenticationOk -> auth_flow(sock, conf, prev)
    internal.AuthenticationSASL(methods:) -> {
      use nonce <- result.try(auth_sasl(sock, methods, conf))

      auth_flow(sock, conf, nonce)
    }
    internal.AuthenticationSASLContinue(first) -> {
      use srv_sig <- result.try(auth_sasl_continue(sock, conf, first, prev))

      auth_flow(sock, conf, srv_sig)
    }
    internal.AuthenticationSASLFinal(server_final:) -> {
      use _ <- result.try(auth_sasl_final(server_final, prev))

      auth_flow(sock, conf, <<>>)
    }
    internal.ErrorResponse(fields:) -> handle_error_response(fields)
    internal.BackendKeyData(_, _) -> auth_flow(sock, conf, <<>>)
    internal.BindComplete -> Ok(<<>>)
    internal.NotificationResponse(_, _, _) -> auth_flow(sock, conf, <<>>)
    internal.NoticeResponse(_) -> auth_flow(sock, conf, <<>>)
    internal.ParameterStatus(name:, value:) -> {
      sock
      |> socket.parameter(name, value)
      |> auth_flow(conf, <<>>)
    }
    internal.ReadyForQuery(status: _) -> Ok(<<>>)
    _ -> {
      internal.MessageError
      |> internal.ProtocolError(message: "Unexpected message")
      |> Error
    }
  }
}

fn auth_sasl(
  sock: Socket,
  methods: List(String),
  conf: Config,
) -> Result(BitArray, internal.PglError) {
  case methods {
    ["SCRAM-SHA-256"] -> {
      let client_nonce = scram.get_nonce(16)

      scram.client_first(<<conf.username:utf8>>, client_nonce)
      |> encode.auth_scram_client_first
      |> socket.send(sock, _)
      |> result.replace(client_nonce)
    }
    _ -> {
      internal.MethodNotSupported
      |> internal.AuthenticationError(
        message: "Supported methods: [SCRAM-SHA-256]",
      )
      |> Error
    }
  }
}

fn handle_error_response(
  fields: Dict(internal.Field, String),
) -> Result(a, internal.PglError) {
  internal.from_response_fields(fields)
  |> internal.PostgresError
  |> Error
}

fn auth_sasl_continue(
  sock: Socket,
  conf: Config,
  server_first: BitArray,
  client_nonce: BitArray,
) -> Result(BitArray, internal.PglError) {
  scram.parse_server_first(server_first, client_nonce)
  |> result.try(fn(sf) {
    let user = <<conf.username:utf8>>
    let pass = <<conf.password:utf8>>

    let #(client_final, server_signature) =
      scram.client_final(sf, client_nonce, user, pass)

    let encoded_client_final = encode.scram_response(client_final)

    socket.send(sock, encoded_client_final)
    |> result.replace(server_signature)
  })
}

fn auth_sasl_final(
  server_final: BitArray,
  server_signature: BitArray,
) -> Result(BitArray, internal.PglError) {
  scram.parse_server_final(server_final)
  |> result.map_error(internal.authentication_failed(_, ""))
  |> result.try(fn(srv_final) {
    case srv_final == server_signature {
      True -> Ok(server_signature)
      False -> Error(internal.signature_mismatch("Failed to match signature"))
    }
  })
}

// ---------- Simple Query ---------- //
//
// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY

type Row =
  List(BitArray)

pub fn simple(
  packet: BitArray,
  sock: Socket,
) -> Result(List(Row), internal.PglError) {
  use sock <- result.try(socket.send(sock, packet))

  simple_flow(sock, [])
}

fn simple_flow(
  sock: Socket,
  acc: List(Row),
) -> Result(List(Row), internal.PglError) {
  use msg <- result.try(receive_message(sock))

  case msg {
    internal.CommandComplete(_, _) -> simple_flow(sock, acc)
    internal.DataRow(values:) -> simple_flow(sock, [values, ..acc])
    internal.ErrorResponse(fields:) -> handle_error_response(fields)
    internal.NoticeResponse(_) -> simple_flow(sock, acc)
    internal.NotificationResponse(_, _, _) -> simple_flow(sock, acc)
    internal.ReadyForQuery(status: _) -> Ok(acc)
    internal.RowDescription(_, _) -> simple_flow(sock, acc)
    _ -> Error(internal.PglError("Unexpected message in simple flow"))
  }
}

// ---------- Ping ---------- //

pub fn ping(sock: Socket) -> Result(Socket, internal.PglError) {
  encode.sync()
  |> socket.send(sock, _)
  |> flush(sock)
}

fn flush(
  res: Result(b, internal.PglError),
  sock: Socket,
) -> Result(b, internal.PglError) {
  use msg <- result.try(receive_message(sock))

  case msg {
    internal.ParameterStatus(_, _) -> flush(res, sock)
    internal.ReadyForQuery(status: _) -> res
    _ -> flush(res, sock)
  }
}

fn sync(sock: Socket) -> Result(Socket, internal.PglError) {
  encode.sync()
  |> socket.send(sock, _)
  |> result.try(receive_message)
  |> result.replace(sock)
}

// ---------- Extended(v) Query ---------- //
//
// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

pub type HandleParamDescription(v) =
  fn(String, List(v), List(Int)) -> Result(BitArray, internal.PglError)

pub type HandleDecodeRow =
  fn(Row, List(Int)) -> Result(List(Dynamic), internal.PglError)

pub type Extended(v) {
  Extended(
    needs_sync: Bool,
    handle_decode_row: HandleDecodeRow,
    handle_param_description: HandleParamDescription(v),
    descriptions: List(internal.RowDescriptionField),
    fields: List(String),
    values: List(List(Dynamic)),
    count: Int,
  )
}

pub fn extended() -> Extended(v) {
  let default = internal.PglError("Extended(v) flow not configured")

  Extended(
    needs_sync: False,
    handle_decode_row: fn(_, _) { Error(default) },
    handle_param_description: fn(_, _, _) { Error(default) },
    descriptions: [],
    fields: [],
    values: [],
    count: 0,
  )
}

pub fn on_param_description(
  ext: Extended(v),
  handle_param_description: HandleParamDescription(v),
) -> Extended(v) {
  Extended(..ext, handle_param_description:)
}

pub fn on_decode_row(
  ext: Extended(v),
  handle_decode_row: HandleDecodeRow,
) -> Extended(v) {
  Extended(..ext, handle_decode_row:)
}

pub fn process(
  flow: Extended(v),
  query: encode.Query(v, t),
  sock: Socket,
) -> Result(Extended(v), internal.PglError) {
  let needs_sync = encode.needs_sync(query)
  let packet = encode.to_bit_array(query)
  let flow = Extended(..flow, needs_sync:)
  let pl = pipeline()

  use sock <- result.try(socket.send(sock, packet))
  use pl <- result.try(do_pipeline(pl, flow, [query], sock))

  pl.acc
  |> list.first
  |> result.map_error(fn(_) { internal.PglError("missing rows") })
}

fn handle_row_description(
  ext: Extended(v),
  descriptions: List(internal.RowDescriptionField),
) -> Extended(v) {
  let fields = list.map(descriptions, fn(desc) { desc.name })

  Extended(..ext, descriptions:, fields:)
}

fn handle_data_row(
  row: Row,
  rows: Extended(v),
  with decode_row: HandleDecodeRow,
) -> Result(Extended(v), internal.PglError) {
  let oids = list.map(rows.descriptions, fn(d) { d.data_type_oid })

  use values <- result.map(decode_row(row, oids))

  let values = list.prepend(rows.values, values)

  Extended(..rows, values:)
}

fn receive_message(sock: Socket) -> Result(internal.Message, internal.PglError) {
  use data <- result.try(socket.receive(sock, internal.header_size))

  case data {
    <<code:bits-size(8), size:int-size(32)>> -> {
      case size - 4 {
        0 -> decode.message(code, <<>>)
        size1 -> {
          use payload <- result.try(socket.receive(sock, size1))

          decode.message(code, payload)
        }
      }
    }
    _ -> {
      internal.DecodingError
      |> internal.ProtocolError(message: "Unexpected data received")
      |> Error
    }
  }
}

// ---------- Pipeline ---------- //

pub type Pipeline(v) {
  Pipeline(syncs: Int, ready: Int, acc: List(Extended(v)))
}

fn set_acc(pl: Pipeline(v), acc: List(Extended(v))) -> Pipeline(v) {
  Pipeline(..pl, acc:)
}

fn reverse_acc(pl: Pipeline(v)) -> Pipeline(v) {
  Pipeline(..pl, acc: list.reverse(pl.acc))
}

fn increment_sync(pl: Pipeline(v)) -> Pipeline(v) {
  Pipeline(..pl, syncs: pl.syncs + 1)
}

fn increment_ready(pl: Pipeline(v)) -> Pipeline(v) {
  Pipeline(..pl, ready: pl.ready + 1)
}

pub fn pipeline() -> Pipeline(v) {
  Pipeline(syncs: 0, ready: 0, acc: [])
}

pub fn batch_process(
  flow: Pipeline(v),
  extended: Extended(v),
  queries: List(encode.Query(v, t)),
  sock: Socket,
) -> Result(List(Extended(v)), internal.PglError) {
  let packet =
    queries
    |> list.map(encode.to_bit_array)
    |> bit_array.concat
    |> bit_array.append(encode.sync())

  use sock <- result.try(socket.send(sock, packet))

  flow
  |> increment_sync
  |> do_pipeline(extended, queries, sock)
  |> result.map(fn(pl) { pl.acc })
}

fn do_pipeline(
  pl: Pipeline(v),
  ext: Extended(v),
  queries: List(encode.Query(v, t)),
  sock: Socket,
) -> Result(Pipeline(v), internal.PglError) {
  use msg <- result.try(receive_message(sock))

  case msg {
    internal.BindComplete -> do_pipeline(pl, ext, queries, sock)
    internal.CommandComplete(command: _, rows: count) -> {
      let ext = Extended(..ext, count:)
      let acc = list.prepend(pl.acc, ext)

      let next_ext =
        Extended(
          needs_sync: ext.needs_sync,
          handle_decode_row: ext.handle_decode_row,
          handle_param_description: ext.handle_param_description,
          descriptions: [],
          fields: [],
          values: [],
          count: 0,
        )

      set_acc(pl, acc)
      |> do_pipeline(next_ext, queries, sock)
    }
    internal.DataRow(values:) -> {
      handle_data_row(values, ext, ext.handle_decode_row)
      |> result.try(do_pipeline(pl, _, queries, sock))
    }
    internal.ErrorResponse(fields:) -> {
      fields
      |> handle_error_response
      |> error_response_cleanup(ext.needs_sync, pl.syncs, pl.ready, sock)
    }
    internal.NoData -> do_pipeline(pl, ext, queries, sock)
    internal.NoticeResponse(_) -> do_pipeline(pl, ext, queries, sock)
    internal.NotificationResponse(_, _, _) ->
      do_pipeline(pl, ext, queries, sock)
    internal.ParameterDescription(_, data_types:) ->
      handle_parameter_description(pl, queries, ext, data_types, sock)
    internal.ParseComplete -> do_pipeline(pl, ext, queries, sock)
    internal.ReadyForQuery(status: _) -> {
      let pl = increment_ready(pl)

      case pl.syncs > pl.ready {
        True -> do_pipeline(pl, ext, queries, sock)
        False -> Ok(reverse_acc(pl))
      }
    }
    internal.RowDescription(_, descriptions) -> {
      handle_row_description(ext, descriptions)
      |> do_pipeline(pl, _, queries, sock)
    }
    _ -> {
      sync(sock)
      |> result.try_recover(Error)
      |> result.try(fn(_) {
        Error(internal.PglError("Unexpected message in flow"))
      })
    }
  }
}

fn error_response_cleanup(
  err: Result(a, internal.PglError),
  needs_sync: Bool,
  syncs: Int,
  ready: Int,
  sock: Socket,
) -> Result(a, internal.PglError) {
  let err = case needs_sync {
    False -> flush(err, sock)
    True ->
      encode.sync()
      |> socket.send(sock, _)
      |> result.try(fn(_) { flush(err, sock) })
  }

  case syncs > ready {
    True -> error_response_cleanup(err, False, syncs, ready + 1, sock)
    False -> err
  }
}

fn handle_parameter_description(
  pl: Pipeline(v),
  queries: List(encode.Query(v, t)),
  ext: Extended(v),
  oids: List(Int),
  sock: Socket,
) {
  case queries {
    [] -> do_pipeline(pl, ext, queries, sock)
    [query] -> next_param_description(pl, query, [], ext, oids, sock)
    [query, ..rest] -> next_param_description(pl, query, rest, ext, oids, sock)
  }
}

fn next_param_description(
  pl: Pipeline(v),
  query: encode.Query(v, t),
  rest: List(encode.Query(v, t)),
  ext: Extended(v),
  oids: List(Int),
  sock: Socket,
) -> Result(Pipeline(v), internal.PglError) {
  let sql = query.sql
  let params = query.params

  use packet <- result.try(ext.handle_param_description(sql, params, oids))

  use sock <- result.try(socket.send(sock, packet))

  increment_sync(pl)
  |> do_pipeline(ext, rest, sock)
}
