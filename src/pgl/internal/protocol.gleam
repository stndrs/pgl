import gleam/bit_array
import gleam/crypto
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/function
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import pgl/internal
import pgl/internal/decode
import pgl/internal/encode
import pgl/internal/scram
import pgl/internal/socket.{type Socket}

// ---------- Config ---------- //

pub opaque type Config {
  Config(
    database: String,
    username: String,
    password: Option(String),
    application: String,
    connection_parameters: List(#(String, String)),
    ssl: internal.Ssl,
  )
}

pub const config = Config(
  database: "",
  username: "",
  password: None,
  application: "",
  connection_parameters: [],
  ssl: internal.SslVerified,
)

pub fn application(conf: Config, application: String) -> Config {
  Config(..conf, application:)
}

pub fn connection_parameters(
  conf: Config,
  connection_parameters: List(#(String, String)),
) -> Config {
  Config(..conf, connection_parameters:)
}

pub fn username(conf: Config, username: String) -> Config {
  Config(..conf, username:)
}

pub fn password(conf: Config, password: String) -> Config {
  Config(..conf, password: Some(password))
}

pub fn database(conf: Config, database: String) -> Config {
  Config(..conf, database:)
}

pub fn ssl(conf: Config, ssl: internal.Ssl) -> Config {
  Config(..conf, ssl:)
}

// ---------- Auth flow ---------- //

pub fn auth(
  sock: Socket,
  conf: Config,
) -> Result(Socket, internal.InternalError) {
  sock
  |> ssl_upgrade(conf.ssl)
  |> result.try(setup(_, conf))
}

// SSL functions

fn ssl_upgrade(
  sock: Socket,
  ssl: internal.Ssl,
) -> Result(Socket, internal.InternalError) {
  case ssl {
    internal.SslVerified -> do_ssl_upgrade(sock, verified: True)
    internal.SslUnverified -> do_ssl_upgrade(sock, verified: False)
    internal.SslDisabled -> Ok(sock)
  }
}

fn do_ssl_upgrade(
  sock: Socket,
  verified verified: Bool,
) -> Result(Socket, internal.InternalError) {
  use sock <- result.try(socket.send(sock, encode.ssl_request()))

  case socket.receive(sock, 1) {
    Ok(<<"S":utf8>>) -> socket.to_ssl(sock, verified:)
    Ok(<<"N":utf8>>) -> {
      internal.SslError
      |> internal.ProtocolError(message: "SSL Refused")
      |> Error
    }
    Ok(_) -> {
      internal.SslError
      |> internal.ProtocolError(message: "Failed to upgrade SSL")
      |> Error
    }
    Error(err) -> Error(err)
  }
}

fn setup(sock: Socket, conf: Config) -> Result(Socket, internal.InternalError) {
  let message =
    [
      #("user", conf.username),
      #("database", conf.database),
      #("application_name", conf.application),
      ..conf.connection_parameters
    ]
    |> encode.startup

  use sock <- result.try(socket.send(sock, message))

  auth_flow(sock, conf, <<>>)
}

// https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256
fn auth_flow(
  sock: Socket,
  conf: Config,
  prev: BitArray,
) -> Result(Socket, internal.InternalError) {
  use msg <- result.try(receive_message(sock))

  case msg {
    internal.AuthenticationOk -> auth_flow(sock, conf, prev)
    internal.AuthenticationMD5Password(salt:) ->
      do_password_auth(conf, md5_password(conf.username, _, salt), "MD5", sock)
    internal.AuthenticationCleartextPassword ->
      do_password_auth(conf, function.identity, "password", sock)
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
    internal.NotificationResponse(_, _, _) -> auth_flow(sock, conf, <<>>)
    internal.NoticeResponse(_) -> auth_flow(sock, conf, <<>>)
    internal.ParameterStatus(name:, value:) -> {
      sock
      |> socket.parameter(name, value)
      |> auth_flow(conf, <<>>)
    }
    internal.ReadyForQuery(status: _) -> Ok(sock)
    _ -> {
      internal.MessageError
      |> internal.ProtocolError(message: "Unexpected message")
      |> Error
    }
  }
}

fn md5_password(username: String, password: String, salt: BitArray) -> String {
  let inner =
    crypto.hash(crypto.Md5, <<password:utf8, username:utf8>>)
    |> bit_array.base16_encode
    |> string.lowercase

  let outer =
    crypto.hash(crypto.Md5, <<inner:utf8, salt:bits>>)
    |> bit_array.base16_encode
    |> string.lowercase

  "md5" <> outer
}

fn do_password_auth(
  conf: Config,
  process_password: fn(String) -> String,
  kind: String,
  sock: Socket,
) -> Result(Socket, internal.InternalError) {
  case conf.password {
    option.Some(pass) -> {
      pass
      |> process_password
      |> encode.password
      |> socket.send(sock, _)
      |> result.try(auth_flow(_, conf, <<>>))
    }
    option.None ->
      internal.AuthenticationFailed
      |> internal.AuthenticationError(
        "Server requested "
        <> kind
        <> "authentication but no password was provided",
      )
      |> Error
  }
}

fn auth_sasl(
  sock: Socket,
  methods: List(String),
  conf: Config,
) -> Result(BitArray, internal.InternalError) {
  case methods {
    ["SCRAM-SHA-256"] -> {
      let client_nonce = scram.get_nonce(16)

      scram.client_first(<<conf.username:utf8>>, client_nonce)
      |> encode.auth_scram_client_first
      |> socket.send(sock, _)
      |> result.replace(client_nonce)
    }
    _ -> {
      internal.AuthenticationError(
        kind: internal.MethodNotImplemented,
        message: "Supported methods: [SCRAM-SHA-256]",
      )
      |> Error
    }
  }
}

fn handle_error_response(
  fields: Dict(BitArray, String),
) -> Result(a, internal.InternalError) {
  let code = dict.get(fields, <<"C":utf8>>) |> result.unwrap("")
  let message = dict.get(fields, <<"M":utf8>>) |> result.unwrap("")
  let name = internal.pg_error_code_name(code) |> result.unwrap("")

  internal.PostgresError(code:, name:, message:, fields:)
  |> Error
}

fn auth_sasl_continue(
  sock: Socket,
  conf: Config,
  server_first: BitArray,
  client_nonce: BitArray,
) -> Result(BitArray, internal.InternalError) {
  scram.parse_server_first(server_first, client_nonce)
  |> result.try(fn(sf) {
    let user = <<conf.username:utf8>>
    case conf.password {
      option.None -> {
        internal.AuthenticationFailed
        |> internal.AuthenticationError(
          "Server requested SCRAM authentication but no password was provided",
        )
        |> Error
      }
      option.Some(password) -> {
        let pass = <<password:utf8>>

        use #(client_final, server_signature) <- result.try(scram.client_final(
          sf,
          client_nonce,
          user,
          pass,
        ))

        let encoded_client_final = encode.scram_response(client_final)

        socket.send(sock, encoded_client_final)
        |> result.replace(server_signature)
      }
    }
  })
}

fn auth_sasl_final(
  server_final: BitArray,
  server_signature: BitArray,
) -> Result(BitArray, internal.InternalError) {
  use srv_final <- result.try(scram.parse_server_final(server_final))

  case srv_final == server_signature {
    True -> Ok(server_signature)
    False -> {
      internal.AuthenticationError(
        kind: internal.AuthenticationFailed,
        message: "Failed to match server signature",
      )
      |> Error
    }
  }
}

// ---------- Simple Query ---------- //
//
// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY

type Row =
  List(Option(BitArray))

pub fn simple(
  packet: BitArray,
  sock: Socket,
) -> Result(List(Row), internal.InternalError) {
  use sock <- result.try(socket.send(sock, packet))

  simple_flow(sock, [])
}

fn simple_flow(
  sock: Socket,
  acc: List(Row),
) -> Result(List(Row), internal.InternalError) {
  use msg <- result.try(receive_message(sock))

  case msg {
    internal.CommandComplete(_, _) -> simple_flow(sock, acc)
    internal.DataRow(values:) -> simple_flow(sock, [values, ..acc])
    internal.ErrorResponse(fields:) -> handle_error_response(fields)
    internal.NoticeResponse(_) -> simple_flow(sock, acc)
    internal.NotificationResponse(_, _, _) -> simple_flow(sock, acc)
    internal.ReadyForQuery(status: _) -> Ok(acc)
    internal.RowDescription(_, _) -> simple_flow(sock, acc)
    _ -> {
      internal.ProtocolError(
        kind: internal.MessageError,
        message: "Unexpected message in simple flow",
      )
      |> Error
    }
  }
}

fn flush(
  res: Result(b, internal.InternalError),
  sock: Socket,
) -> Result(b, internal.InternalError) {
  use msg <- result.try(receive_message(sock))

  case msg {
    internal.ParameterStatus(_, _) -> flush(res, sock)
    internal.ReadyForQuery(status: _) -> res
    _ -> flush(res, sock)
  }
}

fn sync(sock: Socket) -> Result(Socket, internal.InternalError) {
  use sock <- result.try(
    encode.sync()
    |> socket.send(sock, _),
  )
  use msg <- result.try(receive_message(sock))
  case msg {
    internal.ReadyForQuery(_) -> Ok(sock)
    _ ->
      internal.MessageError
      |> internal.ProtocolError(message: "Expected ReadyForQuery after Sync")
      |> Error
  }
}

// ---------- Extended(v) Query ---------- //
//
// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

pub type HandleParamDescription(v) =
  fn(String, List(v), List(Int)) -> Result(BitArray, internal.InternalError)

pub type HandleDecodeRow =
  fn(Row, List(Int)) -> Result(List(Dynamic), internal.InternalError)

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
  Extended(
    needs_sync: False,
    handle_decode_row: fn(_, _) { Ok([]) },
    handle_param_description: fn(_, _, _) { Ok(<<>>) },
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
) -> Result(Extended(v), internal.InternalError) {
  let needs_sync = encode.needs_sync(query)
  use packet <- result.try(encode.to_bit_array(query))

  let flow = Extended(..flow, needs_sync:)
  let pl = pipeline()

  use sock <- result.try(socket.send(sock, packet))
  use pl <- result.try(do_pipeline(pl, flow, [query], sock))

  case pl.acc {
    [extended] -> Ok(extended)
    _ -> {
      internal.ProtocolError(
        kind: internal.ProcessingError,
        message: "Missing rows",
      )
      |> Error
    }
  }
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
) -> Result(Extended(v), internal.InternalError) {
  let oids = list.map(rows.descriptions, fn(d) { d.data_type_oid })

  use values <- result.map(decode_row(row, oids))

  let values = list.prepend(rows.values, values)

  Extended(..rows, values:)
}

fn receive_message(
  sock: Socket,
) -> Result(internal.Message, internal.InternalError) {
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
) -> Result(List(Extended(v)), internal.InternalError) {
  use encoded <- result.try(list.try_map(queries, encode.to_bit_array))

  let packet =
    encoded
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
) -> Result(Pipeline(v), internal.InternalError) {
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
        internal.ProtocolError(
          kind: internal.MessageError,
          message: "Unexpected message in flow",
        )
        |> Error
      })
    }
  }
}

fn error_response_cleanup(
  err: Result(a, internal.InternalError),
  needs_sync: Bool,
  syncs: Int,
  ready: Int,
  sock: Socket,
) -> Result(a, internal.InternalError) {
  let err = case needs_sync {
    False -> flush(err, sock)
    True ->
      case socket.send(sock, encode.sync()) {
        Ok(_) -> flush(err, sock)
        Error(_) -> err
      }
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
) -> Result(Pipeline(v), internal.InternalError) {
  let sql = query.sql
  let params = query.params

  use packet <- result.try(ext.handle_param_description(sql, params, oids))

  use sock <- result.try(socket.send(sock, packet))

  increment_sync(pl)
  |> do_pipeline(ext, rest, sock)
}
