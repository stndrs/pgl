import birdie
import gleam/list
import gleam/string
import pgl/internal

pub fn decoding_error_test() {
  let assert internal.ProtocolError(
    kind: internal.DecodingError,
    message: "test message",
  ) = internal.decode_error("test message")
}

pub fn encoding_error_test() {
  let assert internal.ProtocolError(
    kind: internal.EncodingError,
    message: "test message",
  ) = internal.encode_error("test message")
}

pub fn message_error_test() {
  let assert internal.ProtocolError(
    kind: internal.MessageError,
    message: "test message",
  ) = internal.message_error("test message")
}

pub fn rollback_test() {
  let assert internal.RollbackError(cause: Nil) = internal.rollback(Nil)
}

pub fn not_in_transaction_test() {
  let assert internal.NotInTransaction(message: "test message") =
    internal.not_in_transaction("test message")
}

pub fn type_encode_error_test() {
  let assert internal.TypeError(
    kind: internal.TypeEncodeError,
    message: "test message",
  ) = internal.type_encode_error("test message")
}

pub fn type_decode_error_test() {
  let assert internal.TypeError(
    kind: internal.TypeDecodeError,
    message: "test message",
  ) = internal.type_decode_error("test message")
}

pub fn pg_error_codes_test() {
  let assert Ok(result) =
    pg_error_codes
    |> list.try_map(internal.pg_error_code_name)

  result
  |> string.join(", ")
  |> birdie.snap("pg error codes")
}

pub fn posix_error_to_string_test() {
  posix_error_codes
  |> list.map(internal.posix_error_to_string)
  |> string.join(", ")
  |> birdie.snap("posix error codes")
}

const pg_error_codes = [
  "00000", "01000", "0100C", "01008", "01003", "01007", "01006", "01004",
  "01P01", "02000", "02001", "03000", "08000", "08003", "08006", "08001",
  "08004", "08007", "08P01", "09000", "0A000", "0B000", "0F000", "0F001",
  "0L000", "0LP01", "0P000", "0Z000", "0Z002", "20000", "21000", "22000",
  "2202E", "22021", "22008", "22012", "22005", "2200B", "22022", "22015",
  "2201E", "22014", "22016", "2201F", "2201G", "22018", "22007", "22019",
  "2200D", "22025", "22P06", "22010", "22023", "22013", "2201B", "2201W",
  "2201X", "2202H", "2202G", "22009", "2200C", "2200G", "22004", "22002",
  "22003", "2200H", "22026", "22001", "22011", "22027", "22024", "2200F",
  "22P01", "22P02", "22P03", "22P04", "22P05", "2200L", "2200M", "2200N",
  "2200S", "2200T", "22030", "22031", "22032", "22033", "22034", "22035",
  "22036", "22037", "22038", "22039", "2203A", "2203B", "2203C", "2203D",
  "2203E", "2203F", "23000", "23001", "23502", "23503", "23505", "23514",
  "23P01", "24000", "25000", "25001", "25002", "25008", "25003", "25004",
  "25005", "25006", "25007", "25P01", "25P02", "25P03", "26000", "27000",
  "28000", "28P01", "2B000", "2BP01", "2D000", "2F000", "2F005", "2F002",
  "2F003", "2F004", "34000", "38000", "38001", "38002", "38003", "38004",
  "39000", "39001", "39004", "39P01", "39P02", "39P03", "3B000", "3B001",
  "3D000", "3F000", "40000", "40002", "40001", "40003", "40P01", "42000",
  "42601", "42501", "42846", "42803", "42P20", "42P19", "42830", "42602",
  "42622", "42939", "42804", "42P18", "42P21", "42P22", "42809", "428C9",
  "42703", "42883", "42P01", "42P02", "42704", "42701", "42P03", "42P04",
  "42723", "42P05", "42P06", "42P07", "42712", "42710", "42702", "42725",
  "42P08", "42P09", "42P10", "42611", "42P11", "42P12", "42P13", "42P14",
  "42P15", "42P16", "42P17", "44000", "53000", "53100", "53200", "53300",
  "53400", "54000", "54001", "54011", "54023", "55000", "55006", "55P02",
  "55P03", "55P04", "57000", "57014", "57P01", "57P02", "57P03", "57P04",
  "57P05", "58000", "58030", "58P01", "58P02", "72000", "F0000", "F0001",
  "HV000", "HV005", "HV002", "HV010", "HV021", "HV024", "HV007", "HV008",
  "HV004", "HV006", "HV091", "HV00B", "HV00C", "HV00D", "HV090", "HV00A",
  "HV009", "HV014", "HV001", "HV00P", "HV00J", "HV00K", "HV00Q", "HV00R",
  "HV00L", "HV00M", "HV00N", "P0000", "P0001", "P0002", "P0003", "P0004",
  "XX000", "XX001", "XX002",
]

const posix_error_codes = [
  internal.Closed,
  internal.Timeout,
  internal.Eaddrinuse,
  internal.Eaddrnotavail,
  internal.Eafnosupport,
  internal.Ealready,
  internal.Econnaborted,
  internal.Econnrefused,
  internal.Econnreset,
  internal.Edestaddrreq,
  internal.Ehostdown,
  internal.Ehostunreach,
  internal.Einprogress,
  internal.Eisconn,
  internal.Emsgsize,
  internal.Enetdown,
  internal.Enetunreach,
  internal.Enopkg,
  internal.Enoprotoopt,
  internal.Enotconn,
  internal.Enotty,
  internal.Enotsock,
  internal.Eproto,
  internal.Eprotonosupport,
  internal.Eprototype,
  internal.Esocktnosupport,
  internal.Etimedout,
  internal.Ewouldblock,
  internal.Exbadport,
  internal.Exbadseq,
  internal.Nxdomain,
  internal.Eacces,
  internal.Eagain,
  internal.Ebadf,
  internal.Ebadmsg,
  internal.Ebusy,
  internal.Edeadlk,
  internal.Edeadlock,
  internal.Edquot,
  internal.Eexist,
  internal.Efault,
  internal.Efbig,
  internal.Eftype,
  internal.Eintr,
  internal.Einval,
  internal.Eio,
  internal.Eisdir,
  internal.Eloop,
  internal.Emfile,
  internal.Emlink,
  internal.Emultihop,
  internal.Enametoolong,
  internal.Enfile,
  internal.Enobufs,
  internal.Enodev,
  internal.Enolck,
  internal.Enolink,
  internal.Enoent,
  internal.Enomem,
  internal.Enospc,
  internal.Enosr,
  internal.Enostr,
  internal.Enosys,
  internal.Enotblk,
  internal.Enotdir,
  internal.Enotsup,
  internal.Enxio,
  internal.Eopnotsupp,
  internal.Eoverflow,
  internal.Eperm,
  internal.Epipe,
  internal.Erange,
  internal.Erofs,
  internal.Espipe,
  internal.Esrch,
  internal.Estale,
  internal.Etxtbsy,
  internal.Exdev,
]
