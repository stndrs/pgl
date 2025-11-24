import gleam/dict.{type Dict}
import gleam/result

pub const oid_max = 0xFFFFFFFF

pub const int2_min = 0x8000

pub const int2_max = 0x7FFF

pub const int4_min = 0x80000000

pub const int4_max = 0x7FFFFFFF

pub const int8_max = 0x7FFFFFFFFFFFFFFF

pub const int8_min = 0x8000000000000000

// Seconds between Jan 1, 0001 and Dec 31, 1999
pub const postgres_gs_epoch = 63_113_904_000

// Seconds between Jan 1, 0001 and Jan 1, 1970
pub const gs_to_unix_epoch = 62_167_219_200

// Days between Jan 1, 0001 and Dec 31, 1999
pub const postgres_gd_epoch = 730_485

pub const usecs_per_sec = 1_000_000

pub const nsecs_per_usec = 1000

pub fn to_microseconds(
  kind: a,
  to_seconds_and_nanoseconds: fn(a) -> #(Int, Int),
) -> Int {
  let #(seconds, nanoseconds) = to_seconds_and_nanoseconds(kind)

  { seconds * usecs_per_sec } + { nanoseconds / nsecs_per_usec }
}

// ---------- Messages ---------- //

pub const header_size = 5

pub type Message {
  AuthenticationOk
  AuthenticationKerberosV5
  AuthenticationCleartextPassword
  AuthenticationMD5Password(salt: BitArray)
  AuthenticationGSS
  AuthenticationGSSContinue(data: BitArray)
  AuthenticationSSPI
  AuthenticationSCM
  AuthenticationSASL(methods: List(String))
  AuthenticationSASLContinue(server_first: BitArray)
  AuthenticationSASLFinal(server_final: BitArray)
  BackendKeyData(proc_id: Int, secret: Int)
  BindComplete
  CloseComplete
  CommandComplete(command: Command, rows: Int)
  CopyBothResponse
  CopyData(data: BitArray)
  CopyDone
  CopyInResponse
  CopyOutResponse
  DataRow(values: List(BitArray))
  EmptyQueryResponse
  ErrorResponse(fields: Dict(Field, String))
  FunctionCallResponse
  NoData
  NoticeResponse(fields: Dict(Field, String))
  NotificationResponse(proc_id: Int, channel: BitArray, payload: BitArray)
  ParameterDescription(count: Int, data_types: List(Int))
  ParameterStatus(name: String, value: String)
  ParseComplete
  PortalSuspended
  ReadyForQuery(status: Status)
  RowDescription(count: Int, fields: List(RowDescriptionField))
}

pub type Status {
  Idle
  Transaction
  Err
}

pub type PgSqlFormat {
  Text
  Binary
}

pub type RowDescriptionField {
  RowDescriptionField(
    name: String,
    table_oid: Int,
    attr_number: Int,
    data_type_oid: Int,
    data_type_size: Int,
    type_modifier: Int,
    format: PgSqlFormat,
  )
}

pub type Command {
  Select(Int)
  Insert(Int)
  Update(Int)
  Delete(Int)
  Fetch(Int)
  Move(Int)
  Copy(Int)
  Begin
  Commit
  Rollback
  Other(String)
  // verb
}

// ---------- Errors ---------- //

pub type PglError {
  // Generic package errors
  PglError(message: String)
  QueryCacheError(kind: CacheError, message: String)
  TypeCacheError(kind: CacheError, message: String)
  AuthenticationError(kind: AuthenticationError, message: String)
  SocketError(kind: SocketError, message: String)
  ScramError(kind: ScramError, message: String)
  // Errors related to working with Postgres wire protocol
  ProtocolError(kind: ProtocolError, message: String)
  // Errors from the postgres server
  PostgresError(kind: PgError)
  TypeError(kind: TypeError, message: String)
}

pub type PgError {
  PgError(
    code: String,
    name: String,
    message: String,
    fields: Dict(Field, String),
  )
}

pub fn error_to_string(err: PglError) -> String {
  case err {
    PglError(message:) -> "[PglError] " <> message
    QueryCacheError(_, msg) -> "[QueryCacheError] " <> msg
    TypeCacheError(_, msg) -> "[TypeCacheError] " <> msg
    AuthenticationError(_, msg) -> "[AuthenticationError] " <> msg
    SocketError(_, msg) -> "[SocketError] " <> msg
    ScramError(_, msg) -> "[ScramError] " <> msg
    ProtocolError(_, msg) -> "[ProtocolError] " <> msg
    PostgresError(PgError(code, name, message, _)) ->
      "[PostgresError] code:"
      <> code
      <> ", name: "
      <> name
      <> ", message: "
      <> message
    TypeError(_, msg) -> "[TypeError] " <> msg
  }
}

// https://www.postgresql.org/docs/current/protocol-error-fields.html
/// Error and notice message fields
pub type Field {
  Severity
  Code
  Message
  Detail
  Hint
  Position
  InternalPosition
  InternalQuery
  Where
  Schema
  Table
  Column
  DataType
  Constraint
  File
  Line
  Routine
  Unknown(code: BitArray)
}

pub fn field_to_string(field: Field) -> String {
  case field {
    Severity -> "Severity"
    Code -> "Code"
    Message -> "Message"
    Detail -> "Detail"
    Hint -> "Hint"
    Position -> "Position"
    InternalPosition -> "InternalPosition"
    InternalQuery -> "InternalQuery"
    Where -> "Where"
    Schema -> "Schema"
    Table -> "Table"
    Column -> "Column"
    DataType -> "DataType"
    Constraint -> "Constraint"
    File -> "File"
    Line -> "Line"
    Routine -> "Routine"
    Unknown(code: _) -> "Unknown"
  }
}

pub type CacheError {
  LoadError
  CacheStartError
  NotFoundError
}

pub type AuthenticationError {
  UnsupportedSASLMethod(method: BitArray)
  AuthenticationFailed(cause: PglError)
  SignatureMismatch
}

pub fn authentication_failed(cause: PglError, message: String) -> PglError {
  AuthenticationError(kind: AuthenticationFailed(cause:), message:)
}

pub fn signature_mismatch(message: String) -> PglError {
  AuthenticationError(kind: SignatureMismatch, message:)
}

pub type ScramError {
  ServerFirst
  ServerFinal
  ServerContinue
}

pub fn server_first_error(message: String) -> PglError {
  ScramError(kind: ServerFirst, message:)
}

pub fn server_final_error(message: String) -> PglError {
  ScramError(kind: ServerFinal, message:)
}

pub fn server_continue_error(message: String) -> PglError {
  ScramError(kind: ServerContinue, message:)
}

pub type SocketError {
  SocketConfigurationError
  SendError(code: PosixError)
  ReceiveError(code: PosixError)
  ConnectError(code: PosixError)
  ShutdownError(code: PosixError)
  SslError
}

pub type ProtocolError {
  DecodingError
  EncodingError
  MessageError
}

pub fn decode_error(message: String) -> PglError {
  ProtocolError(kind: DecodingError, message:)
}

pub fn encode_error(message: String) -> PglError {
  ProtocolError(kind: EncodingError, message:)
}

pub fn message_error(message: String) -> PglError {
  ProtocolError(kind: MessageError, message:)
}

pub fn from_response_fields(fields: Dict(Field, String)) -> PgError {
  let code = dict.get(fields, Code) |> result.unwrap("")
  let message = dict.get(fields, Message) |> result.unwrap("")
  let name = pg_error_code_name(code) |> result.unwrap("")

  PgError(code:, name:, message:, fields:)
}

pub type TransactionError(error) {
  RollbackError(cause: error)
  NotInTransaction(message: String)
  FailedTransaction(message: String, cause: PglError)
  TransactionError
}

pub fn rollback(cause: error) -> TransactionError(error) {
  RollbackError(cause:)
}

pub fn not_in_transaction(message: String) -> TransactionError(error) {
  NotInTransaction(message:)
}

pub fn failed_transaction(
  message: String,
  cause: PglError,
) -> TransactionError(error) {
  FailedTransaction(message:, cause:)
}

pub type TypeError {
  TypeEncodeError
  TypeDecodeError
}

pub fn type_encode_error(message: String) -> PglError {
  TypeError(kind: TypeEncodeError, message:)
}

pub fn type_decode_error(message: String) -> PglError {
  TypeError(kind: TypeDecodeError, message:)
}

// https://www.erlang.org/doc/apps/kernel/inet.html#module-posix-error-codes
pub type PosixError {
  Closed
  Timeout
  Eaddrinuse
  Eaddrnotavail
  Eafnosupport
  Ealready
  Econnaborted
  Econnrefused
  Econnreset
  Edestaddrreq
  Ehostdown
  Ehostunreach
  Einprogress
  Eisconn
  Emsgsize
  Enetdown
  Enetunreach
  Enopkg
  Enoprotoopt
  Enotconn
  Enotty
  Enotsock
  Eproto
  Eprotonosupport
  Eprototype
  Esocktnosupport
  Etimedout
  Ewouldblock
  Exbadport
  Exbadseq
  Nxdomain
  Eacces
  Eagain
  Ebadf
  Ebadmsg
  Ebusy
  Edeadlk
  Edeadlock
  Edquot
  Eexist
  Efault
  Efbig
  Eftype
  Eintr
  Einval
  Eio
  Eisdir
  Eloop
  Emfile
  Emlink
  Emultihop
  Enametoolong
  Enfile
  Enobufs
  Enodev
  Enolck
  Enolink
  Enoent
  Enomem
  Enospc
  Enosr
  Enostr
  Enosys
  Enotblk
  Enotdir
  Enotsup
  Enxio
  Eopnotsupp
  Eoverflow
  Eperm
  Epipe
  Erange
  Erofs
  Espipe
  Esrch
  Estale
  Etxtbsy
  Exdev
}

pub fn posix_error_to_string(code: PosixError) -> String {
  case code {
    Closed -> "closed"
    Timeout -> "timeout"
    Eaddrinuse -> "eaddrinuse"
    Eaddrnotavail -> "eaddrnotavail"
    Eafnosupport -> "eafnosupport"
    Ealready -> "ealready"
    Econnaborted -> "econnaborted"
    Econnrefused -> "econnrefused"
    Econnreset -> "econnreset"
    Edestaddrreq -> "edestaddrreq"
    Ehostdown -> "ehostdown"
    Ehostunreach -> "ehostunreach"
    Einprogress -> "einprogress"
    Eisconn -> "eisconn"
    Emsgsize -> "emsgsize"
    Enetdown -> "enetdown"
    Enetunreach -> "enetunreach"
    Enopkg -> "enopkg"
    Enoprotoopt -> "enoprotoopt"
    Enotconn -> "enotconn"
    Enotty -> "enotty"
    Enotsock -> "enotsock"
    Eproto -> "eproto"
    Eprotonosupport -> "eprotonosupport"
    Eprototype -> "eprototype"
    Esocktnosupport -> "esocktnosupport"
    Etimedout -> "etimedout"
    Ewouldblock -> "ewouldblock"
    Exbadport -> "exbadport"
    Exbadseq -> "exbadseq"
    Nxdomain -> "nxdomain"
    Eacces -> "eacces"
    Eagain -> "eagain"
    Ebadf -> "ebadf"
    Ebadmsg -> "ebadmsg"
    Ebusy -> "ebusy"
    Edeadlk -> "edeadlk"
    Edeadlock -> "edeadlock"
    Edquot -> "edquot"
    Eexist -> "eexist"
    Efault -> "efault"
    Efbig -> "efbig"
    Eftype -> "eftype"
    Eintr -> "eintr"
    Einval -> "einval"
    Eio -> "eio"
    Eisdir -> "eisdir"
    Eloop -> "eloop"
    Emfile -> "emfile"
    Emlink -> "emlink"
    Emultihop -> "emultihop"
    Enametoolong -> "enametoolong"
    Enfile -> "enfile"
    Enobufs -> "enobufs"
    Enodev -> "enodev"
    Enolck -> "enolck"
    Enolink -> "enolink"
    Enoent -> "enoent"
    Enomem -> "enomem"
    Enospc -> "enospc"
    Enosr -> "enosr"
    Enostr -> "enostr"
    Enosys -> "enosys"
    Enotblk -> "enotblk"
    Enotdir -> "enotdir"
    Enotsup -> "enotsup"
    Enxio -> "enxio"
    Eopnotsupp -> "eopnotsupp"
    Eoverflow -> "eoverflow"
    Eperm -> "eperm"
    Epipe -> "epipe"
    Erange -> "erange"
    Erofs -> "erofs"
    Espipe -> "espipe"
    Esrch -> "esrch"
    Estale -> "estale"
    Etxtbsy -> "etxtbsy"
    Exdev -> "exdev"
  }
}

/// https://www.postgresql.org/docs/current/errcodes-appendix.html
pub fn pg_error_code_name(error_code: String) -> Result(String, PglError) {
  case error_code {
    "00000" -> Ok("successful_completion")
    "01000" -> Ok("warning")
    "0100C" -> Ok("dynamic_result_sets_returned")
    "01008" -> Ok("implicit_zero_bit_padding")
    "01003" -> Ok("null_value_eliminated_in_set_function")
    "01007" -> Ok("privilege_not_granted")
    "01006" -> Ok("privilege_not_revoked")
    "01004" -> Ok("string_data_right_truncation")
    "01P01" -> Ok("deprecated_feature")
    "02000" -> Ok("no_data")
    "02001" -> Ok("no_additional_dynamic_result_sets_returned")
    "03000" -> Ok("sql_statement_not_yet_complete")
    "08000" -> Ok("connection_exception")
    "08003" -> Ok("connection_does_not_exist")
    "08006" -> Ok("connection_failure")
    "08001" -> Ok("sqlclient_unable_to_establish_sqlconnection")
    "08004" -> Ok("sqlserver_rejected_establishment_of_sqlconnection")
    "08007" -> Ok("transaction_resolution_unknown")
    "08P01" -> Ok("protocol_violation")
    "09000" -> Ok("triggered_action_exception")
    "0A000" -> Ok("feature_not_supported")
    "0B000" -> Ok("invalid_transaction_initiation")
    "0F000" -> Ok("locator_exception")
    "0F001" -> Ok("invalid_locator_specification")
    "0L000" -> Ok("invalid_grantor")
    "0LP01" -> Ok("invalid_grant_operation")
    "0P000" -> Ok("invalid_role_specification")
    "0Z000" -> Ok("diagnostics_exception")
    "0Z002" -> Ok("stacked_diagnostics_accessed_without_active_query")
    "20000" -> Ok("case_not_found")
    "21000" -> Ok("cardinality_violation")
    "22000" -> Ok("data_exception")
    "2202E" -> Ok("array_subscript_error")
    "22021" -> Ok("character_not_in_repertoire")
    "22008" -> Ok("datetime_field_overflow")
    "22012" -> Ok("division_by_zero")
    "22005" -> Ok("error_in_assignment")
    "2200B" -> Ok("escape_character_conflict")
    "22022" -> Ok("indicator_overflow")
    "22015" -> Ok("interval_field_overflow")
    "2201E" -> Ok("invalid_argument_for_logarithm")
    "22014" -> Ok("invalid_argument_for_ntile_function")
    "22016" -> Ok("invalid_argument_for_nth_value_function")
    "2201F" -> Ok("invalid_argument_for_power_function")
    "2201G" -> Ok("invalid_argument_for_width_bucket_function")
    "22018" -> Ok("invalid_character_value_for_cast")
    "22007" -> Ok("invalid_datetime_format")
    "22019" -> Ok("invalid_escape_character")
    "2200D" -> Ok("invalid_escape_octet")
    "22025" -> Ok("invalid_escape_sequence")
    "22P06" -> Ok("nonstandard_use_of_escape_character")
    "22010" -> Ok("invalid_indicator_parameter_value")
    "22023" -> Ok("invalid_parameter_value")
    "22013" -> Ok("invalid_preceding_or_following_size")
    "2201B" -> Ok("invalid_regular_expression")
    "2201W" -> Ok("invalid_row_count_in_limit_clause")
    "2201X" -> Ok("invalid_row_count_in_result_offset_clause")
    "2202H" -> Ok("invalid_tablesample_argument")
    "2202G" -> Ok("invalid_tablesample_repeat")
    "22009" -> Ok("invalid_time_zone_displacement_value")
    "2200C" -> Ok("invalid_use_of_escape_character")
    "2200G" -> Ok("most_specific_type_mismatch")
    "22004" -> Ok("null_value_not_allowed")
    "22002" -> Ok("null_value_no_indicator_parameter")
    "22003" -> Ok("numeric_value_out_of_range")
    "2200H" -> Ok("sequence_generator_limit_exceeded")
    "22026" -> Ok("string_data_length_mismatch")
    "22001" -> Ok("string_data_right_truncation")
    "22011" -> Ok("substring_error")
    "22027" -> Ok("trim_error")
    "22024" -> Ok("unterminated_c_string")
    "2200F" -> Ok("zero_length_character_string")
    "22P01" -> Ok("floating_point_exception")
    "22P02" -> Ok("invalid_text_representation")
    "22P03" -> Ok("invalid_binary_representation")
    "22P04" -> Ok("bad_copy_file_format")
    "22P05" -> Ok("untranslatable_character")
    "2200L" -> Ok("not_an_xml_document")
    "2200M" -> Ok("invalid_xml_document")
    "2200N" -> Ok("invalid_xml_content")
    "2200S" -> Ok("invalid_xml_comment")
    "2200T" -> Ok("invalid_xml_processing_instruction")
    "22030" -> Ok("duplicate_json_object_key_value")
    "22031" -> Ok("invalid_argument_for_sql_json_datetime_function")
    "22032" -> Ok("invalid_json_text")
    "22033" -> Ok("invalid_sql_json_subscript")
    "22034" -> Ok("more_than_one_sql_json_item")
    "22035" -> Ok("no_sql_json_item")
    "22036" -> Ok("non_numeric_sql_json_item")
    "22037" -> Ok("non_unique_keys_in_a_json_object")
    "22038" -> Ok("singleton_sql_json_item_required")
    "22039" -> Ok("sql_json_array_not_found")
    "2203A" -> Ok("sql_json_member_not_found")
    "2203B" -> Ok("sql_json_number_not_found")
    "2203C" -> Ok("sql_json_object_not_found")
    "2203D" -> Ok("too_many_json_array_elements")
    "2203E" -> Ok("too_many_json_object_members")
    "2203F" -> Ok("sql_json_scalar_required")
    "23000" -> Ok("integrity_constraint_violation")
    "23001" -> Ok("restrict_violation")
    "23502" -> Ok("not_null_violation")
    "23503" -> Ok("foreign_key_violation")
    "23505" -> Ok("unique_violation")
    "23514" -> Ok("check_violation")
    "23P01" -> Ok("exclusion_violation")
    "24000" -> Ok("invalid_cursor_state")
    "25000" -> Ok("invalid_transaction_state")
    "25001" -> Ok("active_sql_transaction")
    "25002" -> Ok("branch_transaction_already_active")
    "25008" -> Ok("held_cursor_requires_same_isolation_level")
    "25003" -> Ok("inappropriate_access_mode_for_branch_transaction")
    "25004" -> Ok("inappropriate_isolation_level_for_branch_transaction")
    "25005" -> Ok("no_active_sql_transaction_for_branch_transaction")
    "25006" -> Ok("read_only_sql_transaction")
    "25007" -> Ok("schema_and_data_statement_mixing_not_supported")
    "25P01" -> Ok("no_active_sql_transaction")
    "25P02" -> Ok("in_failed_sql_transaction")
    "25P03" -> Ok("idle_in_transaction_session_timeout")
    "26000" -> Ok("invalid_sql_statement_name")
    "27000" -> Ok("triggered_data_change_violation")
    "28000" -> Ok("invalid_authorization_specification")
    "28P01" -> Ok("invalid_password")
    "2B000" -> Ok("dependent_privilege_descriptors_still_exist")
    "2BP01" -> Ok("dependent_objects_still_exist")
    "2D000" -> Ok("invalid_transaction_termination")
    "2F000" -> Ok("sql_routine_exception")
    "2F005" -> Ok("function_executed_no_return_statement")
    "2F002" -> Ok("modifying_sql_data_not_permitted")
    "2F003" -> Ok("prohibited_sql_statement_attempted")
    "2F004" -> Ok("reading_sql_data_not_permitted")
    "34000" -> Ok("invalid_cursor_name")
    "38000" -> Ok("external_routine_exception")
    "38001" -> Ok("containing_sql_not_permitted")
    "38002" -> Ok("modifying_sql_data_not_permitted")
    "38003" -> Ok("prohibited_sql_statement_attempted")
    "38004" -> Ok("reading_sql_data_not_permitted")
    "39000" -> Ok("external_routine_invocation_exception")
    "39001" -> Ok("invalid_sqlstate_returned")
    "39004" -> Ok("null_value_not_allowed")
    "39P01" -> Ok("trigger_protocol_violated")
    "39P02" -> Ok("srf_protocol_violated")
    "39P03" -> Ok("event_trigger_protocol_violated")
    "3B000" -> Ok("savepoint_exception")
    "3B001" -> Ok("invalid_savepoint_specification")
    "3D000" -> Ok("invalid_catalog_name")
    "3F000" -> Ok("invalid_schema_name")
    "40000" -> Ok("transaction_rollback")
    "40002" -> Ok("transaction_integrity_constraint_violation")
    "40001" -> Ok("serialization_failure")
    "40003" -> Ok("statement_completion_unknown")
    "40P01" -> Ok("deadlock_detected")
    "42000" -> Ok("syntax_error_or_access_rule_violation")
    "42601" -> Ok("syntax_error")
    "42501" -> Ok("insufficient_privilege")
    "42846" -> Ok("cannot_coerce")
    "42803" -> Ok("grouping_error")
    "42P20" -> Ok("windowing_error")
    "42P19" -> Ok("invalid_recursion")
    "42830" -> Ok("invalid_foreign_key")
    "42602" -> Ok("invalid_name")
    "42622" -> Ok("name_too_long")
    "42939" -> Ok("reserved_name")
    "42804" -> Ok("datatype_mismatch")
    "42P18" -> Ok("indeterminate_datatype")
    "42P21" -> Ok("collation_mismatch")
    "42P22" -> Ok("indeterminate_collation")
    "42809" -> Ok("wrong_object_type")
    "428C9" -> Ok("generated_always")
    "42703" -> Ok("undefined_column")
    "42883" -> Ok("undefined_function")
    "42P01" -> Ok("undefined_table")
    "42P02" -> Ok("undefined_parameter")
    "42704" -> Ok("undefined_object")
    "42701" -> Ok("duplicate_column")
    "42P03" -> Ok("duplicate_cursor")
    "42P04" -> Ok("duplicate_database")
    "42723" -> Ok("duplicate_function")
    "42P05" -> Ok("duplicate_prepared_statement")
    "42P06" -> Ok("duplicate_schema")
    "42P07" -> Ok("duplicate_table")
    "42712" -> Ok("duplicate_alias")
    "42710" -> Ok("duplicate_object")
    "42702" -> Ok("ambiguous_column")
    "42725" -> Ok("ambiguous_function")
    "42P08" -> Ok("ambiguous_parameter")
    "42P09" -> Ok("ambiguous_alias")
    "42P10" -> Ok("invalid_column_reference")
    "42611" -> Ok("invalid_column_definition")
    "42P11" -> Ok("invalid_cursor_definition")
    "42P12" -> Ok("invalid_database_definition")
    "42P13" -> Ok("invalid_function_definition")
    "42P14" -> Ok("invalid_prepared_statement_definition")
    "42P15" -> Ok("invalid_schema_definition")
    "42P16" -> Ok("invalid_table_definition")
    "42P17" -> Ok("invalid_object_definition")
    "44000" -> Ok("with_check_option_violation")
    "53000" -> Ok("insufficient_resources")
    "53100" -> Ok("disk_full")
    "53200" -> Ok("out_of_memory")
    "53300" -> Ok("too_many_connections")
    "53400" -> Ok("configuration_limit_exceeded")
    "54000" -> Ok("program_limit_exceeded")
    "54001" -> Ok("statement_too_complex")
    "54011" -> Ok("too_many_columns")
    "54023" -> Ok("too_many_arguments")
    "55000" -> Ok("object_not_in_prerequisite_state")
    "55006" -> Ok("object_in_use")
    "55P02" -> Ok("cant_change_runtime_param")
    "55P03" -> Ok("lock_not_available")
    "55P04" -> Ok("unsafe_new_enum_value_usage")
    "57000" -> Ok("operator_intervention")
    "57014" -> Ok("query_canceled")
    "57P01" -> Ok("admin_shutdown")
    "57P02" -> Ok("crash_shutdown")
    "57P03" -> Ok("cannot_connect_now")
    "57P04" -> Ok("database_dropped")
    "57P05" -> Ok("idle_session_timeout")
    "58000" -> Ok("system_error")
    "58030" -> Ok("io_error")
    "58P01" -> Ok("undefined_file")
    "58P02" -> Ok("duplicate_file")
    "72000" -> Ok("snapshot_too_old")
    "F0000" -> Ok("config_file_error")
    "F0001" -> Ok("lock_file_exists")
    "HV000" -> Ok("fdw_error")
    "HV005" -> Ok("fdw_column_name_not_found")
    "HV002" -> Ok("fdw_dynamic_parameter_value_needed")
    "HV010" -> Ok("fdw_function_sequence_error")
    "HV021" -> Ok("fdw_inconsistent_descriptor_information")
    "HV024" -> Ok("fdw_invalid_attribute_value")
    "HV007" -> Ok("fdw_invalid_column_name")
    "HV008" -> Ok("fdw_invalid_column_number")
    "HV004" -> Ok("fdw_invalid_data_type")
    "HV006" -> Ok("fdw_invalid_data_type_descriptors")
    "HV091" -> Ok("fdw_invalid_descriptor_field_identifier")
    "HV00B" -> Ok("fdw_invalid_handle")
    "HV00C" -> Ok("fdw_invalid_option_index")
    "HV00D" -> Ok("fdw_invalid_option_name")
    "HV090" -> Ok("fdw_invalid_string_length_or_buffer_length")
    "HV00A" -> Ok("fdw_invalid_string_format")
    "HV009" -> Ok("fdw_invalid_use_of_null_pointer")
    "HV014" -> Ok("fdw_too_many_handles")
    "HV001" -> Ok("fdw_out_of_memory")
    "HV00P" -> Ok("fdw_no_schemas")
    "HV00J" -> Ok("fdw_option_name_not_found")
    "HV00K" -> Ok("fdw_reply_handle")
    "HV00Q" -> Ok("fdw_schema_not_found")
    "HV00R" -> Ok("fdw_table_not_found")
    "HV00L" -> Ok("fdw_unable_to_create_execution")
    "HV00M" -> Ok("fdw_unable_to_create_reply")
    "HV00N" -> Ok("fdw_unable_to_establish_connection")
    "P0000" -> Ok("plpgsql_error")
    "P0001" -> Ok("raise_exception")
    "P0002" -> Ok("no_data_found")
    "P0003" -> Ok("too_many_rows")
    "P0004" -> Ok("assert_failure")
    "XX000" -> Ok("internal_error")
    "XX001" -> Ok("data_corrupted")
    "XX002" -> Ok("index_corrupted")
    _ -> Error(PglError("PG SQL Error code not found: " <> error_code))
  }
}
