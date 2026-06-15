import gleam/bit_array
import pgl/internal
import pgl/internal/scram

// ---------- get_nonce ---------- //

pub fn get_nonce_returns_bit_array_test() {
  let nonce = scram.get_nonce(16)
  assert bit_array.byte_size(nonce) == 24
}

pub fn get_nonce_is_unique_test() {
  let n1 = scram.get_nonce(16)
  let n2 = scram.get_nonce(16)
  assert n1 != n2
}

// ---------- client_first ---------- //

pub fn client_first_test() {
  let nonce = scram.get_nonce(16)
  let expected = <<"n,,n=username,r=":utf8, nonce:bits>>

  let assert Ok(bits) = scram.client_first(<<"username":utf8>>, nonce)

  assert expected == bits
}

pub fn client_first_escapes_comma_test() {
  let nonce = <<"testnonce":utf8>>

  let assert Ok(bits) = scram.client_first(<<"user,name":utf8>>, nonce)

  assert <<"n,,n=user=2Cname,r=testnonce":utf8>> == bits
}

pub fn client_first_escapes_equals_test() {
  let nonce = <<"testnonce":utf8>>

  let assert Ok(bits) = scram.client_first(<<"user=name":utf8>>, nonce)

  assert <<"n,,n=user=3Dname,r=testnonce":utf8>> == bits
}

pub fn client_first_escapes_comma_and_equals_test() {
  let nonce = <<"testnonce":utf8>>

  let assert Ok(bits) = scram.client_first(<<"a=b,c":utf8>>, nonce)

  assert <<"n,,n=a=3Db=2Cc,r=testnonce":utf8>> == bits
}

pub fn client_first_no_double_escape_test() {
  let nonce = <<"testnonce":utf8>>

  let assert Ok(bits) = scram.client_first(<<"=2C":utf8>>, nonce)

  assert <<"n,,n==3D2C,r=testnonce":utf8>> == bits
}

pub fn client_first_empty_username_test() {
  let nonce = <<"testnonce":utf8>>

  let assert Ok(bits) = scram.client_first(<<"":utf8>>, nonce)

  assert <<"n,,n=,r=testnonce":utf8>> == bits
}

// ---------- parse_server_first ---------- //

pub fn parse_server_first_valid_test() {
  let client_nonce = <<"rOprNGfwEbeRWgbNEkqO":utf8>>
  let server_first = <<
    "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096":utf8,
  >>

  let assert Ok(sf) = scram.parse_server_first(server_first, client_nonce)

  assert sf.nonce
    == <<"rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0":utf8>>
  assert sf.iterations == 4096
  assert sf.raw == server_first
}

pub fn parse_server_first_nonce_prefix_mismatch_test() {
  let client_nonce = <<"wrongnonce":utf8>>
  let server_first = <<"r=rOprNGfwEbeRWgbNEkqOextended,s=c2FsdA==,i=4096":utf8>>

  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerFirst,
    message: "Failed to parse server_first",
  )) = scram.parse_server_first(server_first, client_nonce)
}

pub fn parse_server_first_iterations_too_low_test() {
  let client_nonce = <<"abc":utf8>>
  let server_first = <<"r=abcdef,s=c2FsdA==,i=100":utf8>>

  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerFirst,
    message: "Failed to parse server_first",
  )) = scram.parse_server_first(server_first, client_nonce)
}

pub fn parse_server_first_iterations_high_test() {
  let client_nonce = <<"abc":utf8>>
  let server_first = <<"r=abcdef,s=c2FsdA==,i=200000":utf8>>

  // There is no upper bound on the iteration count (libpq imposes none).
  let assert Ok(sf) = scram.parse_server_first(server_first, client_nonce)
  assert sf.iterations == 200_000
}

pub fn parse_server_first_iterations_boundary_low_test() {
  let client_nonce = <<"abc":utf8>>
  let server_first = <<"r=abcdef,s=c2FsdA==,i=4096":utf8>>

  let assert Ok(sf) = scram.parse_server_first(server_first, client_nonce)
  assert sf.iterations == 4096
}

pub fn parse_server_first_iterations_boundary_high_test() {
  let client_nonce = <<"abc":utf8>>
  let server_first = <<"r=abcdef,s=c2FsdA==,i=100000":utf8>>

  let assert Ok(sf) = scram.parse_server_first(server_first, client_nonce)
  assert sf.iterations == 100_000
}

pub fn parse_server_first_iterations_at_boundary_reject_test() {
  let client_nonce = <<"abc":utf8>>
  // Below the RFC 5802 minimum of 4096 is rejected.
  let assert Error(_) =
    scram.parse_server_first(
      <<"r=abcdef,s=c2FsdA==,i=4095":utf8>>,
      client_nonce,
    )
  // Above the old 100_000 cap is now accepted (no upper bound).
  let assert Ok(_) =
    scram.parse_server_first(
      <<"r=abcdef,s=c2FsdA==,i=100001":utf8>>,
      client_nonce,
    )
}

pub fn parse_server_first_invalid_salt_test() {
  let client_nonce = <<"abc":utf8>>
  let server_first = <<"r=abcdef,s=!!!,i=4096":utf8>>

  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerFirst,
    message: "Failed to parse server_first",
  )) = scram.parse_server_first(server_first, client_nonce)
}

pub fn parse_server_first_invalid_iterations_test() {
  let client_nonce = <<"abc":utf8>>
  let server_first = <<"r=abcdef,s=c2FsdA==,i=notanumber":utf8>>

  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerFirst,
    message: "Failed to parse server_first",
  )) = scram.parse_server_first(server_first, client_nonce)
}

pub fn parse_server_first_missing_fields_test() {
  let client_nonce = <<"abc":utf8>>
  let assert Error(_) =
    scram.parse_server_first(<<"r=abcdef,s=c2FsdA==":utf8>>, client_nonce)

  let assert Error(_) =
    scram.parse_server_first(<<"garbage":utf8>>, client_nonce)

  let assert Error(_) = scram.parse_server_first(<<"":utf8>>, client_nonce)
}

pub fn parse_server_first_nonce_shorter_than_client_test() {
  let client_nonce = <<"longclientnonce":utf8>>
  let server_first = <<"r=short,s=c2FsdA==,i=4096":utf8>>

  let assert Error(_) = scram.parse_server_first(server_first, client_nonce)
}

// ---------- parse_server_final ---------- //

pub fn parse_server_final_valid_test() {
  let sig = <<"somesignaturedata":utf8>>
  let encoded = bit_array.base64_encode(sig, True)
  let server_final = <<"v=":utf8, encoded:utf8>>

  let assert Ok(decoded) = scram.parse_server_final(server_final)

  assert decoded == sig
}

pub fn parse_server_final_rfc_vector_test() {
  // https://datatracker.ietf.org/doc/html/rfc7677#section-3
  let server_final = <<"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=":utf8>>

  let assert Ok(decoded) = scram.parse_server_final(server_final)

  assert bit_array.byte_size(decoded) == 32
}

pub fn parse_server_final_error_response_test() {
  let server_final = <<"e=invalid-proof":utf8>>

  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerError,
    message: "Server error: 'invalid-proof'",
  )) = scram.parse_server_final(server_final)
}

pub fn parse_server_final_error_other_errors_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerError,
    message: "Server error: 'channel-bindings-dont-match'",
  )) = scram.parse_server_final(<<"e=channel-bindings-dont-match":utf8>>)

  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerError,
    message: "Server error: 'unknown-user'",
  )) = scram.parse_server_final(<<"e=unknown-user":utf8>>)
}

pub fn parse_server_final_unexpected_payload_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerFinal,
    message: "Unexpected SASL server final payload",
  )) = scram.parse_server_final(<<"garbage":utf8>>)
}

pub fn parse_server_final_empty_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerFinal,
    message: "Unexpected SASL server final payload",
  )) = scram.parse_server_final(<<>>)
}

pub fn parse_server_final_invalid_base64_test() {
  let assert Error(internal.ProtocolError(
    kind: internal.SaslServerFinal,
    message: "Failed to parse server_final",
  )) = scram.parse_server_final(<<"v=not!valid!base64!!!":utf8>>)
}

// ---------- client_final (RFC 7677 test vector) ---------- //

pub fn client_final_rfc7677_test() {
  // https://datatracker.ietf.org/doc/html/rfc7677#section-3
  let username = <<"user":utf8>>
  let password = <<"pencil":utf8>>
  let client_nonce = <<"rOprNGfwEbeRWgbNEkqO":utf8>>

  let server_first_raw = <<
    "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096":utf8,
  >>

  let assert Ok(sf) = scram.parse_server_first(server_first_raw, client_nonce)

  let assert Ok(#(client_final, server_signature)) =
    scram.client_final(sf, client_nonce, username, password)

  let expected_client_final = <<
    "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=":utf8,
  >>

  assert client_final == expected_client_final

  let assert Ok(expected_server_sig) =
    bit_array.base64_decode("6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")

  assert server_signature == expected_server_sig
}

pub fn client_final_structure_test() {
  let client_nonce = <<"mynonce":utf8>>
  let salt = bit_array.base64_encode(<<"salt1234":utf8>>, True)

  let server_first_raw =
    bit_array.from_string("r=mynonceserverext,s=" <> salt <> ",i=4096")

  let assert Ok(sf) = scram.parse_server_first(server_first_raw, client_nonce)

  let assert Ok(#(client_final, _server_sig)) =
    scram.client_final(sf, client_nonce, <<"testuser":utf8>>, <<
      "testpass":utf8,
    >>)

  let assert Ok(client_final_str) = bit_array.to_string(client_final)

  let assert True = case client_final_str {
    "c=biws,r=mynonceserverext,p=" <> _ -> True
    _ -> False
  }
}

pub fn client_final_different_passwords_produce_different_proofs_test() {
  let client_nonce = <<"nonce1":utf8>>
  let salt = bit_array.base64_encode(<<"salt":utf8>>, True)
  let server_first_raw =
    bit_array.from_string("r=nonce1extended,s=" <> salt <> ",i=4096")

  let assert Ok(sf) = scram.parse_server_first(server_first_raw, client_nonce)

  let assert Ok(#(final1, sig1)) =
    scram.client_final(sf, client_nonce, <<"user":utf8>>, <<"pass1":utf8>>)

  let assert Ok(#(final2, sig2)) =
    scram.client_final(sf, client_nonce, <<"user":utf8>>, <<"pass2":utf8>>)

  assert final1 != final2
  assert sig1 != sig2
}

pub fn client_final_different_usernames_produce_different_proofs_test() {
  let client_nonce = <<"nonce1":utf8>>
  let salt = bit_array.base64_encode(<<"salt":utf8>>, True)
  let server_first_raw =
    bit_array.from_string("r=nonce1extended,s=" <> salt <> ",i=4096")

  let assert Ok(sf) = scram.parse_server_first(server_first_raw, client_nonce)

  let assert Ok(#(final1, sig1)) =
    scram.client_final(sf, client_nonce, <<"alice":utf8>>, <<"pass":utf8>>)

  let assert Ok(#(final2, sig2)) =
    scram.client_final(sf, client_nonce, <<"bob":utf8>>, <<"pass":utf8>>)

  assert final1 != final2
  assert sig1 != sig2
}

pub fn client_final_escaped_username_in_auth_message_test() {
  let client_nonce = <<"nonce1":utf8>>
  let salt = bit_array.base64_encode(<<"salt":utf8>>, True)
  let server_first_raw =
    bit_array.from_string("r=nonce1extended,s=" <> salt <> ",i=4096")

  let assert Ok(sf) = scram.parse_server_first(server_first_raw, client_nonce)

  let assert Ok(#(_final, _sig)) =
    scram.client_final(sf, client_nonce, <<"a=b,c":utf8>>, <<"pass":utf8>>)
}

// ---------- Full round-trip ---------- //

pub fn full_scram_round_trip_test() {
  // https://datatracker.ietf.org/doc/html/rfc7677#section-3

  let client_nonce = <<"rOprNGfwEbeRWgbNEkqO":utf8>>
  let username = <<"user":utf8>>
  let password = <<"pencil":utf8>>

  let assert Ok(client_first) = scram.client_first(username, client_nonce)
  assert client_first == <<"n,,n=user,r=rOprNGfwEbeRWgbNEkqO":utf8>>

  let server_first_raw = <<
    "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096":utf8,
  >>

  let assert Ok(sf) = scram.parse_server_first(server_first_raw, client_nonce)

  let assert Ok(#(client_final, server_signature)) =
    scram.client_final(sf, client_nonce, username, password)

  assert client_final
    == <<
      "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=":utf8,
    >>

  let server_final = <<"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=":utf8>>

  let assert Ok(decoded_sig) = scram.parse_server_final(server_final)

  assert decoded_sig == server_signature
}
