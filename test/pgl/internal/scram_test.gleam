import pgl/internal/scram

pub fn client_first_test() {
  let nonce = scram.get_nonce(16)

  let expected = <<"n,,n=username,r=":utf8, nonce:bits>>

  let bits = scram.client_first(<<"username":utf8>>, <<nonce:bits>>)

  assert expected == bits
}
