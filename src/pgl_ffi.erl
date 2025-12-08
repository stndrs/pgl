-module(pgl_ffi).

-export([
  gen_tcp_listen/1,
  gen_tcp_connect/2,
  gen_tcp_send/2,
  gen_tcp_recv/3,
  gen_tcp_shutdown/1,
  ssl_connect/3,
  ssl_send/2,
  ssl_recv/3,
  ssl_shutdown/1,
  ets_new/1,
  ets_insert/3,
  ets_lookup/2,
  binary_match/2,
  unique_int/0
]).

%%% SSL connection %%%

ssl_connect(Sock, Host, Verified) ->
  ssl:start(),

  Opts = case Verified of
    false -> [{verify, verify_none}];
    true -> [
      {verify, verify_peer},
      {cacerts, public_key:cacerts_get()},
      {server_name_idication, Host},
      {customize_hostname_check, [
        {match_fun, public_key:pkix_verify_hostname_match_fun(https)}
      ]
    }]
  end,

  ssl:connect(Sock, [binary, {packet, raw}, {active, false} | Opts]).

ssl_shutdown(Socket) ->
  Shut = ssl:shutdown(Socket, read_write),
  normalise(Shut).

ssl_recv(Socket, Size, Timeout) ->
  Resp = ssl:recv(Socket, Size, Timeout),
  normalise(Resp).

ssl_send(Socket, Packet) ->
    Sent = ssl:send(Socket, Packet),
    normalise(Sent).

%%% TCP connection %%%

gen_tcp_connect(Host, Port) ->
  Connected = gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}]),
  Connected.

gen_tcp_shutdown(Socket) ->
  Shut = gen_tcp:shutdown(Socket, read_write),
  normalise(Shut).

gen_tcp_recv(Socket, Size, Timeout) ->
  Resp = gen_tcp:recv(Socket, Size, Timeout),
  normalise(Resp).

gen_tcp_send(Socket, Packet) ->
    Sent = gen_tcp:send(Socket, Packet),
    normalise(Sent).

gen_tcp_listen(Port) ->
  Options = [
    {ip, {127,0,0,1}},
    {packet, 0},
    {active, false},
    {reuseaddr, true}
  ],
  gen_tcp:listen(Port, Options).

%%% Normalise results %%%

normalise(ok) -> {ok, nil};
normalise({ok, T}) -> {ok, T};
normalise({error, {timeout, _}}) -> {error, timeout};
normalise({error, _} = E) -> E.

%%% ETS %%%

ets_new(Name) ->
  ets:new(Name, [named_table, {read_concurrency, true}]).

ets_insert(Name, Key, Value) ->
  try
    ets:insert(Name, {Key, Value}),

    {ok, {Key, Value}}
  catch
    error:badarg ->
      {error, nil}
  end.

ets_lookup(Name, Key) ->
  try
    Objects = ets:lookup(Name, Key),

    {ok, Objects}
  catch
    error:badarg ->
      {error, nil}
  end.

%%% Helper functions %%%

binary_match(Binary, Pattern) ->
  case binary:match(Binary, Pattern) of
    nomatch -> none;
    {Start, Length} -> {some, {Start, Length}}
  end.

unique_int() -> erlang:unique_integer([positive]).
