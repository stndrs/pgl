-module(pgl_ffi).

-export([
  gen_tcp_listen/1,
  gen_tcp_listen_ipv6/1,
  gen_tcp_connect/3,
  gen_tcp_send/2,
  gen_tcp_recv/3,
  gen_tcp_recv_infinity/2,
  gen_tcp_shutdown/1,
  ssl_connect/3,
  ssl_send/2,
  ssl_recv/3,
  ssl_shutdown/1,
  ets_new/1,
  ets_insert/3,
  ets_lookup/2,
  ets_delete/1,
  ets_delete/2,
  rescue/1,
  handle_crash/2,
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

gen_tcp_connect(Host, Port, UseInet6) ->
  Inet = case UseInet6 of
    true -> inet6;
    false -> inet
  end,

  gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}, Inet]).

gen_tcp_shutdown(Socket) ->
  Shut = gen_tcp:shutdown(Socket, read_write),
  normalise(Shut).

gen_tcp_recv(Socket, Size, Timeout) ->
  Resp = gen_tcp:recv(Socket, Size, Timeout),
  normalise(Resp).

gen_tcp_recv_infinity(Socket, Size) ->
  Resp = gen_tcp:recv(Socket, Size),
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

gen_tcp_listen_ipv6(Port) ->
  Options = [
    {ip, {0,0,0,0,0,0,0,1}},
    {packet, 0},
    {active, false},
    {reuseaddr, true},
    inet6
  ],
  gen_tcp:listen(Port, Options).

%%% Normalise results %%%

normalise(ok) -> {ok, nil};
normalise({ok, T}) -> {ok, T};
normalise({error, {timeout, _}}) -> {error, timeout};
normalise({error, _} = E) -> E.

%%% ETS %%%

ets_new(Name) ->
  ets:new(Name, [named_table, private]).

ets_insert(Name, Key, Value) ->
  with_rescue(fun() ->
    ets:insert(Name, {Key, Value}),

    {ok, nil}
  end).

ets_lookup(Name, Key) ->
  with_rescue(fun() ->
    case ets:lookup(Name, Key) of
      '$end_of_table' -> {error, nil};
      [{_Key, Value}] -> {ok, Value};
      [] -> {error, nil}
    end
  end).

ets_delete(Name) ->
  with_rescue(fun() ->
    ets:delete(Name),

    {ok, nil}
  end).

ets_delete(Name, Key) ->
  with_rescue(fun() ->
    ets:delete(Name, Key),

    {ok, nil}
  end).

%%% Exception handling %%%

with_rescue(Fun) ->
  try Fun()
  catch error:badarg -> {error, nil}
  end.

rescue(Fun) ->
  try {ok, Fun()}
  catch _:_:_ -> {error, nil}
  end.

handle_crash(Handler, Fun) ->
  try Fun()
  catch Class:Reason:Stacktrace ->
    Handler(),
    erlang:raise(Class, Reason, Stacktrace)
  end.

%%% Helper functions %%%

binary_match(Binary, Pattern) ->
  case binary:match(Binary, Pattern) of
    nomatch -> {error, nil};
    {Start, Length} -> {ok, {Start, Length}}
  end.

unique_int() -> erlang:unique_integer([positive]).
