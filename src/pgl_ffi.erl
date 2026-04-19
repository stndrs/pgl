-module(pgl_ffi).

-export([
  rescue/1,
  handle_crash/2,
  binary_match/2
]).

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

binary_match(Binary, Pattern) ->
  case binary:match(Binary, Pattern) of
    nomatch -> {error, nil};
    {Start, Length} -> {ok, {Start, Length}}
  end.
