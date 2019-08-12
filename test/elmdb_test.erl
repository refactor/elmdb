-module(elmdb_test).

-include_lib("eunit/include/eunit.hrl").

startup() ->
    Dir = "./mytestdb1/",
    filelib:ensure_dir(Dir),
    D = elmdb:init(Dir),
    D.

teardown(_) ->
    Dir = "./mytestdb1/",
    [file:delete(F) || F <- filelib:wildcard(Dir ++ "*")],
    file:del_dir(Dir).


list_db(D) ->
    PI = <<"3.14159">>,
    E = <<"2.71828">>,
    elmdb:put(D, {"math", <<"pi">>}, PI),
    elmdb:put(D, {"math", <<"e">>},  E),
    [?_assertEqual(PI, elmdb:get(D, {"math", <<"pi">>})),
     ?_assertEqual(E, elmdb:get(D, {"math", <<"e">>}))].

list_test_() ->
    [{"Try to list all dbs", {setup, fun startup/0, fun teardown/1, fun list_db/1}}].
