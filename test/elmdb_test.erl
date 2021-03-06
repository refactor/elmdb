-module(elmdb_test).

-include_lib("eunit/include/eunit.hrl").

startup() ->
    Dir = "./mytestdb1/",
    ?debugFmt("startup.......... @dir=~ts", [Dir]),
    filelib:ensure_dir(Dir),
    D = elmdb:open(Dir),
    D.

teardown(D) ->
    [elmdb:drop(D, L) || L <- elmdb:ls(D)],
    Dir0 = elmdb:dispose(D),
    Dir = "./mytestdb1/",
    ?debugFmt("teardown.......... @path=~ts", [Dir0]),
    ok.


list_db(D) ->
    ?debugMsg("list......"),
    PI = <<"3.14159">>,
    E = <<"2.71828">>,
    
    elmdb:put(D, {<<"math">>, <<"pi">>}, PI),
    elmdb:put(D, {"math", <<"e">>},  E),
    [?_assertEqual(2, elmdb:count(D, "math")),
     ?_assertEqual(PI, elmdb:get(D, {"math", <<"pi">>})),
     ?_assertEqual(E, elmdb:get(D, {<<"math">>, <<"e">>}))].

count_db(D) ->
    ?debugMsg("count......"),
    [?_assertEqual(2, elmdb:count(D, "math")),
     ?_assertEqual(0, elmdb:count(D, "nonsense"))].

del_db(D) ->
    ?debugMsg("del......"),
    elmdb:put(D, {"math", <<"PI">>}, <<"3.14">>),
    elmdb:del(D, {"math", <<"PI">>}),
    [?_assertEqual({error,notfound}, elmdb:get(D, {"math", <<"PI">>}))].
    %[?_assertError(notfound, elmdb:get(D, {"math", <<"PI">>}))].

db_test(D) ->
    [list_db(D), 
     count_db(D)
    , del_db(D)
    ].

list_test_() ->
    [{"Try to list all dbs", {setup, fun startup/0, fun teardown/1, fun db_test/1}}].
