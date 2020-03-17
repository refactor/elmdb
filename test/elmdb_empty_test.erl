-module(elmdb_empty_test).

-include_lib("eunit/include/eunit.hrl").

startup() ->
    Dir = "./myfoodb1/",
    ?debugFmt("startup.......... @dir=~ts", [Dir]),
    filelib:ensure_dir(Dir),
    D = elmdb:open(Dir),
    D.

teardown(D) ->
    [elmdb:drop(D, L) || L <- elmdb:ls(D)],
    ok = elmdb:dispose(D),
    {error, _} = elmdb:dispose(D),
    ok.

count_db(D) ->
    ?debugMsg("count......"),
    [?_assertEqual(0, lists:sum([elmdb:count(D, L) || L <- elmdb:ls(D)])),
     ?_assertEqual(0, elmdb:count(D, "nonsense"))].

empty_test(D) ->
    [count_db(D)
    ].

empty_test_() ->
    [{"Try to list empty dbs", {setup, fun startup/0, fun teardown/1, fun empty_test/1}}].
