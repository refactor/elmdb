-module(elmdb_empty_test).

-include_lib("eunit/include/eunit.hrl").

startup() ->
    Dir = "./myfoodb1/",
    ?debugFmt("startup.......... @dir=~ts", [Dir]),
    filelib:ensure_dir(Dir),
    D = elmdb:open(Dir),
    elmdb:put(D, {"accum-str", <<"123">>}, <<1:32/integer>>),
    elmdb:del(D, {"accum-str", <<"123">>}),
    elmdb:put(D, {"accum-int", 123}, <<1:32/integer>>),
    elmdb:del(D, {"accum-int", 123}),
    D.

teardown(D) ->
    [elmdb:drop(D, L) || L <- elmdb:ls(D)],
    ok = elmdb:dispose(D),
    {error, _} = elmdb:dispose(D),
    ok.

count_db(D) ->
    ?debugMsg("count......"),
    ?assertEqual(0, lists:sum([elmdb:count(D, L) || L <- elmdb:ls(D)])),
    ?assertEqual(0, elmdb:count(D, "nonsense")).

fold_db(D) ->
    ?debugFmt("fold_normal_db........~w", [D]),
    Fn = fun({K,V}, Acc) ->
                 ?debugFmt("K = ~p",[K]),
                 <<I:32/integer>> = V,
                 I + Acc end,
    ?debugFmt("Fn ~w~n", [Fn]),
    %Res = elmdb:foldl(D, Layer, 0, Fn),
    ?debugMsg("after fold!"),
    %?_assertEqual(6, Res).
    ?assertEqual(0, elmdb:foldl(D, "accum-str", 0, Fn)),
    ?assertEqual(0, elmdb:foldl(D, "accum-int", 0, Fn)).

empty_test(D) ->
    [?_test(count_db(D)),
     ?_test(fold_db(D))
    ].

empty_test_() ->
    [{"Try to list empty dbs", {setup, fun startup/0, fun teardown/1, fun empty_test/1}}].
