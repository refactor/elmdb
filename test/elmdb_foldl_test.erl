-module(elmdb_foldl_test).

-include_lib("eunit/include/eunit.hrl").

startup() ->
    ?debugMsg("startup.........."),
    Dir = "./mytestdb2/",
    filelib:ensure_dir(Dir),
    D = elmdb:open(Dir),
    D.

teardown(D) ->
    [elmdb:drop(D, L) || L <- elmdb:ls(D)],
    %%ok = elmdb:close(D),

    elmdb:dispose(D).

fold_db(D) ->
    [fold_normal_db(D), fold_empty_db(D)].

fold_normal_db(D) ->
    ?debugMsg("fold......"),
    Layer = "accum",
    I1 = <<1:32/integer>>,
    I2 = <<2:32/integer>>,
    I3 = <<3:32/integer>>,
    elmdb:put(D, {Layer, <<"i1">>}, I1),
    elmdb:put(D, {Layer, <<"i2">>}, I2),
    elmdb:put(D, {Layer, <<"i3">>}, I3),
    Fn = fun({K,V}, Acc) ->
                 ?debugFmt("K = ~p",[K]),
                 <<I:32/integer>> = V,
                 I + Acc end,
    [?_assertEqual(6, elmdb:foldl(D, Layer, 0, Fn))].
fold_empty_db(D) ->
    Layer = "empty",
    elmdb:put(D, {Layer, <<"i1">>}, <<1:32/integer>>),
    elmdb:del(D, {Layer, <<"i1">>}),
    Fn = fun({_K,V}, Acc) ->
                 <<I:32/integer>> = V,
                 I + Acc end,
    [?_assertEqual(0, elmdb:foldl(D, Layer, 0, Fn))].

travel_test_() ->
    [{"Try to foldl all elements",
      {setup, fun startup/0, fun teardown/1, fun fold_db/1}}].
