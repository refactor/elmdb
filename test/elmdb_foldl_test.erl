-module(elmdb_foldl_test).

-include_lib("eunit/include/eunit.hrl").

startup() ->
    ?debugMsg("startup.........."),
    Dir = "./mytestdb2/",
    filelib:ensure_dir(Dir),
    D = elmdb:open(Dir),
    D.

teardown(D) ->
    ?debugFmt("drop all tables of ~p",[D]),
%    [elmdb:drop(D, L) || L <- elmdb:ls(D)],
    %%ok = elmdb:close(D),
    ?debugFmt("dispose db: ~p", [D]),
    elmdb:dispose(D),
    ok.

fold_db(D) ->
    {inparallel,
    [?_test(fold_normal_db(D))
     , ?_test(fold_intkey_db(D))
    ]
    }
    .

fold_intkey_db(D) ->
    ?debugFmt("fold_intkey_db........~w", [D]),
    Layer = "accum-intkey",
    I1 = <<1:32/integer>>,
    I2 = <<2:32/integer>>,
    I3 = <<3:32/integer>>,
    elmdb:put(D, {Layer, 1}, I1),
    elmdb:put(D, {Layer, 2}, I2),
    elmdb:put(D, {Layer, 3}, I3),
    Fn = fun({K,V}, Acc) ->
                 ?debugFmt("K = ~p",[K]),
                 <<I:32/integer>> = V,
                 I + Acc end,
    ?debugFmt("Fn ~w~n", [Fn]),
    %Res = elmdb:foldl(D, Layer, 0, Fn),
    ?debugMsg("after fold!"),
    %?_assertEqual(6, Res).
    ?assertEqual(6, elmdb:foldl(D, Layer, 0, Fn)).

fold_normal_db(D) ->
    ?debugFmt("fold_normal_db........~w", [D]),
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
    ?debugFmt("Fn ~w~n", [Fn]),
    %Res = elmdb:foldl(D, Layer, 0, Fn),
    ?debugMsg("after fold!"),
    %?_assertEqual(6, Res).
    ?assertEqual(6, elmdb:foldl(D, Layer, 0, Fn)).

travel_test_() ->
    [{"Try to foldl all elements",
      {setup, fun startup/0, fun teardown/1, fun fold_db/1}}].
