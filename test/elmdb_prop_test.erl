-module(elmdb_prop_test).

-export([prop_em/0]).

-include_lib("proper/include/proper.hrl").

prop_em() ->
    Dir = "./mytestdb2/",
    filelib:ensure_dir(Dir),
    D = elmdb:open(Dir),
    Layer = "my-intkey",
    ?FORALL(L, list(non_neg_integer()), 
            begin
                lists:foreach(fun(I) ->
                                  elmdb:put(D, {Layer, I}, <<I:160/integer>>)
                              end,
                              L),
                Res = lists:map(fun(I) ->
                                  <<I:160/integer>> = elmdb:get(D, {Layer, I}),
                                  I
                          end,
                          L),
                Res == L
            end).



