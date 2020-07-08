-module(elmdb_prop_test).

-behaviour(proper_statem).

-include_lib("kernel/include/logger.hrl").
-include_lib("proper/include/proper.hrl").

-export([test/0, sample/0]).
-export([prop_small/0]).

-export([initial_state/0,
         command/1,
         precondition/2,
         postcondition/3,
         next_state/3]).

-record(state, {db :: reference(),
                layers :: #{string() => #{binary()|non_neg_integer() => binary()}}}).

-define(KEYS, lists:seq(0,100)).

-define(DB_NAME, "./tmp-myelmdb").

test() ->
    proper:quickcheck(?MODULE:prop_small()).

sample() ->
    proper_gen:pick(commands(?MODULE)).

prop_small() ->
    %logger:set_handler_config(default, #{=> error, formatter => {logger_formatter,#{single_line=>true}}}),
    logger:set_primary_config(level, error),
    ?SETUP(fun() ->
            Dir = ?DB_NAME,
            ?LOG_NOTICE("~p setup........ db: ~ts", [self(),Dir]),
            ok = filelib:ensure_dir(Dir),
            D = elmdb:open(Dir),
            put(db, D),
            fun() -> 
                ?LOG_NOTICE("~p finalizing: ~ts", [self(), elmdb:db_path(D)]),
                [elmdb:drop(D, L) || L <- elmdb:ls(D)],
                ok = elmdb:dispose(D)
            end
           end,

        ?FORALL(Cmds, commands(?MODULE),
                ?TRAPEXIT(
                begin
                    ?LOG_NOTICE("run cmd........ ~p",[Cmds]),
                    %% setup here
                    {History,State,Result} = run_commands(?MODULE, Cmds),
                    ?LOG_NOTICE("ResuLt: ~p",[Result]),

                    %% teardown here
                    ?LOG_NOTICE("cleanup with state: ~p", [State]),
                    cleanup(State),

                    ?WHENFAIL(io:format("History: ~w~nState: ~w\nResult: ~w~n",
                                        [History,State,Result]),
                              aggregate(command_names(Cmds), Result =:= ok))
                end))).

cleanup(State) ->
    #state{db = D} = State,
    [elmdb:drop(D, L) || L <- elmdb:ls(D)],
    ?LOG_NOTICE("~p teardown: ~ts", [self(),elmdb:db_path(D)]).

initial_state() ->
    D = get(db),
    #state{db = D,
           layers = #{}}.

command(S) ->
    #state{db = D} = S,
    ?LOG_NOTICE("cmd -----> ~ts", [elmdb:db_path(D)]),
    frequency([{2, {call, elmdb,put,[D,{layer(S),key()}, value()]}},
               {7, {call, elmdb,get,[D,{layer(S),key()}]}},
               {5, {call, elmdb,count,[D,layer(S)]}},
               {1, {call, elmdb,del,[D,{layer(S),key()}]}}
              ]).

next_state(S, _V, {call,elmdb,del,[_D, {L,Key}]}) ->
    #state{layers = Layers} = S,
    case maps:find(L, Layers) of
        error ->
            NewLayers = Layers;
        {ok, LM0} ->
            LM = maps:remove(Key, LM0),
            NewLayers = maps:put(L, LM, Layers)
    end,
    S#state{layers = NewLayers};
next_state(S, _V, {call,elmdb,put,[_D, {L,Key}, Value]}) ->
    #state{layers = Layers} = S,
    LM0 = maps:get(L, Layers, #{}),
    LM = maps:put(Key,Value, LM0),

    NewLayers = maps:put(L, LM, Layers),
    S#state{layers = NewLayers};
next_state(S, _, _C) ->
    S.

precondition(_S, {call,_,_,_}) ->
    true.

postcondition(_S, {call,elmdb,put,_}, Result) ->
    ok =:= Result;
postcondition(S, {call,elmdb,count,[D,L]}, Result) ->
    #state{db = D, layers = Layers} = S,
    case maps:get(L, Layers, undefined) of
        undefined ->
            0 =:= Result;
        LM ->
            maps:size(LM) =:= Result
    end;
postcondition(S, {call,elmdb,get,[D,{L,Key}]}, Result) ->
    #state{db = D, layers = Layers} = S,
    case maps:get(L, Layers, undefined) of
        undefined ->
            {error, {dbi_not_found, L}} =:= Result;
        LM ->
            case maps:find(Key, LM) of
                error -> {error,notfound} =:= Result;
                {ok,Value} -> Value =:= Result
            end
    end;
postcondition(S, {call,elmdb,del,[D,{L,Key}]}, Result) ->
    #state{db = D, layers = Layers} = S,
    case maps:get(L, Layers, undefined) of
        undefined ->
            {error, {notfound_layer, L}} =:= Result;
        LM ->
            case maps:find(Key, LM) of
                error -> {error,notfound} =:= Result;
                {ok,Value} -> ok =:= Result
            end
    end;
postcondition(_S, {call,_,_,_}, Result) ->
    ?LOG_NOTICE("sth happen: Resutl=~p", [Result]),
    true.

layer(#state{layers= Layers}) ->
    elements(["foo", "bar"]).

key() ->
    non_neg_integer().

value() ->
    elements([<<"one">>,<<"two">>,<<"three">>,<<"five">>,<<"six">>,<<"seven">>,
             <<"eight">>,<<"nine">>,<<"ten">>]).

bin_value() ->
    binary(1024).
