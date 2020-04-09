-module(elmdb).

-export([hello/1]).
-export([to_map/2]).

-export([open/1]).
-export([dispose/1]).
-export([close/1]).
-export([drop/2]).
-export([count/2]).
-export([put/3]).
-export([get/2]).
-export([del/2]).
-export([minkey/2]).
-export([maxkey/2]).
-export([range/3]).
-export([range/4]).
-export([ls/1]).
-export([foldl/4]).

-on_load(on_load/0).

on_load() ->
	PrivDir = case code:priv_dir(?MODULE) of
		{error, _} ->
			AppPath = filename:dirname(filename:dirname(code:which(?MODULE))),
			filename:join(AppPath, "priv");
		Path ->
			Path
	end,
	erlang:load_nif(filename:join(PrivDir, atom_to_list(?MODULE)), 0).

hello(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).

to_map(_LmdbRes, _Layer) ->
	erlang:nif_error({not_loaded, ?MODULE}).

range(_LmdbRes, _Layer, _Begin) ->
	erlang:nif_error({not_loaded, ?MODULE}).

range(_LmdbRes, _Layer, _Begin, _End) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec open(filelib:dirname()) -> reference().
open(DirName) ->
    filelib:ensure_dir(DirName ++ "/"),
    init(DirName).

-spec init(filelib:dirname()) -> reference().
init(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec close(reference()) -> ok | {error, any()}.
close(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec db_path(reference()) -> file:filename() | {error, closed}.
db_path(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec dispose(reference()) -> ok | {error, any()}.
dispose(LmdbRes) ->
    try db_path(LmdbRes) of
        Dir ->
            ok = close(LmdbRes),
            [file:delete(F) || F <- filelib:wildcard(Dir ++ "/*")],
            file:del_dir(Dir)
    catch error:Reason -> {error, Reason}
    end.

-spec drop(reference(), string()) -> ok | {error, any()}.
drop(_LmdbRes, _Layer) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec count(reference(), string()) -> non_neg_integer().
count(_LmdbRes, _Layer) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec put(reference(), {string(), binary()}, binary()) -> reference().
put(_LmdbRes, {_Layer,_Key}, _Value) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec get(reference(), {string(), binary()|integer()}) -> binary().
get(_LmdbRes, {_Layer,_Key}) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec del(reference(), {string(), binary()|integer()}) -> reference().
del(_LmdbRes, {_Layer,_Key}) ->
	erlang:nif_error({not_loaded, ?MODULE}).

minkey(_LmdbRes, _Layer) ->
	erlang:nif_error({not_loaded, ?MODULE}).

maxkey(_LmdbRes, _Layer) ->
	erlang:nif_error({not_loaded, ?MODULE}).

ls(_LmdbRes) ->
	erlang:nif_error({not_loaded, ?MODULE}).

foldl(LmdbRes, Layer, Acc0, Fun) -> 
    LFun = fun({Key, Value}, Acc) ->
               Fun({{Layer, Key}, Value}, Acc)
           end,
    Cursor = iter(LmdbRes, Layer),
    travel(next(Cursor), Cursor, Acc0, LFun).

travel(end_of_table, Cursor, Acc, _Fun) ->
    close_iter(Cursor),
    Acc;
travel({Key, Value}, Cursor, Acc, Fun) ->
    NewAcc = Fun({Key, Value}, Acc),
    travel(next(Cursor), Cursor, NewAcc, Fun).

-spec iter(reference(), string()) -> reference().
iter(_LmdbRes, _Layer) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec close_iter(reference()) -> ok.
close_iter(_LmdbRes) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec next(reference()) -> {binary(), binary()}.
next(_Cursor) ->
	erlang:nif_error({not_loaded, ?MODULE}).

