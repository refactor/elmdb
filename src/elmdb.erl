-module(elmdb).

-export([hello/1]).
-export([init/1]).
-export([put/3]).
-export([get/2]).

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

init(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).

put(_LmdbRes, {_Layer,_Key}, _Value) ->
	erlang:nif_error({not_loaded, ?MODULE}).

get(_LmdbRes, {_Layer,_Key}) ->
	erlang:nif_error({not_loaded, ?MODULE}).
