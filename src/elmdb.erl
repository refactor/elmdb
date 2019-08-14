-module(elmdb).

-export([hello/1]).
-export([to_map/2]).
-export([init/1]).
-export([close/1]).
-export([put/3]).
-export([get/2]).
-export([list_layers/1]).

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

to_map(_Reference, _Layer) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec init(filelib:dirname()) -> reference().
init(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).

-spec close(reference()) -> ok | {error, any()}.
close(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).

put(_LmdbRes, {_Layer,_Key}, _Value) ->
	erlang:nif_error({not_loaded, ?MODULE}).

get(_LmdbRes, {_Layer,_Key}) ->
	erlang:nif_error({not_loaded, ?MODULE}).

list_layers(_LmdbRes) ->
	erlang:nif_error({not_loaded, ?MODULE}).
