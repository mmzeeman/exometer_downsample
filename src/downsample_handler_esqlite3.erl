%%
%% SQLite3 Storage handler module
%%

-module(downsample_handler_esqlite3).

-export([
    downsample_handler_init/1,
    downsample_handler_close/1,
    downsample_handler_transaction/2,
    downsample_handler_init_datapoint/4,
    downsample_handler_insert_datapoint/3
]).

-export([
      get_history/4
]).

-behaviour(downsample_handler).

%%
%% Callbacks
%%

% Initialize the handler 
downsample_handler_init([DbArg]) ->
    {ok, Db} = esqlite3:open(DbArg),
    Db.

% Close the handler 
downsample_handler_close(Db) ->
    ok = esqlite3:close(Db).

% Folds related inserts into one transaction
downsample_handler_transaction(Fun,  Db) ->
    esqlite3_utils:transaction(Fun, Db).

% Initialize the handler for the given datapoint.
downsample_handler_init_datapoint(Metric, DataPoint, Period, Db) ->
    {ok, TableName} = ensure_table(Metric, DataPoint, Period, Db),
    <<"INSERT INTO \"", TableName/binary, "\" VALUES (?, ?)">>.

% One datapoint insert.
downsample_handler_insert_datapoint(Query, Args, Storage) ->
    esqlite3:q(Query, Args, Storage).

%%
%% Helpers
%%

ensure_table(Name, DataPoint, Period, Db) ->
    TableName = table_name(Name, DataPoint, Period), 
    case esqlite3_utils:table_exists(TableName, Db) of
        false -> create_table(TableName, Db);
        true -> ok
    end,
    {ok, TableName}.

table_name(Metric, DataPoint, Period) ->
    erlang:iolist_to_binary(io_lib:format("~p", [{Metric, DataPoint, Period}])).

create_table(TableName, Db) ->
    ColumnDefs = <<"time double PRIMARY KEY NOT NULL, value double NOT NULL">>,
    [] = esqlite3:q(<<"CREATE TABLE \"", TableName/binary, "\"(", ColumnDefs/binary, ")">>, Db),
    ok.

%%
%% Extra Api
%%

get_history(Metric, DataPoint, Period, Db) ->
    TableName = table_name(Metric, DataPoint, Period),
    [ [{time, z_convert:to_integer(T)}, {value, V}] || {T, V} <- esqlite3:q(<<"SELECT time, value FROM\"", TableName/binary, "\" ORDER BY time DESC LIMIT 600">>, Db)].

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%%init_metric_store_test() ->
%%    {ok, Db} = esqlite3:open("metric-store.db"),
%%   Bucket = init_metric_store([a,b], [min, max], Db),
%%  ok

-endif.