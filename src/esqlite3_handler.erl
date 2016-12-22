%%
%% SQLite3 Storage handler module
%%

-module(esqlite3_handler).

-export([
    storage_init/1,
    storage_close/2,
    storage_transaction/1,
    storage_init_datapoint/2,
    storage_insert_datapoint/2
]).

% Initialize the storage
storage_init(Args) ->
    init_database(DbArg) ->
    {ok, Db} = esqlite3:open(DbArg),
    Db.

% Close the storage
storage_close(Db) ->
    ok = esqlite3:close(Db).

% Fold related updates into one transaction
storage_transaction(Fun,  Storage) ->
    esqlite3_utils:transaction(Fun, Storage).

% Initialize storage for the given datapoint.
storage_init_datapoint(Metric, DataPoint, Period, Storage) ->
    {ok, TableName} = ensure_table(MetricName, DataPoint, Period, Storage),
    <<"INSERT INTO \"", TableName/binary, "\" VALUES (?, ?)">>.

% One datapoint insert.
storage_insert_datapoint(Query, Args, Storage) ->
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
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%%init_metric_store_test() ->
%%    {ok, Db} = esqlite3:open("metric-store.db"),
%%   Bucket = init_metric_store([a,b], [min, max], Db),
%%  ok

-endif.