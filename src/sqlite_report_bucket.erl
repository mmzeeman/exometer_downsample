%%
%% 
%% 

-module(sqlite_report_bucket).

-define(NO_SAMPLES, 600).

-export([
    init_metric_store/3,

    insert/3,

    get_history/4
]).


-record(store, {
    stores
}).

-record(dp_store, {
    insert_query,
    samplers
}).


% @doc Initializes the history tables needed to store the samples.
init_metric_store(Name, DataPoint, Db) when is_atom(DataPoint) ->
    init_metric_store(Name, [DataPoint], Db);
init_metric_store(Name, DataPoints, Db) ->
    % Periods = [hour, day, week, month, month3, month6, year],
    Periods = [hour, day],
    DpStores = [{DP, init_dp_store(Name, DP, Periods, Db)}  || DP <- DataPoints],
    #store{stores = DpStores}.

init_dp_store(MetricName, DataPoint, Periods, Db) ->
    Samplers = [begin 
            Table = init_table(MetricName, DataPoint, Period, Db),
             init_sampler(Period, Table) 
         end || Period <- Periods],

    #dp_store{samplers=Samplers}.

init_table(MetricName, DataPoint, Period, Db) ->
    {ok, TableName} = ensure_table(MetricName, DataPoint, Period, Db),
    <<"INSERT INTO \"", TableName/binary, "\" VALUES (?, ?)">>.

init_sampler(hour, Table) ->
    lttb:downsample_stream(6, Table);
init_sampler(day, Table) ->
    lttb:downsample_stream(24, Table).


% @doc Insert a new sample in the store.
%
insert(#store{stores=Stores}=MS, Values, InsertDb) ->
    Now = unix_time(),
    Stores1 = [ {Dp, insert_value(Store, Now, proplists:get_value(Dp, Values), InsertDb)} || {Dp, Store} <- Stores],
    MS#store{stores=Stores1}.

insert_value(Store, _Now, undefined, _InsertDb) -> Store;
insert_value(#dp_store{samplers=Samplers}=Store, Now, Value, InsertDb) ->
    Samplers1 = insert_sample(InsertDb, Samplers, {Now, Value}, false, []),
    Store#dp_store{samplers=Samplers1}.

insert_sample(_InsertDb, [], _Point, _Ready, Acc) -> lists:reverse(Acc);
insert_sample(InsertDb, [H|T], _Point, true, Acc) -> insert_sample(InsertDb, T, _Point, true, [H|Acc]);
insert_sample(InsertDb, [H|T], Point, false, Acc) ->
    case lttb:add(Point, H) of
        {continue, H1} -> 
            %% Done
            insert_sample(InsertDb, T, Point, true, [H1|Acc]);
        {ok, {Ts, V}=P, H1} ->
            InsertDb(lttb:state(H1), [Ts, V]),
            insert_sample(InsertDb, T, P, false, [H1|Acc])
    end.

get_history(Metric, DataPoint, Period, Db) ->
    TableName = table_name(Metric, DataPoint, Period),
    [ [{time, z_convert:to_integer(T)}, {value, V}] || {T, V} <- esqlite3:q(<<"SELECT time, value FROM\"", TableName/binary, "\" ORDER BY time DESC LIMIT 600">>, Db)].

%%
%% Helpers
%%

unix_time() ->
    {Mega, Secs, _} = os:timestamp(),
    Mega * 1000000 + Secs.


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


% heuristics for the number of seconds in a period.
seconds(hour) -> 3600;
seconds(day) -> seconds(hour) * 24;
seconds(week) -> seconds(day) * 7;
seconds(month) -> seconds(day) * 30;
seconds(month3) -> seconds(month) * 3;
seconds(month6) -> seconds(month) * 6;
seconds(year) -> seconds(day) * 365.

interval(NumberOfSamples, Period) -> 
    seconds(Period) / NumberOfSamples.

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

interval_test() ->
    % 25920, 52560,
    ?assertEqual(6.0, interval(600, hour)),
    ?assertEqual(144.0, interval(600, day)),
    ?assertEqual(1008.0, interval(600, week)),
    ?assertEqual(4320.0, interval(600, month)),
    ?assertEqual(12960.0, interval(600, month3)),
    ?assertEqual(25920.0, interval(600, month6)),
    ?assertEqual(52560.0, interval(600, year)),
    ok.

init_metric_store_test() ->
    {ok, Db} = esqlite3:open("metric-store.db"),
    Bucket = init_metric_store([a,b], [min, max], Db),
    ok.

%table_name_test() ->
%    ?assertEqual(<<"{[a,b],year}">>, table_name([a,b], year)),
%    ?assertEqual(<<"{[zotonic,site,webzmachine,data_out],month}">>, table_name([zotonic, site, webzmachine, data_out], month)),
%    ok.

-endif.

