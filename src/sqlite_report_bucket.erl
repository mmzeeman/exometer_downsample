%%
%% 
%% 

-module(sqlite_report_bucket).

-define(NO_SAMPLES, 600).

-export([
    init_metric_store/3,

    insert/3
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
    DpStores = [{ {Name, DP}, init_dp_store(Name, DP, Periods, Db)}  || DP <- DataPoints],
    #store{stores = DpStores}.

init_dp_store(MetricName, DataPoint, Periods, Db) ->
    Samplers = [init_sampler(Period)  || Period <- Periods],
    #dp_store{samplers=Samplers}.

init_sampler(hour) ->
    lttb:downsample_stream(6);
init_sampler(day) ->
    lttb:downsample_stream(144).


% @doc Insert a new sample in the store.
%
insert(#store{stores=Stores}=MS, Values, Db) ->
    Now = unix_time(),

    io:fwrite(standard_error, "insert: ~p, ~p, ~p~n", [Now, MS, Values]),

    MS.


%%
%% Helpers
%%

unix_time() ->
    {Mega, Secs, _} = os:timestamp(),
    Mega * 1000000 + Secs.


%%
%% Helpers
%%

%ensure_table(Period, Name, DataPoints, Db) ->
%    TableName = table_name(Name, Period), 
%    case esqlite3_utils:table_exists(TableName, Db) of
%        false -> 
%            create_table(TableName, DataPoints, Db);
%        true -> 
%            ok
%    end.

%table_name(Metric, Period) ->
%    erlang:iolist_to_binary(io_lib:format("~p", [{Metric, Period}])).

%create_table(TableName, DataPoints, Db) ->
%    ColumnDefs = <<"sample double PRIMARY KEY NOT NULL, time double NOT NULL, value double NOT NULL">>,
%    [] = esqlite3:q(<<"CREATE TABLE \"", TableName/binary, "\"(", ColumnDefs/binary, ")">>, Db),
%    ok.


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

    io:fwrite(standard_error, "store: ~p~n", [Bucket]),
    ok.

%table_name_test() ->
%    ?assertEqual(<<"{[a,b],year}">>, table_name([a,b], year)),
%    ?assertEqual(<<"{[zotonic,site,webzmachine,data_out],month}">>, table_name([zotonic, site, webzmachine, data_out], month)),
%    ok.

-endif.

