%%
%% 
%% 

-module(sqlite_report_bucket).

-define(NO_SAMPLES, 600).

-export([
    new/3,
    load_history/2,

    set_next_bucket/2

]).

-record(bucket, {
    name, 
    datapoints,
    period,
    interval,

    insert_query, % the query needed to insert a sample.
    history_query, % query needed to retrieve historic samples.

    next_bucket, % The next bucket
    number_of_samples, % The number of samples we must remember.

    sample_id, % The current sample if of this table.
    samples  % The last couple of samples needed to calculate a sample for the next bucket
}).

init_metric_store(Name, DataPoints, Db) ->
    Periods = [hour, day, week, month, month3, month6, year],

    Bucket =  lists:foldr(fun(Period, B) -> 
        New = new(Name, DataPoints, Period),
        case B of
            undefined -> New;
            _ -> set_next_bucket(New, B)
        end
    end, undefined, Periods),

    load_history(Bucket, Db).


%% Create a new bucket.
new(Name, DataPoints, PeriodName) ->
    new(Name, DataPoints, PeriodName, ?NO_SAMPLES).

new(Name, DataPoints, PeriodName, NumberOfSamples) ->
    Interval = interval(NumberOfSamples, PeriodName),

    TableName = table_name(Name, PeriodName),
    %HistoryQ = <<"select * from ",  Table/binary, " order by sample desc limit ", NoSamples/binary>>,

    #bucket{name=Name, datapoints=DataPoints, period=PeriodName, interval=Interval}.


%% Attach a bucket to this bucket.
set_next_bucket(#bucket{next_bucket=undefined}=Bucket, #bucket{}=NextBucket) ->

    % Q = <<"select * from ",  Table/binary, " order by sample desc limit ", NoSamples/binary>>,


    Bucket#bucket{next_bucket = NextBucket}.

%% Load historic samples from the specified database
load_history(#bucket{next_bucket=undefined, name=Name, period=Period, datapoints=DataPoints}=Bucket, Db) -> 
    ok = ensure_table(Period, Name, DataPoints, Db),
    Bucket;
load_history(#bucket{next_bucket=NextBucket, name=Name, period=Period, datapoints=DataPoints, interval=Interval}=Bucket, Db) ->
    ok = ensure_table(Period, Name, DataPoints, Db),

    %% How many samples we need depends on the sample interval of the next bucket.
    NextBucketInterval = NextBucket#bucket.interval,
    Amount = ceil(NextBucketInterval / Interval),
    Samples = get_samples(Amount, Period, Name, DataPoints, Db),

    io:fwrite(standard_error, "samples: ~p, ~p~n", [Period, Samples]),

    %% Load the history of the next bucket in line.
    NextBucket1 = load_history(NextBucket, Db),
    
    Bucket#bucket{next_bucket=NextBucket1, samples=Samples}.

%%
add_sample( ) ->
    ok.

%%
%% Helpers
%%

get_samples(Amount, Period, Name, DataPoints, Db) ->
    Number = integer_to_binary(Amount, 10),
    Table = table_name(Name, Period),
    Q = <<"SELECT * from \"", Table/binary, "\" ORDER  BY sample DESC LIMIT ", Number/binary>>,
    esqlite3:q(Q, Db).

ensure_table(Period, Name, DataPoints, Db) ->
    TableName = table_name(Name, Period), 
    case esqlite3_utils:table_exists(TableName, Db) of
        false -> create_table(TableName, DataPoints, Db);
        true -> ok
    end.

table_name(Metric, Period) ->
    erlang:iolist_to_binary(io_lib:format("~p", [{Metric, Period}])).

create_table(TableName, DataPoints, Db) ->
    Defs = [<<"sample double PRIMARY KEY NOT NULL">>, <<"time double NOT NULL">> | column_defs(DataPoints)],
    ColumnDefs= bjoin(Defs),
    [] = esqlite3:q(<<"CREATE TABLE \"", TableName/binary, "\"(", ColumnDefs/binary, ")">>, Db),
    ok.

column_defs(DpInfo) -> column_defs(DpInfo, []).

column_defs([], Acc)-> lists:reverse(Acc);
column_defs([H|T], Acc) when is_atom(H) ->
    Name = atom_to_binary(H, utf8),
    column_defs(T, [<<Name/binary, 32, "double NOT NULL">> | Acc]);
column_defs([H|T], Acc) when is_integer(H) ->
    Name = integer_to_binary(H, 10),
    column_defs(T, [<<$_, Name/binary, 32, "double NOT NULL">> | Acc]).
     
bjoin(List) -> bjoin(List, <<$,>>).

bjoin(List, Separator) -> bjoin(List, Separator, <<>>).

bjoin([], _Separator, Acc) -> Acc;
bjoin([H|T], Separator, <<>>) -> bjoin(T, Separator, H);
bjoin([H|T], Separator, Acc) -> bjoin(T, Separator, <<Acc/binary, Separator/binary, H/binary>>).    

make_samples(List) -> make_samples(List, []).

make_samples([], Acc) -> lists:reverse(Acc);
make_samples([H|T], Acc) ->
    ok.

ceil(X) when X < 0 -> trunc(X);
ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

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

ceil_test() ->
    ?assertEqual(2, ceil(1.2)),
    ?assertEqual(-11, ceil(-11.2)),
    ?assertEqual(0, ceil(-0.2)),
    ?assertEqual(1, ceil(0.2)),
    ok.

make_samples_test() ->
    ?assertEqual([], make_samples([])),
    ok.

new_bucket_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    Metric = [a,b],
    DataPoint = [min, max],

    Year = new(Metric, DataPoint, year),
    Month6 = set_next_bucket(new(Metric, DataPoint, month6), Year),

    load_history(Month6, Db),

    ok.

init_metric_store_test() ->
    {ok, Db} = esqlite3:open("metric-store.db"),

    Bucket = init_metric_store([a,b], [min, max], Db),

    io:fwrite(standard_error, "store: ~p~n", [Bucket]),

    ok.
    

table_name_test() ->
    ?assertEqual(<<"{[a,b],year}">>, table_name([a,b], year)),
    ?assertEqual(<<"{[zotonic,site,webzmachine,data_out],month}">>, table_name([zotonic, site, webzmachine, data_out], month)),
    ok.



-endif.
