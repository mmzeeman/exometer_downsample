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
    interval,

    insert_query, % the query needed to insert a sample.
    history_query, % query needed to retrieve historic samples.

    next_bucket, % The next bucket
    number_of_samples, % The number of samples we must remember.

    sample_id, % The current sample if of this table.
    samples  % The last couple of samples needed to calculate a sample for the next bucket
}).

-record(sample, {
    sample_id,
    time,
    next_time,

    data 
}).

%% Create a new bucket.
new(Name, DataPoints, PeriodName) ->
    new(Name, DataPoints, PeriodName, ?NO_SAMPLES).

new(Name, DataPoints, PeriodName, NumberOfSamples) ->
    Interval = interval(NumberOfSamples, PeriodName),

    TableName = table_name(Name, PeriodName),
    %HistoryQ = <<"select * from ",  Table/binary, " order by sample desc limit ", NoSamples/binary>>,

    #bucket{name=Name, datapoints=DataPoints, interval=Interval}.


%% Attach a bucket to this bucket.
set_next_bucket(#bucket{next_bucket=undefined}=Bucket, #bucket{}=NextBucket) ->

    % Q = <<"select * from ",  Table/binary, " order by sample desc limit ", NoSamples/binary>>,


    Bucket#bucket{next_bucket = NextBucket}.

%% Load historic samples from the specified database
load_history(#bucket{next_bucket=undefined}=Bucket, _Db) -> Bucket;
load_history(#bucket{next_bucket=NextBucket, interval=Interval}=Bucket, Db) ->
    %% How many samples we need depends on the sample interval of the next bucket.
    NextBucketInterval = NextBucket#bucket.interval,

    %% Load the samples needed to calculate the sample for the next bucket.
    %NoSamples = integer_to_binary(ceil(NextBucketInterval / Interval), 10),
    %Q = <<"select * from ",  Table/binary, " order by sample desc limit ", NoSamples/binary>>,
    %Result = esqlite3:q(Q, Db),

    %Samples = make_samples(Result),
    Samples = [],

    %% Load the history of the next bucket
    NextBucket1 = load_history(NextBucket, Db),
    
    Bucket#bucket{next_bucket=NextBucket1, samples=Samples}.

%%
add_sample( ) ->
    ok.

%%
%% Helpers
%%

table_name(Metric, Period) ->
    BaseName = table_name(Metric),
    PeriodName = erlang:atom_to_binary(Period, utf8),
    <<BaseName/binary, $_, PeriodName/binary>>.

table_name(Metric) -> table_name1(Metric, <<>>).

table_name1([], Acc) -> Acc;
table_name1([H|T], <<>>) -> table_name1(T, erlang:atom_to_binary(H, utf8));
table_name1([H|T], Acc) ->
    B = erlang:atom_to_binary(H, utf8),
    table_name1(T, <<Acc/binary, $$, B/binary>>).

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

table_name_test() ->
    ?assertEqual(<<"a$b_year">>, table_name([a,b], year)),
    ?assertEqual(<<"a$b_year">>, table_name([a,b], year)),
    ?assertEqual(<<"zotonic$site$webzmachine$data_out_month">>, table_name([zotonic, site, webzmachine, data_out], month)),
    ?assertEqual(<<"zotonic$site$session$page_processes_month">>, table_name([zotonic, site, session, page_processes], day )),
    ok.



-endif.
