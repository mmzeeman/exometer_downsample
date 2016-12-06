%%
%% 
%% 


-module(sqlite_report_bucket).

-export([
    new/3,
    load_history/2,

    set_next_bucket/2

]).

-record(bucket, {
    name,
    table,
    interval,

    next,

    sample_id,
    samples 
}).

-record(sample, {
    sample_id,
    time,
    next_time,

    data 
}).

%% Create a new bucket.
new(Name, Interval, TableName) ->
    #bucket{name=Name, interval=Interval, table=TableName}.

%% Attach a bucket to this bucket.
set_next_bucket(#bucket{next=undefined}=Bucket, #bucket{}=NextBucket) ->
    Bucket#bucket{next = NextBucket}.

%% Load historic samples from the specified database
load_history(#bucket{next=undefined}=Bucket, _Db) -> Bucket;
load_history(#bucket{next=NextBucket, interval=Interval, table=Table}=Bucket, Db) ->
    %% How many samples we need depends on the sample interval of the next bucket.
    NextBucketInterval = NextBucket#bucket.interval,

    %% Load the samples needed to calculate the sample for the next bucket.
    NoSamples = integer_to_binary(ceil(NextBucketInterval / Interval), 10),
    Q = <<"select * from ",  Table/binary, " order by sample desc limit ", NoSamples/binary>>,
    Result = esqlite3:q(Q, Db),
    Samples = make_samples(Result),

    %% Load the history of the next bucket
    NextBucket1 = load_history(NextBucket, Db),
    
    Bucket#bucket{next=NextBucket1, samples=Samples}.

%%
add_sample( ) ->
    ok.

%%
%% Helpers
%%

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

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

ceil_test() ->
    ?assertEqual(2, ceil(1.2)),
    ?assertEqual(-11, ceil(-11.2)),
    ?assertEqual(0, ceil(-0.2)),
    ?assertEqual(1, ceil(0.2)),
    ok.

make_samples_test() ->
    ?assertEqual([], make_samples([])),

    ok.



-endif.
