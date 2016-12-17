%%
%% Largest Triangle Three Buckets 
%%
%% Timeseries downsampling
%% 
%% http://skemman.is/stream/get/1946/15343/37285/3/SS_MSthesis.pdf
%%
%% Live example: http://d3fc.github.io/d3fc-sample/
%%

-module(lttb).

-export([
    downsample/2,

    downsample_stream/1, downsample_stream/2, downsample_stream/3,
    state/1,
    add/2
]).

-record(lttb, {
    bucket_size, % the size of the buckets.
    selected, % the selected point of the first bucket

    bucket2, % the second bucket from which we will select the next point.
    bucket3, % the third bucket

    state %  
}).

% @doc Downsample Data with the Largest Triangle Three Buckets algorithm
downsample([], _Threshold) -> [];
downsample(Data, Threshold) when length(Data) =< Threshold-> Data;
downsample(Data, Threshold) when Threshold > 2 ->
    BucketSize = (length(Data) - 2) div (Threshold - 2),
    [Selected|Tail] = Data,
    sample(Tail,  BucketSize, [Selected]).

% @doc Tail recursive downsam  
sample([Last], _BucketSize, Acc) -> lists:reverse([Last|Acc]);
sample(Data, BucketSize, [Selected|_]=Acc) ->
    {Bucket, Rest} = lists:split(BucketSize, Data),
    AvgPoint = average_point(Rest, BucketSize),
    Highest = rank(Bucket, Selected, AvgPoint),
    sample(Rest, BucketSize, [Highest|Acc]).

% @doc Return the state for a streaming version of largest triangle three buckets.
downsample_stream(BucketSize) -> downsample_stream(BucketSize, undefined).
downsample_stream(BucketSize, State) -> downsample_stream(undefined, BucketSize, State).
downsample_stream(Selected, BucketSize, State) ->
    #lttb{bucket_size=BucketSize, selected=Selected, bucket2=[], bucket3=[], state=State}.

state(#lttb{state=State}) -> State.

% @doc Add a point for downsampling. Returns {ok, Point, State} when a new point is selected.
%      Returns {continue, State} otherwise.
add(Point, #lttb{selected=undefined}=State) ->
    {continue, State#lttb{selected=Point}};
add(Point, #lttb{bucket_size=S, selected=P, bucket2=B2, bucket3=B3}=State) when S =:=length(B2) andalso S =:= length(B3)+1 ->
    % We can calculate the next selected point.
    B3_1 = [Point|B3],
    AvgPoint = average_point(B3_1, S),
    Highest = rank(B2, P, AvgPoint),
    {ok, Highest, State#lttb{selected=Highest, bucket2=B3_1, bucket3=[]} };
add(Point, #lttb{bucket_size=S, bucket2=B2}=State) when length(B2) < S ->
    {continue, State#lttb{bucket2=[Point|B2]}};
add(Point, #lttb{bucket_size=S, bucket2=B2, bucket3=B3}=State) when length(B3) < S andalso length(B2) =:= S ->
    {continue, State#lttb{bucket3=[Point|B3]}}.


%%
%% Helpers
%%


% @doc Calculate the average point in the bucket
average_point(Data, BucketSize) ->
    average_point(Data, 0, BucketSize, 0, 0).

average_point([], N, _BucketSize, TotalX, TotalY) ->
    {TotalX/N, TotalY/N};
average_point(_, N, BucketSize, TotalX, TotalY) when N =:= BucketSize ->
    {TotalX/BucketSize, TotalY/BucketSize};
average_point([{X, Y} | Rest], N, BucketSize, TotalX, TotalY) ->
    average_point(Rest, N + 1, BucketSize, TotalX + X, TotalY + Y).

% @doc Get the highest ranking point in the bucket
rank(Bucket, Selected, AvgPoint) ->
    rank(Bucket, Selected, AvgPoint, undefined, undefined).

rank([], _Selected, _AvgPoint, _Rank, Highest) -> Highest;
rank([A|Rest], Selected, AvgPoint, undefined, _Highest) ->
    Area = area(A, Selected, AvgPoint),
    rank(Rest, Selected, AvgPoint, Area, A);
rank([A|Rest], Selected, AvgPoint, Rank, Highest) ->
    Area = area(A, Selected, AvgPoint),
    case Area >= Rank of
        true -> rank(Rest, Selected, AvgPoint, Area, A);
        false -> rank(Rest, Selected, AvgPoint, Rank, Highest)
    end.

% @doc Calculate the area of the triangle.
area({Ax, Ay}, {Bx, By}, {Cx, Cy}) ->
   abs(Ax *  (By - Cy) + Bx * (Cy - Ay) + Cx * (Ay - By)) / 2.

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

downsample_test() ->
    ?assertEqual([], downsample([], 10)),
    ?assertEqual([{1,1}, {4,4}, {5,4}], downsample([{1,1}, {2,2}, {3,3}, {4,4}, {5,4}], 3)),
    ?assertEqual([{1,2}, {5,6}, {12,2}], downsample([{1,2},{2,2},{3,3},{4,3},{5,6},{6,3},{7,3},{8,5},{9,4},{10,4},{11,1},{12,2}], 3)),
    ?assertEqual([{1,2}, {5,6}, {7,3}, {12,2}], downsample([{1,2},{2,2},{3,3},{4,3},{5,6},{6,3},{7,3},{8,5},{9,4},{10,4},{11,1},{12,2}], 4)),
    ok.

downsample_stream_test() ->
    S = downsample_stream({1,1}, 3, undefined),
    {continue, S1} = add({2,2}, S),
    {continue, S2} = add({3,3}, S1),
    {continue, S3} = add({4,3}, S2),
    {continue, S4} = add({5,6}, S3),
    {continue, S5} = add({6,3}, S4),
    {ok, {3,3}, S6} = add({7,3}, S5),
    {continue, S7} = add({8,5}, S6),
    {continue, S8} = add({9,4}, S7),
    {ok, {5,6}, S9} = add({10,4}, S8),
    {continue, S10} = add({11,1}, S9),
    {continue, S11} = add({12,2}, S10),
    {ok, {8,5}, _S12} = add({13,6}, S11),

    ok.

average_point_test() ->
    ?assertEqual({1.0,1.0}, average_point([{1,1}, {1,1}, {1,1}, {2,2}, {2,2}, {2,2}], 3)),
    ?assertEqual({0.0,0.0}, average_point([{1,1}, {1,-1}, {-1,1}, {-1,-1}, {2,2}, {2,2}], 4)),
    ok.

-endif.