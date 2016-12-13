%%
%% Largest Triangle Three Buckets 
%%
%% Timeseries downsampling
%% 
%% http://skemman.is/stream/get/1946/15343/37285/3/SS_MSthesis.pdf
%%

-module(lttb).

-export([
    downsample/2
]).

% @doc
downsample([], _Threshold) -> [];
downsample(Data, Threshold) when length(Data) =< Threshold->
    Data;
downsample(Data, Threshold) when Threshold > 2 ->
    BucketSize = (length(Data) - 2) div (Threshold- 2),
    [Selected|Tail] = Data,
    sample(Tail,  BucketSize, [Selected]).

% @doc
sample([Last], BucketSize, Acc) -> 
    lists:reverse([Last|Acc]);
sample(Data, BucketSize, [Selected|_]=Acc) ->
    {Bucket, Rest} = lists:split(BucketSize, Data),
    AvgPoint = average_point(Rest, BucketSize),
    Highest = rank(Bucket, Selected, AvgPoint),
    sample(Rest, BucketSize, [Highest|Acc]).

% @doc
average_point(Data, BucketSize) ->
    average_point(Data, 0, BucketSize, 0, 0).

average_point([], N, _BucketSize, TotalX, TotalY) ->
    {TotalX/N, TotalY/N};
average_point(_, N, BucketSize, TotalX, TotalY) when N =:= BucketSize ->
    {TotalX/BucketSize, TotalY/BucketSize};
average_point([{X, Y} | Rest], N, BucketSize, TotalX, TotalY) ->
    average_point(Rest, N+1, BucketSize, TotalX+X, TotalY+Y).

% @doc
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

% @doc
area({Ax, Ay}, {Bx, By}, {Cx, Cy}) ->
   abs(Ax *  (By - Cy) + Bx * (Cy - Ay) + Cx * (Ay - By)) / 2.

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

downsample_test() ->
    ?assertEqual([], downsample([], 10)),
    ?assertEqual([], downsample([{1,1}, {2,2}, {3,3}, {4,4}, {5,4}], 3)),
    ok.

average_point_test() ->
    ?assertEqual({1.0,1.0}, average_point([{1,1}, {1,1}, {1,1}, {2,2}, {2,2}, {2,2}], 3)),
    ?assertEqual({0.0,0.0}, average_point([{1,1}, {1,-1}, {-1,1}, {-1,-1}, {2,2}, {2,2}], 4)),
    ok.

-endif.