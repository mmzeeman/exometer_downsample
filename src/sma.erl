%%
%% Simple Moving Average
%%

-module(sma).

-export([
    new/1, 
    average/1, average/2,
    append/2,
    values/1
]).

-record(sma, {
    period,
    stream,
    stream_length
}).

% @doc New moving average
new(Period) when is_integer(Period) -> 
    #sma{period=Period, stream=queue:new(), stream_length=0}.

% @doc Append a value to the moving average 
append(Number, #sma{period=Period, stream=Stream, stream_length=Period}=SMA) ->
    Stream1 = queue:snoc(queue:tail(Stream), Number),
    SMA#sma{stream=Stream1};
append(Number, #sma{period=Period, stream=Stream, stream_length=StreamLength}=SMA) when StreamLength < Period ->
    Stream1 = queue:snoc(Stream, Number),
    SMA#sma{stream=Stream1, stream_length=StreamLength+1}.

values(#sma{stream=Stream}) -> queue:to_list(Stream).

% @doc Calculate the average.
average(#sma{}=SMA) ->
    average(fun avg_list/1, SMA).

average(Fun, #sma{}=SMA) ->
    Fun(values(SMA)).

avg_list([]) -> 0.0;
avg_list(L) -> lists:sum(L)/length(L).

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

sma3_test() ->
    S0 = new(3),
    ?assertEqual(0.0, average(S0)),

    S1 = append(1, S0),
    ?assertEqual(1.0, average(S1)),

    S2 = append(2, S1),
    ?assertEqual(1.5, average(S2)),

    S3 = append(3, S2),
    ?assertEqual(2.0, average(S3)),
    ?assertEqual([1,2,3], values(S3)),

    S4 = append(4, S3),
    ?assertEqual(3.0, average(S4)),
    ?assertEqual([2,3,4], values(S4)),

    S5 = append(5, S4),
    ?assertEqual(4.0, average(S5)),
    ?assertEqual([3,4,5], values(S5)),

    S6 = append(5, S5),
    ?assertEqual(4.666666666666667, average(S6)),
    ?assertEqual([4,5,5], values(S6)),

    ok.

-endif.
