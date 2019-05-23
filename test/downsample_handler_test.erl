%
% Dummy handler to test the reporter
%

-module(downsample_handler_test).

-behaviour(downsample_handler).

-export([
    downsample_handler_init/1,
    downsample_handler_close/1,
    downsample_handler_transaction/2,
    downsample_handler_init_datapoint/4,
    downsample_handler_insert_datapoint/7,
    downsample_handler_get_history/4,
    downsample_handler_purge/2
]).


%%
downsample_handler_init([]) -> 
    {ok, test_handler_state}.

downsample_handler_close(test_handler_state) -> 
    ok.

downsample_handler_transaction(F, test_handler_state) ->
    F(test_handler_state).

downsample_handler_init_datapoint(Metric, DataPoint, Period, test_handler_state) ->
    [Metric, DataPoint, Period, test_handler_state].

downsample_handler_insert_datapoint(_Metric, _Datapoint, _Period, _Timestamp, _Value, datapoint_state, test_handler_state) ->
    ok.

downsample_handler_get_history([], _Metric, _DatePoint, _Periods) ->
    ok.

downsample_handler_purge(_Args, test_handler_state) ->
    ok.
