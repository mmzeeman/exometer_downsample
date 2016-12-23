%
%
% Dummy handler to test the reportere

-module(downsample_handler_test).

-export([
    downsample_handler_init/1,
    downsample_handler_close/1,
    downsample_handler_transaction/2,
    downsample_handler_init_datapoint/4,
    downsample_handler_insert_datapoint/3
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

downsample_handler_insert_datapoint(_Data, datapoint_state, test_handler_state) ->
    ok.