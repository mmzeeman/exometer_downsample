%%
%%
%%

-module(exometer_report_downsample).

-behaviour(exometer_report).

%% Exometer reporter callbacks
-export([
   exometer_init/1,
   exometer_info/2,
   exometer_cast/2,
   exometer_call/3,
   exometer_report/5,
   exometer_report_bulk/3,
   exometer_subscribe/5,
   exometer_unsubscribe/4,
   exometer_newentry/2,
   exometer_setopts/4,
   exometer_terminate/2
]).

-export([
    get_history/2, 
    get_history/3
]).

-include_lib("exometer_core/include/exometer.hrl").

-define(PURGE_INTERVAL, timer:minutes(30)).

-record(state, {
    handler,
    handler_args,
    handler_state, 

    samplers 
}).

-type value() :: any().
-type options() :: any().
-type state() :: #state{}.
-type callback_result() ::  {ok, state()}.

-spec exometer_init(options()) -> callback_result().
exometer_init(Opts) ->
    % We only support report bulk
    {report_bulk, true} = proplists:lookup(report_bulk, Opts),

    % Initialize the handler
    {handler, Handler} = proplists:lookup(handler, Opts),
    {handler_args, HandlerArgs} = proplists:lookup(handler_args, Opts),
    {ok, HandlerState} = Handler:downsample_handler_init(HandlerArgs),

    % Samplers.
    Samplers = dict:new(),

    trigger_purge(0),

    {ok, #state{handler_state = HandlerState, handler=Handler, handler_args=HandlerArgs, samplers = Samplers}}.

-spec exometer_report(exometer_report:metric(), exometer_report:datapoint(), exometer_report:extra(), value(), state()) -> callback_result().
exometer_report(_Metric, _DataPoint, _Extra, _Value, State) ->
    {ok, State}.

exometer_report_bulk(Found, _Extra,  #state{handler_state = HandlerState, handler=Handler}=State) ->
    Transaction = fun(Stg) ->
        Fun = fun(Query, Args) -> 
            Handler:downsample_handler_insert_datapoint(Query, Args, Stg)
        end,

        lists:foldl(fun({Metric, Values}, #state{samplers =Samplers}=S) -> 
            InsertFun =  fun(Store) -> downsample_bucket:insert(Store, Values, Fun) end,
            Samplers1 = dict:update(Metric, InsertFun, Samplers),
            S#state{samplers=Samplers1}
        end, State, Found)
    end,

    State1 = case Handler:downsample_handler_transaction(Transaction, HandlerState) of
        {rollback, _}=R -> throw(R);
        S -> S
    end,

    {ok, State1}.

-spec exometer_subscribe(exometer_report:metric(), exometer_report:datapoint(), exometer_report:interval(), exometer_report:extra(), state()) -> callback_result().
exometer_subscribe(Metric, DataPoint, _Interval, _SubscribeOpts,  #state{handler_state = HandlerState, handler=Handler, samplers=Samplers}=State) ->
    Store = downsample_bucket:init_buckets(Metric, DataPoint, Handler, HandlerState),
    {ok, State#state{samplers=dict:store(Metric, Store, Samplers)}}.

-spec exometer_unsubscribe(exometer_report:metric(), exometer_report:datapoint(), exometer_report:extra(), state()) -> callback_result().
exometer_unsubscribe(Metric, _DataPoint, _Extra, #state{samplers=Samplers}=State) ->
    %% Remove the entry of this metric.
    {ok, State#state{samplers=dict:erase(Metric, Samplers)}}.

-spec exometer_call(any(), pid(), state()) -> {reply, any(), state()} | {noreply, state()} | any().
exometer_call({history, _Metric, _DataPoint}, _From, #state{handler=Handler, handler_args = Args}=State) ->
    {reply, {ok, Handler, Args}, State};
exometer_call(_Unknown, _From, State) ->
    {ok, State}.

-spec exometer_cast(any(), state()) -> {noreply, state()} | any().
exometer_cast(_Unknown, State) ->
    {ok, State}.

-spec exometer_info(any(), state()) -> callback_result().
exometer_info(purge, #state{handler=Handler, handler_args=HandlerArgs, handler_state=HandlerState}=State) ->
    Handler:downsample_handler_purge(HandlerArgs, HandlerState),
    trigger_purge(?PURGE_INTERVAL),
    {ok, State};
exometer_info(_Unknown, State) ->
    {ok, State}.

-spec exometer_newentry(exometer:entry(), state()) -> callback_result().
exometer_newentry(_Entry,  State) ->
    {ok, State}.

-spec exometer_setopts(exometer:entry(), options(), exometer:status(), state()) -> callback_result().
exometer_setopts(_Metric, _Options, _Status, State) ->
    {ok, State}.

-spec exometer_terminate(any(), state()) -> any().
exometer_terminate(_Reason, #state{handler=Handler, handler_state=HandlerState}) ->
    ok = Handler:downsample_handler_close(HandlerState).

%%
%% Extra API
%%

-spec get_history(exometer:metric(), exometer_report:datapoint())  -> list().
get_history(Metric, DataPoint) ->
    get_history(Metric, DataPoint, [hour, day]).

get_history(Metric, DataPoint, Periods) ->
    {ok, Handler, HandlerArgs} = exometer_report:call_reporter(?MODULE, {history, Metric, DataPoint}),
    Handler:downsample_handler_get_history(HandlerArgs, Metric, DataPoint, Periods).

%%
%% Helpers
%%

trigger_purge(Time) ->
    erlang:send_after(Time, self(), purge).
