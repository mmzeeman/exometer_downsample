%%
%%
%%

-module(exometer_report_sqlite).

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
    get_history/2, get_history/3
]).

-include_lib("exometer_core/include/exometer.hrl").

-record(state, {
    db_arg,
    db,

    storages
}).

-type value() :: any().
-type options() :: any().
-type state() :: #state{}.
-type callback_result() ::  {ok, state()}.

-spec exometer_init(options()) -> callback_result().
exometer_init(Opts) ->
    lager:info("~p(~p): Starting", [?MODULE, Opts]),

    {db_arg, DbArg} = proplists:lookup(db_arg, Opts),
    {report_bulk, true} = proplists:lookup(report_bulk, Opts),
    Db = init_database(DbArg),
    Storages = dict:new(),
    {ok, #state{db_arg = DbArg, db = Db, storages = Storages}}.

-spec exometer_report(exometer_report:metric(), exometer_report:datapoint(), exometer_report:extra(), value(), state()) -> callback_result().
exometer_report(_Metric, _DataPoint, _Extra, _Value, State) ->
    lager:warning("~p: Use {report_bulk, true}.", [?MODULE]),
    {ok, State}.

exometer_report_bulk(Found, _Extra,  #state{db=Db}=State) ->
    Transaction = fun(TDb) ->
        InsertDb = fun(Query, Args) -> 
            esqlite3:q(Query, Args, TDb)
        end,
        lists:foldl(fun({Metric, Values}, #state{storages=Storages}=S) -> 
             Storages1 = dict:update(Metric, fun(Store) -> 
                sqlite_report_bucket:insert(Store, Values, InsertDb) 
                end, Storages),
            S#state{storages=Storages1}
        end, State, Found)
    end,

    State1 = case esqlite3_utils:transaction(Transaction, Db) of
        {rollback, _}=R -> throw(R);
        S -> S
    end,

    {ok, State1}.

-spec exometer_subscribe(exometer_report:metric(), exometer_report:datapoint(), exometer_report:interval(), exometer_report:extra(), state()) -> callback_result().
exometer_subscribe(Metric, DataPoint, _Interval, _SubscribeOpts,  #state{db=Db, storages=Storages}=State) ->
    %% Initialize the storage for this metric. The storage combines the buckets   
    Store = init_storage(Metric, DataPoint, Db),
    Storages1 = dict:store(Metric, Store, Storages),

    {ok, State#state{storages=Storages1}}.

init_storage(Metric, DataPoint, Db) ->
    case esqlite3_utils:transaction(fun(TDb) -> 
                sqlite_report_bucket:init_metric_store(Metric, DataPoint, TDb) 
            end, Db) of
        {rollback, _Reason}=Rollback -> 
            throw({error, Rollback});
        Result -> 
            Result
    end.

-spec exometer_unsubscribe(exometer_report:metric(), exometer_report:datapoint(), exometer_report:extra(), state()) -> callback_result().
exometer_unsubscribe(Metric, _DataPoint, _Extra, #state{storages=Storages}=State) ->
    %% Remove the entry of this metric.
    Storages1 = dict:erase(Metric, Storages),

    {ok, State#state{storages=Storages1}}.

-spec exometer_call(any(), pid(), state()) -> {reply, any(), state()} | {noreply, state()} | any().
exometer_call(_Unknown, _From, State) ->
    {ok, State}.

-spec exometer_cast(any(), state()) -> {noreply, state()} | any().
exometer_cast(_Unknown, State) ->
    {ok, State}.

-spec exometer_info(any(), state()) -> callback_result().
exometer_info(_Unknown, State) ->
    {ok, State}.

-spec exometer_newentry(exometer:entry(), state()) -> callback_result().
exometer_newentry(_Entry,  State) ->
    {ok, State}.

-spec exometer_setopts(exometer:entry(), options(), exometer:status(), state()) -> callback_result().
exometer_setopts(_Metric, _Options, _Status, State) ->
    {ok, State}.

-spec exometer_terminate(any(), state()) -> any().
exometer_terminate(Reason, #state{db=Db}) ->
    lager:info("~p(~p): Terminating", [?MODULE, Reason]),
    ok = esqlite3:close(Db).

%%
%% Extra API
%%

-spec get_history(exometer:metric(), exometer_report:datapoint(), any())  -> list().
get_history(Metric, DataPoint) ->
    %% TODO: fix hard code db name. 
    {ok, Conn} = esqlite3:open("priv/log/metrics.db"),
    Result = get_history(Metric, DataPoint, Conn),
    esqlite3:close(Conn),
    Result.

get_history(Metric, DataPoint, Db) when is_atom(DataPoint) -> get_history(Metric, [DataPoint], Db);
get_history(Metric, DataPoint, Db) ->
    F = fun(TDb) -> get_history(Metric, DataPoint, [hour, day], TDb, []) end,
    esqlite3_utils:transaction(F, Db).

% @doc Get the historic values of a datapoint
get_history(_Metric, [], _Periods, _Db, Acc) -> lists:reverse(Acc);
get_history(Metric, [Point|Rest], Periods, Db, Acc) ->
    DataPoints = [sqlite_report_bucket:get_history(Metric, Point, Period, Db) || Period <- Periods],
    Stats = lists:zip(Periods, DataPoints),
    get_history(Metric, Rest, Periods, Db, [{{Metric, Point}, Stats} | Acc]).
   
%%
%% Helpers
%%

init_database(DbArg) ->
    {ok, Db} = esqlite3:open(DbArg),
    Db.

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

init_database_test() ->
    Db = init_database(":memory:"),
    ok.

-endif.