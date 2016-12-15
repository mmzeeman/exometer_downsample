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
    {db_arg, DbArg} = proplists:lookup(db_arg, Opts),
    {report_bulk, true} = proplists:lookup(report_bulk, Opts),
    Db = init_database(DbArg),
    Storages = dict:new(),
    {ok, #state{db_arg = DbArg, db = Db, storages = Storages}}.

-spec exometer_report(exometer_report:metric(), exometer_report:datapoint(), exometer_report:extra(), value(), state()) -> callback_result().
exometer_report(Metric, DataPoint, Extra, Value, #state{db=Db}=State) ->
    lager:warning("Use {report_bulk, true}."),
    {ok, State}.

exometer_report_bulk(Found, Extra,  #state{db=Db}=State) ->
    Transaction = fun(TDb) ->
        InsertDb = fun(Query, Args) -> esqlite3:q(Query, Args, TDb) end,
        lists:foldl(fun({Metric, Values}, #state{storages=Storages}=S) -> 
             Storages1 = dict:update(Metric, fun(Store) -> 
                sqlite_report_bucket:insert(Store, Values, InsertDb) 
                end, Storages),
            S#state{storages=Storages1}
        end, State, Found)
    end,

    State1 = esqlite3_utils:transaction(Transaction, Db),

    {ok, State1}.

-spec exometer_subscribe(exometer_report:metric(), exometer_report:datapoint(), exometer_report:interval(), exometer_report:extra(), state()) -> callback_result().
exometer_subscribe(Metric, DataPoint, Interval, SubscribeOpts,  #state{db=Db, storages=Storages}=State) ->
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
exometer_unsubscribe(Metric, DataPoint, Extra, #state{storages=Storages}=State) ->
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
exometer_newentry(#exometer_entry{name=Name, type=Type},  #state{db=Db}=State) ->
    {ok, State}.

-spec exometer_setopts(exometer:entry(), options(), exometer:status(), state()) -> callback_result().
exometer_setopts(_Metric, _Options, _Status, State) ->
    {ok, State}.

-spec exometer_terminate(any(), state()) -> any().
exometer_terminate(Reason, #state{db=Db}) ->
    ok = esqlite3:close(Db).
   
%%
%% Helpers
%%

init_database(DbArg) ->
    {ok, Db} = esqlite3:open(DbArg),
    Db.

%%insert(Name, DataPoint, Value, Counter, Db) when is_atom(DataPoint) ->
%%    insert(Name, [DataPoint], [Value], Counter, Db);
%%insert(Name, DataPoints, Values, Counter, Db) ->
%%    TableName = table_name(Name),
%%    Values1 = [Counter, unix_time() | Values],
%%    QuestionMarks = bjoin([ <<$?>> || _V <- Values1]),
%%    io:fwrite(standard_error, "insert: ~p, ~p, ~p~n", [DataPoints, QuestionMarks, Values1]),
%%    esqlite3:q(<<"insert into ", TableName/binary, " (sample, time, value) values (", QuestionMarks/binary, ")">>, Values1, Db).

%%
%% Time
%% 

unix_time() ->
    {Mega, Secs, _} = os:timestamp(),
    Mega * 1000000 + Secs.

%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

init_database_test() ->
    Db = init_database(":memory:"),
    ok.

-endif.