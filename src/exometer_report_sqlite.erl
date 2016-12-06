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

    sample_ids
}).

-record(metric, {
    name,

    % buckets 
    now_bucket,
    hour_bucket,
    day_bucket,
    week_bucket,
    month,
    month3,
    month6,
    year
}).

-record(bucket, {
    name,
    table_name,

    history 
}).

-record(sample, {
    time
}).

-type state() :: #state{}.
-type value() :: any().
-type options() :: any().
-type callback_result() ::  {ok, state()}.

-spec exometer_init(options()) -> callback_result().
exometer_init(Opts) ->
    {db_arg, DbArg} = proplists:lookup(db_arg, Opts),
    Db = init_database(DbArg),
    SampleIds = dict:new(),
    {ok, #state{db_arg = DbArg, db = Db, sample_ids = SampleIds}}.

-spec exometer_report(exometer_report:metric(), exometer_report:datapoint(), exometer_report:extra(), value(), state()) -> callback_result().
exometer_report(Metric, DataPoint, Extra, Value, #state{db=Db, sample_ids=SIds}=State) ->
    io:fwrite(standard_error, "report: ~p~n", [{self(), Metric, DataPoint, Extra, Value}]),

    {ok, Counter} = dict:find(Metric, SIds),

    insert(Metric, DataPoint, Value, Counter+1, Db),
    SIds1 = dict:update_counter(Metric, 1, SIds),

    {ok, State#state{sample_ids=SIds1}}.
    
-spec exometer_subscribe(exometer_report:metric(), exometer_report:datapoint(), exometer_report:interval(), exometer_report:extra(), state()) -> callback_result().
exometer_subscribe(Metric, DataPoint, Interval, SubscribeOpts,  #state{db=Db, sample_ids=SIds}=State) ->
    io:fwrite(standard_error, "subscribe: ~p~n", [{self(), Metric, DataPoint, Interval, SubscribeOpts}]),

    SampleId = ensure_dp_table(Metric, DataPoint, Db),
    SIds1 = dict:store(Metric, SampleId, SIds),

    {ok, State#state{sample_ids=SIds1}}.

-spec exometer_unsubscribe(exometer_report:metric(), exometer_report:datapoint(), exometer_report:extra(), state()) -> callback_result().
exometer_unsubscribe(Metric, DataPoint, Extra, State) ->
    io:fwrite(standard_error, "unsubscribe: ~p~n", [{Metric, DataPoint, Extra}]),
    {ok, State}.

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
    ensure_entry_table(Db),
    Db.

insert(Name, DataPoint, Value,Counter, Db) when is_atom(DataPoint) ->
    insert(Name, [DataPoint], [Value], Counter, Db);
insert(Name, DataPoints, Values, Counter, Db) ->
    TableName = table_name(Name),
    Values1 = [Counter, unix_time() | Values],
    esqlite3:q(<<"insert into ", TableName/binary, " (sample, time, value) values (?, ?, ?)">>, Values1, Db).

ensure_dp_table(Name, DpInfo, Db) when is_atom(DpInfo) -> ensure_dp_table(Name, [DpInfo], Db);
ensure_dp_table(Name, DpInfo, Db) ->
    TableName = table_name(Name),
    case table_exists(TableName, Db) of
        true -> 
            R = esqlite3:q(<<"select sample from ", TableName/binary,  " order by sample desc limit 1">>, Db),
            case R of
                [] -> 0;
                [{SampleId}] -> SampleId
            end;
        false -> 
            create_dp_tables(Name, DpInfo, Db),
            0
    end.

table_exists(TableName, Db) ->
    case esqlite3:q("SELECT name FROM sqlite_master WHERE type='table' AND name=$1", [TableName], Db) of
        [{_BinTableName}] -> true;
        [] -> false
    end.
   
ensure_entry_table(Db) ->
    case table_exists(entry, Db) of
        true -> ok;
        false -> create_entry_table(Db)
    end.

create_dp_tables(Name, DpInfo, Db) ->
    TableName = table_name(Name),
    Defs = [<<"sample double PRIMARY KEY NOT NULL">>, <<"time double NOT NULL">> | column_defs(DpInfo)],
    SqlDef = bjoin(Defs, <<$,>>),
    esqlite3:q(<<"CREATE TABLE ", TableName/binary, $(, SqlDef/binary,  $)>>, [], Db).

column_defs(DpInfo) -> column_defs(DpInfo, []).

column_defs([], Acc)-> lists:reverse(Acc);
column_defs([H|T], Acc) when is_atom(H) ->
    Name = atom_to_binary(H, utf8),
    column_defs(T, [<<Name/binary, 32, "double NOT NULL">> | Acc]);
column_defs([H|T], Acc) when is_integer(H) ->
    Name = integer_to_binary(H, 10),
    column_defs(T, [<<$_, Name/binary, 32, "double NOT NULL">> | Acc]).

create_entry_table(Db) ->
    esqlite3:exec("CREATE TABLE entry(\"type\" text)", Db).

-spec table_name(exometer:name()) -> binary().
table_name(EntryName) ->
    table_name(EntryName, <<>>).

table_name([], Acc) -> Acc;
table_name([H|T], <<>>) ->
    table_name(T, erlang:atom_to_binary(H, utf8));
table_name([H|T], Acc) ->
    B = erlang:atom_to_binary(H, utf8),
    table_name(T, <<Acc/binary, $$, B/binary>>).

bjoin(List, Separator) -> bjoin(List, Separator, <<>>).

bjoin([], _Separator, Acc) -> Acc;
bjoin([H|T], Separator, <<>>) -> bjoin(T, Separator, H);
bjoin([H|T], Separator, Acc) -> bjoin(T, Separator, <<Acc/binary, Separator/binary, H/binary>>).

%%
%% Time
%% 

unix_time() ->
    {Mega, Secs, _} = os:timestamp(),
    Mega * 1000000 + Secs.

%% n = #samples in table.
%% the interval is #seconds in period 
%% (3600 / n) - hour
%% (86400 / n)  - day
%%  
%%

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

init_database_test() ->
    Db = init_database(":memory:"),
    ok.

table_exists_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ?assertEqual(false, table_exists(foo, Db)),

    esqlite3:q("create table foo(\"a\" text)", Db),
    ?assertEqual(true, table_exists(foo, Db)),
    ok.

table_name_test() ->
    ?assertEqual(<<"test$test">>, table_name([test, test])),
    ?assertEqual(<<"a$b$c">>, table_name([a, b, c])),
    ?assertEqual(<<"a$b$c$d">>, table_name([a, b, c, d])),
    ok.

create_dp_tables_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    create_dp_tables([test, a], [nprocs,avg1,avg5,avg15], Db),
    ?assertEqual([{2}], esqlite3:q("SELECT count(*) FROM sqlite_master", Db)),
    ok.

column_defs_test() ->
    ?assertEqual([<<"nprocs double NOT NULL">>,<<"avg1 double NOT NULL">>,<<"avg5 double NOT NULL">>,
                      <<"avg15 double NOT NULL">>], column_defs([nprocs,avg1,avg5,avg15])),
    ?assertEqual([<<"_50 double NOT NULL">>,<<"_70 double NOT NULL">>,<<"_90 double NOT NULL">>,
                      <<"_99 double NOT NULL">>], column_defs([50,70,90,99])),
    ok.

ensure_dp_table_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    ?assertEqual(0, ensure_dp_table([a,b], [v], Db)),

    ok.

-endif.