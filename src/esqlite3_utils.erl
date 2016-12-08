

%% Handy sqlite utilities functions.

-module(esqlite3_utils).

-export([
    table_exists/2,
    transaction/2
]).

% @doc Return true iff a table with TableName exists.
table_exists(TableName, Db) ->
    case esqlite3:q("SELECT name FROM sqlite_master WHERE type='table' AND name=$1", [TableName], Db) of
        [{_BinTableName}] -> true;
        [] -> false
    end.

% @doc transaction(Fun, Db)
transaction(Fun, Db) ->
    try
        case esqlite3:q("BEGIN", Db) of
            [] -> ok;
            {error, _} = ErrorBegin -> throw(ErrorBegin)
        end,

        case Fun(Db) of
            {rollback, Result} ->
                esqlite3:q("ROLLBACK", Db),
                Result;
            Result ->
                case esqlite3:q("COMMIT", Db) of
                    [] -> Result;
                    {error, _} = ErrorCommit -> 
                        throw(ErrorCommit)
                end
        end
    catch 
        _:Why ->
            esqlite3:q("ROLLBACK", Db),
            {rollback, {Why, erlang:get_stacktrace()}}
    end.


%%
%% Tests
%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

table_exists_test() ->
    {ok, Db} = esqlite3:open(":memory:"),
    ?assertEqual(false, table_exists(foo, Db)),

    esqlite3:q("create table foo(\"a\" text)", Db),
    ?assertEqual(true, table_exists(foo, Db)),
    ok.

transaction_test() ->
    {ok, Db} = esqlite3:open(":memory:"),

    ?assertEqual(ok, transaction(fun(_TDb) -> ok end, Db)),
    ?assertMatch({rollback, _}, transaction(fun(_TDb) -> 1/0 end, Db)),
    ?assertEqual(ok, transaction(fun(_TDb) -> {rollback, ok} end, Db)),

    %% Now some db changing things to see of things are actually rolled back
    ?assertEqual([], transaction(fun(TDb) ->  
        esqlite3:q("create table test(id integer primary key, name text)", TDb)
    end, Db)),
    %% Check if the commit is done.
    ?assertEqual([], esqlite3:q("select * from test", Db)),

    %% Now some db changing things to see of things are actually rolled back
    ?assertMatch({rollback, _}, transaction(fun(TDb) ->  
        esqlite3:q("create table test2(id integer primary key, name text)", TDb),
        %% Simulate a bug in code...
        1/0
    end, Db)),

    %% There should be no test2 table now.
    ?assertThrow({error, {sqlite_error, "no such table: test2"}}, esqlite3:q("select * from test2", Db)),

    ?assertEqual(oops, transaction(fun(TDb) ->  
        esqlite3:q("create table test2(id integer primary key, name text)", TDb),
        %% Explicitly rollback.
        {rollback, oops}
    end, Db)),

    %% There should be no test2 table now.
    ?assertThrow({error, {sqlite_error, "no such table: test2"}}, esqlite3:q("select * from test2", Db)),

    ok.

-endif.
