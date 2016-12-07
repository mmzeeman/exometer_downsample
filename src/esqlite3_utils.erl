

%% Handy utilities functions.

-module(esqlite3_utils).

-export([
    table_exists/2
]).

% @doc Return true iff a table with TableName exists.
table_exists(TableName, Db) ->
    case esqlite3:q("SELECT name FROM sqlite_master WHERE type='table' AND name=$1", [TableName], Db) of
        [{_BinTableName}] -> true;
        [] -> false
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

-endif.
