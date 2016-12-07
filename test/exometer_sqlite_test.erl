%%
%%
%%

-module(exometer_sqlite_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("exometer_core/include/exometer.hrl").

do_test() ->
     Subscribers =  [ 
        {reporters, [ {exometer_report_sqlite, [
                {db_arg, "test.db"},
                {report_bulk, true}
            ]}
        ]}],

    error_logger:tty(false),
    application:set_env(lager, handlers, [{lager_console_backend, none}]),
    application:set_env(exometer, report, Subscribers),

    {ok, _Apps} = application:ensure_all_started(exometer_sqlite),

    ?assertMatch([{exometer_report_sqlite, _}], exometer_report:list_reporters()),

    %exometer:update_or_create([metric, cpu], 1, cpu, []),
    exometer:update_or_create([metric, histogram], 1, histogram, []),
    exometer:update_or_create([metric, counter], 2, counter, []),
    %exometer:update_or_create([metric, uniform], 2, uniform, []),
    %exometer:update_or_create([metric, gauge], 2, gauge, []),
    %exometer:update_or_create([metric, spiral], 2, spiral, []),

    timer:sleep(100),

    ok = exometer_report:subscribe(exometer_report_sqlite, [metric, counter], value, 200, [], true),
    ok = exometer_report:subscribe(exometer_report_sqlite, [metric, histogram], [min, max], 230, [], true),

    timer:sleep(100),
    exometer:update([metric, counter], 13),
    exometer:update([metric, histogram], 13),
    exometer:update([metric, histogram], 75),

    io:fwrite(standard_error, "m,h: ~p~n", [exometer:get_value([metric, histogram], [min, max])]),

    timer:sleep(100),
    exometer:update([metric, counter], 26),
    timer:sleep(100),
    exometer:update([metric, counter], 26),
    timer:sleep(100),
    exometer:update([metric, counter], 26),
    timer:sleep(100),
    exometer:update([metric, counter], 26),
    timer:sleep(100),
    exometer:update([metric, counter], 26),
    timer:sleep(100),
    exometer:update([metric, counter], 26),
    timer:sleep(100),

    %% ?assertEqual(5, length(exometer_report:list_subscriptions(exometer_report_sqlite))),

    ok.