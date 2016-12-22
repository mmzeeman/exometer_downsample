%%
%%
%%

-module(exometer_sqlite_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("exometer_core/include/exometer.hrl").

do_test() ->
     Subscribers =  [ 
        {reporters, [ {exometer_report_sqlite, [ 
            {storage_handler, custom_handler}
            {storage_handler_args, []}
            {db_arg, ":memory:"}, 
            {report_bulk, true}
            ]}
        ]}],

    error_logger:tty(false),
    application:set_env(lager, handlers, [{lager_console_backend, none}]),
    application:set_env(exometer, report, Subscribers),

    {ok, _Apps} = application:ensure_all_started(exometer_sqlite),

    ?assertMatch([{exometer_report_sqlite, _}], exometer_report:list_reporters()),

    ok.