%%
%%
%%

-module(exometer_downsample_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("exometer_core/include/exometer.hrl").

do_test() ->
     Subscribers =  [ 
        {reporters, [ {exometer_report_downsample, [ 
            {handler, downsample_handler_test},
            {handler_args, []},
            {report_bulk, true}
            ]}
        ]}],

    error_logger:tty(false),
    application:set_env(lager, handlers, [{lager_console_backend, none}]),
    application:set_env(exometer, report, Subscribers),

    {ok, _Apps} = application:ensure_all_started(exometer_downsample),

    ?assertMatch([{exometer_report_downsample, _}], exometer_report:list_reporters()),

    ok.