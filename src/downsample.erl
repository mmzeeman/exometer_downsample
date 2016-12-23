%%
%%
%%


-module(downsample).

-export_type([period/0]).

-type period() :: hour | day | week | month | month3 | month6 | year.
