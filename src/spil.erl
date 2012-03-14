-module(spil).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emysql/include/emysql.hrl").

test1_test() ->
    crypto:start(),
    application:stop(emysql),
    application:start(emysql),
    Ct=100000,
    emysql:add_pool( p, 1, "bart", "bart", "erlang6", 3306, "versions_and_shards", utf8),
    {U,_} =timer:tc(emysql,execute, [p, "select * from profile limit "++integer_to_list(Ct)]),
    ?debugFmt("Reading ~p rows took ~6.2fs~n", [ Ct, U * 1.0E-6]),
    
    application:stop(emysql).


% vim:tw=120:ts=4:expandtab
