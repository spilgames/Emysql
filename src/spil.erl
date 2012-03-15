-module(spil).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emysql/include/emysql.hrl").

test1_test() ->
    setup_pool(),
    Ct=100000,
    {U,_R} =timer:tc(emysql,execute, [p, "select * from profile limit "++integer_to_list(Ct)]),
    ?debugFmt("Reading ~p rows took ~6.2fs~n", [ Ct, U * 1.0E-6]),
    ok.

setup_pool() ->
    crypto:start(),
    application:start(emysql),
    case [P || #pool{pool_id=P} <- emysql_conn_mgr:pools(), P=:=p] of
        [] -> emysql:add_pool( p, 1, "bart", "bart", "erlang6", 3306, "versions_and_shards", utf8);
        _ -> ?debugMsg("Pool p already existing~n")
    end.

setup_table() ->
    setup_pool(),
    emysql:execute(p, "DROP TABLE `test1`"),
    emysql:execute(p, 
        "CREATE TABLE `test1` ("
        "  `bigint` bigint(20) NOT NULL,"
        "  `int` int(11) NOT NULL,"
        "  `tinyint` tinyint(4) NOT NULL,"
        "  `text` text NOT NULL,"
        "  `enum` enum('a','b','c','1','2','3') NOT NULL,"
        "  `varchar100` varchar(100) NOT NULL"
        ") " ),

    populate(100000).

populate(0) ->
    ok;
populate(Ct) when Ct > 0->
    Query = lists:flatten(io_lib:format("insert into `test1` values ( ~B, ~B, ~B, '~s', '~s', '~s' )",
        [ 123456789012345600+Ct, 123456700+Ct, Ct rem 10000, long_string(Ct), abc(Ct), long_string(Ct,100)])) ,

    #ok_packet{} = emysql:execute(p, Query),
    populate(Ct-1).
    

long_string(Ct) when is_integer(Ct) ->
    I = random:uniform(20) + 20 ,
    string:join( [ integer_to_list(100000+Ct) || _ <- lists:seq(1,I)], "," ).

long_string(Ct, Length) ->
    long_string(Ct, Length, "").

long_string(Ct, Length, S) ->
    L = length(S),
    if
        L > Length ->
            string:left(S, Length);
        L =:= Length ->
            S;
        true ->
            long_string(Ct, Length, S ++ integer_to_list(Ct) )
    end.
    
abc(Ct) ->
    C = Ct rem 6,
    case C of
        0 -> "a";
        1 -> "b";
        2 -> "c";
        3 -> "1";
        4 -> "2";
        5 -> "3"
    end.

    







    




    
% vim:tw=120:ts=4:expandtab
