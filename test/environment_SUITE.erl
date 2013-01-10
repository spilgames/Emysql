%%%-------------------------------------------------------------------
%%% File     : Emysql/test/environment_SUITE.erl
%%% Descr    : Suite #1 - testing the test setup, db and pathes =
%%%            availability of crypto app, emysql app and test db. 
%%% Author   : H. Diedrich
%%% Created  : 12/13/2011 hd
%%% Requires : Erlang 14B (prior may not have ct_run)
%%%-------------------------------------------------------------------
%%%
%%% THIS SUITE DOES NO ACTUAL TESTS BUT CHECKS THE TEST DATABASE ETC.
%%% Test Cases are in this high granularity for clear failure reports.
%%%
%%% Run from Emysql/: 
%%%     make test
%%%
%%% Results see:
%%%     test/index.html
%%%
%%%-------------------------------------------------------------------

-define(POOL, environment_test_pool).

-module(environment_SUITE).
-include_lib("common_test/include/ct.hrl").

-include("include/emysql.hrl").

-export([
        all/0,
        init_per_testcase/2,
        end_per_testcase/2,

        initializing_crypto_app/1,
        initializing_emysql_app/1,
        accessing_emysql_module/1,

        connecting_to_db_and_creating_a_pool_transition/1,
        insert_a_record/1,
        select_a_record/1,

        add_pool_undefined/1,
        add_pool_empty_bin/1,
        add_pool_empty_list/1,
        add_pool_database/1,
        add_pool_utf8/1,
        add_pool_latin1/1,
        add_pool_latin1_compatible/1,
        add_pool_time_zone/1
    ]).

% List of test cases.
% Test cases have self explanatory names.
%%--------------------------------------------------------------------
all() -> 
    [
        initializing_crypto_app,
        initializing_emysql_app,
        accessing_emysql_module,
        connecting_to_db_and_creating_a_pool_transition,
        insert_a_record,
        select_a_record,

        add_pool_undefined,
        add_pool_empty_bin,
        add_pool_empty_list,
        add_pool_database,
        add_pool_utf8,
        add_pool_latin1,
        add_pool_latin1_compatible,
        add_pool_time_zone
    ].

init_per_testcase(T, Config) when
        T == connecting_to_db_and_creating_a_pool_transition orelse
        T == insert_a_record orelse
        T == select_a_record ->
    emysql:add_pool(?POOL, 10, "hello_username",
        "hello_password", "localhost", 3306, "hello_database", utf8),
    Config;

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(T, _) when
        T == connecting_to_db_and_creating_a_pool_transition orelse
        T == insert_a_record orelse
        T == select_a_record orelse
        T == add_pool_undefined orelse
        T == add_pool_empty_bin orelse
        T == add_pool_empty_list orelse
        T == add_pool_database orelse
        T == add_pool_utf8 orelse
        T == add_pool_latin1 orelse
        T == add_pool_latin1_compatible orelse
        T == add_pool_time_zone ->
	emysql:remove_pool(?POOL);

end_per_testcase(_, _) ->
    ok.

% Test Case: Test if the crypt app is available. This detects a path error.
%%--------------------------------------------------------------------
initializing_crypto_app(_) ->
    crypto:start(),
    ok.

% Test Case: Test if the emysql app is available. This detects a path error.
%%--------------------------------------------------------------------
initializing_emysql_app(_) ->
    application:start(emysql),
    ok.

% Test Case: Test if the emysql module is available. This detects a path error.
%%--------------------------------------------------------------------
accessing_emysql_module(_) ->
    emysql:modules(),
    ok.
%% Test case: test obsolete transitional API
%%--------------------------------------------------------------------
connecting_to_db_and_creating_a_pool_transition(_) ->
    #result_packet{rows=[[<<"hello_database">>]]} =
    emysql:execute(?POOL, <<"SELECT DATABASE();">>),
    #result_packet{rows=[[<<"utf8">>]]} =
    emysql:execute(?POOL, <<"SELECT @@character_set_connection;">>).

% Test Case: Test if we can insert a record.
%%--------------------------------------------------------------------
insert_a_record(_) ->
    #ok_packet{} = emysql:execute(?POOL, <<"DELETE FROM hello_table">>),
    #ok_packet{} = emysql:execute(?POOL,
        <<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>).

% Test Case: Test if we can select records.
%%--------------------------------------------------------------------
select_a_record(_) ->
    #result_packet{rows=[[<<"Hello World!">>]]} =
    emysql:execute(?POOL, <<"select hello_text from hello_table">>).

% Test Case: Test if we can connect to the test db and create a connection pool.
%%--------------------------------------------------------------------
add_pool_undefined(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, undefined),
    #result_packet{rows=[[undefined]]} =
    emysql:execute(?POOL, <<"SELECT DATABASE();">>),
    #result_packet{rows=[[<<"utf8">>]]} =
    emysql:execute(?POOL, <<"SELECT @@character_set_connection;">>).

add_pool_empty_bin(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, <<"">>),
    #result_packet{rows=[[undefined]]} =
    emysql:execute(?POOL, <<"SELECT DATABASE();">>).

add_pool_empty_list(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, ""),
    #result_packet{rows=[[undefined]]} =
    emysql:execute(?POOL, <<"SELECT DATABASE();">>).

add_pool_database(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, "hello_database"),
    #result_packet{rows=[[<<"hello_database">>]]} =
    emysql:execute(?POOL, <<"SELECT DATABASE();">>).

add_pool_utf8(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, undefined, [{encoding, utf8}]),
    #result_packet{rows=[[<<"utf8">>]]} =
    emysql:execute(?POOL, <<"SELECT @@character_set_connection;">>).

add_pool_latin1(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, undefined, [{encoding, latin1}]),
    #result_packet{rows=[[<<"latin1">>]]} =
    emysql:execute(?POOL, <<"SELECT @@character_set_connection;">>).

add_pool_latin1_compatible(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, undefined, latin1),
    #result_packet{rows=[[<<"latin1">>]]} =
    emysql:execute(?POOL, <<"SELECT @@character_set_connection;">>).

add_pool_time_zone(_) ->
    emysql:add_pool(?POOL, 10, "hello_username", "hello_password", "localhost",
        3306, undefined, [{time_zone, "+00:00"}]),
    #result_packet{rows=[[<<"+00:00">>]]} =
    emysql:execute(?POOL, <<"SELECT @@time_zone;">>).
