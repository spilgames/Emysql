%%%-------------------------------------------------------------------
%%% File     : Emysql/test/basics_SUITE.erl
%%% Descr    : Suite #2: Tests of basic SQL statements,
%%%            prepared statements, stored procedures 
%%% Author   : H. Diedrich
%%% Created  : 12/13/2011 hd
%%% Requires : Erlang 14B (prior may not have ct_run)
%%%-------------------------------------------------------------------
%%%
%%% Run from Emysql/: 
%%%     make test
%%%
%%% Results see:
%%%     test/index.html
%%%
%%%-------------------------------------------------------------------

-module(basics_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

-include_lib("../include/emysql.hrl").

-record(hello_record, {hello_text}).

%% Optional suite settings
%%--------------------------------------------------------------------
%% Function: suite() -> Info
%% Info = [tuple()]
%%--------------------------------------------------------------------

suite() ->
    [{timetrap,{seconds,30}}].

%% Mandatory list of test cases and test groups, and skip orders. 
%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%%--------------------------------------------------------------------

all() -> 
    [delete_all,
     encode_atoms,
     insert_only,
     insert_and_read_back,
     insert_and_read_back_as_recs,
     same_query,
     same_query_timeout,
     same_query_prepared_statement,
     same_query_error,
     select_by_prepared_statement,
	 delete_non_existant_procedure,
	 select_by_stored_procedure,
     encode_floating_point_data].


%% Optional suite pre test initialization
%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------

init_per_suite(Config) ->

	% if this fails, focus on environment_SUITE to fix test setup.
    crypto:start(),
    application:start(emysql),
    emysql:add_pool(test_pool, 1,
        "hello_username", "hello_password", "localhost", 3306,
        "hello_database", utf8),
    emysql:add_pool(test_pool_many, 10,
        "hello_username", "hello_password", "localhost", 3306,
        "hello_database", utf8),
    Config.

%% Optional suite post test wind down
%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------

end_per_suite(_Config) ->
	emysql:remove_pool(test_pool),
	emysql:remove_pool(test_pool_many),
    ok.

%% A test case. The ok is irrelevant. What matters is, if it returns.
%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%%--------------------------------------------------------------------

%% Test Case: Delete all records in the test database
%%--------------------------------------------------------------------
delete_all(_) ->
	
    emysql:execute(test_pool, <<"DELETE FROM hello_table">>),
    ok.


%% Test Case: Make an Insert
%%--------------------------------------------------------------------
insert_only(_) ->
	
    emysql:execute(test_pool,
        <<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>),

    ok.

%% Test Case: Allow insertion of atom values through the encoder
%%--------------------------------------------------------------------
encode_atoms(_Config) ->
    emysql:execute(test_pool, <<"DROP TABLE encode_atoms_test">>),
    emysql:execute(test_pool, <<"CREATE TABLE encode_atoms_test (x VARCHAR(32))">>),

    emysql:prepare(encode_atoms, <<"INSERT INTO encode_atoms_test (x) VALUES (?)">>),
    Result = emysql:execute(test_pool, encode_atoms, [foo]),
    ct:log("Result: ~p", [Result]),

    ok_packet = element(1, Result),
    ok.

%% Test Case: Make an Insert and Select it back
%%--------------------------------------------------------------------
insert_and_read_back(_) ->
	
    emysql:execute(test_pool, <<"DELETE FROM hello_table">>),

    emysql:execute(test_pool,
        <<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>),

    Result = emysql:execute(test_pool,
        <<"select hello_text from hello_table">>),
	
	% find this output by clicking on the test name, then case name in test/index.html
	ct:log("~p~n", [Result]),

	% the test
	Result = {result_packet,5,
               [{field,2,<<"def">>,<<"hello_database">>,<<"hello_table">>,
                       <<"hello_table">>,<<"hello_text">>,<<"hello_text">>,
                       254,<<>>,33,60,0,0}],
               [[<<"Hello World!">>]],
               <<>>},
    
    ok.

same_query(_) ->
    #ok_packet{} = emysql:execute(test_pool, <<"DELETE FROM hello_table">>),
    [#ok_packet{}, #result_packet{rows=[[1]]}] = emysql:execute_many(
        test_pool_many, [
            <<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>,
            <<"SELECT ROW_COUNT()">>
        ]).

same_query_prepared_statement(_) ->
    emysql:execute(test_pool_many, <<"DELETE FROM hello_table">>),

	emysql:prepare(test_stmt,
		<<"INSERT INTO hello_table SET hello_text=?">>),

    [#ok_packet{}, #result_packet{rows=[[1]]}] = emysql:execute_many(
        test_pool_many, [
            {test_stmt, ["Hello, world!"]},
            {<<"SELECT ROW_COUNT()">>, []}
        ]
    ).

same_query_timeout(_) ->
    {Pid, MRef} = spawn_monitor(fun() ->
                Q = [<<"select sleep(1)">>],
                emysql:execute_many(test_pool_many, Q, 10)
        end
    ),
    receive
        {'DOWN', MRef, process, Pid, mysql_timeout} ->
            ok
    after 100 ->
            exit(message_timeout)
    end.

%% Make sure queries are not executed in case of failure
same_query_error(_) ->
    [#error_packet{}] = emysql:execute_many(
        test_pool_many, [
            <<"syntax error">>,
            <<"select 1">>
        ]).

%% Test Case: Encode floating point data into a test table (Issue 57)
encode_floating_point_data(_Config) ->
    emysql:execute(test_pool, <<"DROP TABLE float_test">>),
    emysql:execute(test_pool, <<"CREATE TABLE float_test ( x FLOAT )">>),
    emysql:prepare(encode_float_stmt, <<"INSERT INTO float_test (x) VALUES (?)">>),
    
    Result = emysql:execute(test_pool, encode_float_stmt, [3.14]),

    ct:log("Result: ~p", [Result]),
    ok_packet = element(1, Result),
    ok.

%% Test Case: Make an Insert and Select it back, reading out as Record
%%--------------------------------------------------------------------
insert_and_read_back_as_recs(_) ->
	
    emysql:execute(test_pool, <<"DELETE FROM hello_table">>),

	emysql:execute(test_pool,
		<<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>),

	Result = emysql:execute(test_pool, <<"SELECT * from hello_table">>),

	Recs = emysql_util:as_record(
		Result, hello_record, record_info(fields, hello_record)),

	% find this output by clicking on the test name, then case name in test/index.html
	ct:log("~p~n", [Recs]),

	% the test
	Recs = [{hello_record,<<"Hello World!">>}],

	ok.
	

%% Test Case: Create a Prepared Statement and make a Select with it
%%--------------------------------------------------------------------
select_by_prepared_statement(_) ->

    emysql:execute(test_pool, <<"DELETE FROM hello_table">>),

	emysql:execute(test_pool,
		<<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>),

	emysql:prepare(test_stmt, 
		<<"SELECT * from hello_table WHERE hello_text like ?">>),

	Result = emysql:execute(test_pool, test_stmt, ["Hello%"]),

	% find this output by clicking on the test name, then case name in test/index.html
	ct:log("Result: ~p~n", [Result]),

	% the test
	Result = {result_packet,5,
                       [{field,2,<<"def">>,<<"hello_database">>,
                               <<"hello_table">>,<<"hello_table">>,
                               <<"hello_text">>,<<"hello_text">>,254,<<>>,33,
                               60,0,0}],
                       [[<<"Hello World!">>]],
                       <<>>},

    ok.



%% Test Case: Delete a non-existant Stored Procedure
%%--------------------------------------------------------------------
delete_non_existant_procedure(_) ->

	Result1 = emysql:execute(test_pool,
	  	<<"drop procedure sp_me_no_exist">>),
	  	% note: returns ok even if sp_hello does not exist

	% find this output by clicking on the test name, then case name in test/index.html
	ct:log("~p~n", [Result1]),

	% test
	Result1 = {error_packet,1,1305,<<"42000">>,
              "PROCEDURE hello_database.sp_me_no_exist does not exist"},

	ok.

%% Test Case: Create a Stored Procedure and make a Select with it
%%--------------------------------------------------------------------
select_by_stored_procedure(_) ->

    emysql:execute(test_pool, <<"DELETE FROM hello_table">>),

	emysql:execute(test_pool,
		<<"INSERT INTO hello_table SET hello_text = 'Hello World!'">>),

	Result1 = emysql:execute(test_pool,
	  	<<"drop procedure sp_hello">>),
	  	% note: returns ok even if sp_hello does not exist

	% find this output by clicking on the test name, then case name in test/index.html
	ct:log("~p~n", [Result1]),

	% first test
	case Result1 of
		{ok_packet,1,0,0,_,0,[]} -> ok;
		{error_packet,1,1305,<<"42000">>,
              "PROCEDURE hello_database.sp_hello does not exist"} -> ok
    end,

	Result2 = emysql:execute(test_pool,
	  	<<"create procedure sp_hello() begin select * from hello_table limit 2; end">>),

	% find this output by clicking on the test name, then case name in test/index.html
	ct:log("~p~n", [Result2]),

	% second test
	{ok_packet,1,0,0,_,0,[]} = Result2,

	Result3 = emysql:execute(test_pool,
	   	<<"call sp_hello();">>),

	% find this output by clicking on the test name, then case name in test/index.html
	ct:log("~p~n", [Result3]),
	
	% third, main test
	[{result_packet,5,
              		[{field,2,<<"def">>,<<"hello_database">>,<<"hello_table">>,
                        <<"hello_table">>,<<"hello_text">>,<<"hello_text">>,
                        254,<<>>,33,60,0,0}],
                	[[<<"Hello World!">>]],
                	<<>>},
			   {ok_packet,6,0,0,_,0,[]}]
			   = Result3,
	
	ok.
