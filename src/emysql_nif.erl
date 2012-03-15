-module(emysql_nif).

-compile(export_all).

-include("emysql.hrl").

decode_row_data( RowData, FieldList, _Acc) ->
	Row = emysql_tcp:decode_row_data( RowData, FieldList, _Acc),
	%io:format("Row=~p~n   RowData=~p~n", [Row, RowData]),
	Row.
	
load_nif(Path) ->
	Result = erlang:load_nif( Path, 0),
	io:format("erlang:load_nif(~p) -> ~p~n", [Path, Result]),
	Result.
