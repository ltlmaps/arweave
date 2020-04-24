-module(ar_kv).

-include("ar.hrl").

-export([
	open/2,
	create_column_family/3,
	close/1,
	put/3,
	get/2,
	get_next/2,
	get_prev/2,
	get_range/2,
	get_range/3,
	delete/2,
	delete_range/3,
	destroy/1
]).

open(Name, CFDescriptors) ->
	RocksDBDir = filename:join(ar_meta_db:get(data_dir), ?ROCKS_DB_DIR),
	Filename = filename:join(RocksDBDir, Name),
	ok = filelib:ensure_dir(Filename ++ "/"),
	Opts = [{create_if_missing, true}, {create_missing_column_families, true}],
	rocksdb:open(Filename, Opts, CFDescriptors).

create_column_family(DB, Name, Opts) ->
	rocksdb:create_column_family(DB, Name, Opts).

close(DB) ->
	rocksdb:close(DB).

put({DB, CF}, Key, Value) ->
	rocksdb:put(DB, CF, Key, Value, []).

get({DB, CF}, Key) ->
	rocksdb:get(DB, CF, Key, []).

get_next({DB, CF}, OffsetBinary) ->
	case rocksdb:iterator(DB, CF, []) of
		{ok, Iterator} ->
			rocksdb:iterator_move(Iterator, {seek, OffsetBinary});
		Error ->
			Error
	end.

get_prev({DB, CF}, OffsetBinary) ->
	case rocksdb:iterator(DB, CF, []) of
		{ok, Iterator} ->
			rocksdb:iterator_move(Iterator, {seek_for_prev, OffsetBinary});
		Error ->
			Error
	end.

get_range({DB, CF}, StartOffsetBinary) ->
	case rocksdb:iterator(DB, CF, []) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, Value} ->
					get_range2(Iterator, #{ Key => Value });
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end.

get_range({DB, CF}, StartOffsetBinary, EndOffsetBinary) ->
	case rocksdb:iterator(DB, CF, []) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, _Value} when Key > EndOffsetBinary ->
					{ok, #{}};
				{ok, Key, Value} ->
					get_range2(Iterator, #{ Key => Value }, EndOffsetBinary);
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end.

delete({DB, CF}, Key) ->
	rocksdb:delete(DB, CF, Key, []).

delete_range({DB, CF}, StartKey, EndKey) ->
	rocksdb:delete_range(DB, CF, StartKey, EndKey, []).

destroy(Name) ->
	RocksDBDir = filename:join(ar_meta_db:get(data_dir), ?ROCKS_DB_DIR),
	Filename = filename:join(RocksDBDir, Name),
	case filelib:is_dir(Filename) of
		true ->
			rocksdb:destroy(Filename, []);
		false ->
			ok
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_range2(Iterator, Map) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, Value} ->
			get_range2(Iterator, Map#{ Key => Value });
		{error, invalid_iterator} ->
			{ok, Map};
		{error, Reason} ->
			{error, Reason}
	end.

get_range2(Iterator, Map, EndOffsetBinary) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, _Value} when Key > EndOffsetBinary ->
			{ok, Map};
		{ok, Key, Value} ->
			get_range2(Iterator, Map#{ Key => Value }, EndOffsetBinary);
		{error, invalid_iterator} ->
			{ok, Map};
		{error, Reason} ->
			{error, Reason}
	end.
