-module(ar_data_sync).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_cast/2, handle_call/3]).
-export([terminate/2]).

-export([
	add_block/3,
	add_chunk/1,
	get_chunk/1,
	get_tx_data/1,
	get_tx_offset/1,
	get_sync_record_etf/0,
	get_sync_record_json/0
]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

add_block(SizeTaggedTXs, BI, BlockStartOffset) ->
	gen_server:cast(?MODULE, {add_block, SizeTaggedTXs, BI, BlockStartOffset}).

add_chunk(
	#{
		data_root := DataRoot,
		offset := Offset,
		data_path := DataPath,
		chunk := Chunk
	}
) ->
	gen_server:call(?MODULE, {add_chunk, DataRoot, DataPath, Chunk, Offset}).

get_chunk(Offset) ->
	gen_server:call(?MODULE, {get_chunk, Offset}).

get_tx_data(TXID) ->
	gen_server:call(?MODULE, {get_tx_data, TXID}).

get_tx_offset(TXID) ->
	gen_server:call(?MODULE, {get_tx_offset, TXID}).

get_sync_record_etf() ->
	gen_server:call(?MODULE, get_sync_record_etf).

get_sync_record_json() ->
	gen_server:call(?MODULE, get_sync_record_json).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

-ifdef(DEBUG).
init([]) ->
	ar:info([{event, ar_data_sync_start}]),
	process_flag(trap_exit, true),
	gen_server:cast(self(), reset),
	gen_server:cast(self(), init),
	{ok, #sync_data_state{ status = not_joined }}.
-else.
init([]) ->
	ar:info([{event, ar_data_sync_start}]),
	process_flag(trap_exit, true),
	gen_server:cast(self(), init),
	{ok, #sync_data_state{ status = not_joined }}.
-endif.

handle_cast(reset, State) ->
	ar_storage:delete_term(data_sync_state),
	ar_kv:destroy("ar_data_sync_db"),
	{noreply, State};

handle_cast(init, State) ->
	case whereis(http_entrypoint_node) of
		undefined ->
			timer:sleep(200),
			gen_server:cast(self(), init),
			{noreply, State};
		PID ->
			case ar_node:get_block_index(PID) of
				[] ->
					timer:sleep(200),
					gen_server:cast(self(), init),
					{noreply, State};
				BI ->
					do_init(BI)
			end
	end;

handle_cast(Msg, #sync_data_state{ status = not_joined } = State) ->
	gen_server:cast(self(), Msg),
	timer:sleep(200),
	{noreply, State};

handle_cast({add_block, SizeTaggedTXs, BI, BlockStartOffset}, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex,
		sync_record = SyncRecord
	} = State,
	{ok, UpdatedState} = remove_orphaned_data(State, BlockStartOffset),
	ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset),
	WeaveSize = case SizeTaggedTXs of
		[] ->
			BlockStartOffset;
		_ ->
			{_, EndOffset} = lists:last(SizeTaggedTXs),
			BlockStartOffset + EndOffset
	end,
	UpdatedState2 = UpdatedState#sync_data_state{
		weave_size = WeaveSize,
		sync_record = ar_intervals:cut(SyncRecord, BlockStartOffset),
		block_index = BI
	},
	{ok, UpdatedState3} =
		pick_up_orphaned_chunks(UpdatedState2, SizeTaggedTXs, BlockStartOffset),
	ok = store_sync_state(UpdatedState3),
	{noreply, UpdatedState3};

handle_cast(update_peer_sync_records, State) ->
	case whereis(http_bridge_node) of
		undefined ->
			timer:apply_after(200, gen_server, cast, [self(), update_peer_sync_records]);
		Bridge ->
			Peers = ar_bridge:get_remote_peers(Bridge),
			BestPeers = pick_random_peers(Peers, ?BEST_PEERS_COUNT),
			Self = self(),
			spawn(
				fun() ->
					PeerSyncRecords = lists:foldl(
						fun(Peer, Acc) ->
							case ar_http_iface_client:get_sync_record(Peer) of
								{ok, SyncRecord} ->
									maps:put(Peer, SyncRecord, Acc);
								_ ->
									Acc
							end
						end,
						#{},
						BestPeers
					),
					gen_server:cast(Self, {update_peer_sync_records, PeerSyncRecords})
				end
			)
	end,
	{noreply, State};

handle_cast({update_peer_sync_records, PeerSyncRecords}, State) ->
	timer:apply_after(
		?PEER_SYNC_RECORDS_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), update_peer_sync_records]
	),
	{noreply, State#sync_data_state{
		peer_sync_records = PeerSyncRecords
	}};

%% Pick a random not synced interval and sync it.
handle_cast(sync_random_interval, State) ->
	#sync_data_state{
		sync_record = SyncRecord,
		weave_size = WeaveSize,
		peer_sync_records = PeerSyncRecords
	} = State,
	case get_random_interval(SyncRecord, PeerSyncRecords, WeaveSize) of
		none ->
			timer:apply_after(
				?PAUSE_AFTER_COULD_NOT_FIND_CHUNK_MS,
				gen_server,
				cast,
				[self(), sync_random_interval]
			),
			{noreply, State};
		{ok, {Peer, LeftBound, RightBound}} ->
			gen_server:cast(self(), {sync_chunk, Peer, LeftBound, RightBound}),
			{noreply, State}
	end;

handle_cast({sync_chunk, _, LeftBound, RightBound}, State) when LeftBound >= RightBound ->
	gen_server:cast(self(), sync_random_interval),
	ok = store_sync_state(State),
	{noreply, State};
handle_cast({sync_chunk, Peer, LeftBound, RightBound}, State) ->
	Self = self(),
	spawn(
		fun() ->
			case ar_http_iface_client:get_chunk(Peer, LeftBound + 1) of
				{ok, Proof} ->
					gen_server:cast(
						Self,
						{store_fetched_chunk, Peer, LeftBound, RightBound, Proof}
					);
				{error, _} ->
					NextByte = LeftBound + ?DATA_CHUNK_SIZE,
					timer:apply_after(
						?SYNC_FREQUENCY_MS,
						gen_server,
						cast,
						[Self, {sync_chunk, Peer, NextByte, RightBound}]
					)
			end
		end
	),
	{noreply, State};

handle_cast({store_fetched_chunk, Peer, LeftBound, RightBound, Proof}, State) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		peer_sync_records = PeerSyncRecords
	} = State,
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk } = Proof,
	{ok, Key, Value} = ar_kv:get_prev(DataRootOffsetIndex, << LeftBound:?OFFSET_KEY_BITSIZE >>),
	<< BlockStartOffset:?OFFSET_KEY_BITSIZE >> = Key,
	{TXRoot, BlockSize, _} = binary_to_term(Value),
	Offset = LeftBound - BlockStartOffset,
	case validate_proof(TXRoot, TXPath, DataPath, Offset, Chunk, BlockSize) of
		false ->
			gen_server:cast(self(), sync_random_interval),
			{noreply, State#sync_data_state{
				peer_sync_records = maps:remove(Peer, PeerSyncRecords)
			}};
		{true, DataRoot, TXStartOffset, ChunkEndOffset} ->
			ChunkSize = byte_size(Chunk),
			timer:apply_after(
				?SYNC_FREQUENCY_MS,
				gen_server,
				cast,
				[self(), {sync_chunk, Peer, LeftBound + ChunkSize, RightBound}]
			),
			case store_chunk(
				State,
				BlockStartOffset + TXStartOffset + ChunkEndOffset,
				TXRoot,
				DataRoot,
				DataPath,
				Chunk,
				TXPath
			) of
				{updated, UpdatedState} ->
					{noreply, UpdatedState};
				_ ->
					{noreply, State}
			end
	end;

handle_cast(cleanup_orphaned_chunks, State) ->
	#sync_data_state{
		orphaned_chunks = OrphanedChunks
	} = State,
	Timestamp = erlang:monotonic_time(second),
	CleanedUpOrphanedChunks = maps:filter(
		fun(_, {EntryTimestamp, _}) ->
			EntryTimestamp + ?ORPHANED_DATA_ROOT_EXPIRATION_TIME_S < Timestamp
		end,
		OrphanedChunks
	),
	timer:apply_after(
		?ORPHANED_DATA_ROOTS_CLEANUP_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), cleanup_orphaned_chunks]
	),
	{noreply, State#sync_data_state{ orphaned_chunks = CleanedUpOrphanedChunks }}.

handle_call(_Msg, _From, #sync_data_state{ status = not_joined } = State) ->
	{reply, {error, not_joined}, State};

handle_call({add_chunk, DataRoot, DataPath, Chunk, Offset}, _From, State) ->
	#sync_data_state{
		data_root_index = DataRootIndex
	} = State,
	case ar_kv:get(DataRootIndex, DataRoot) of
		not_found ->
			{reply, {error, data_root_not_found}, State};
		{ok, Value} ->
			DataRootIndexIterator = data_root_index_iterator(binary_to_term(Value)),
			{{_, _, _, TXSize}, _} = next(DataRootIndexIterator),
			case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
				false ->
					{reply, {error, invalid_proof}, State};
				{true, EndOffset} ->
					store_chunk(
						State,
						DataRootIndexIterator,
						DataRoot,
						DataPath,
						Chunk,
						EndOffset
					)
			end
	end;

handle_call({get_chunk, Offset}, _From, State) ->
	#sync_data_state{
		chunks_index = ChunksIndex,
		sync_record = SyncRecord
	} = State,
	case ar_intervals:is_inside(SyncRecord, Offset) of
		false ->
			{reply, {error, chunk_not_found}, State};
		true ->
			case ar_kv:get_next(ChunksIndex, << Offset:?OFFSET_KEY_BITSIZE >>) of
				{error, _} ->
					{reply, {error, chunk_not_found}, State};
				{ok, Key, Value} ->
					<< ChunkOffset:?OFFSET_KEY_BITSIZE >> = Key,
					{DataPathHash, TXRoot, _, TXPath, ChunkSize} = binary_to_term(Value),
					case ChunkOffset - Offset >= ChunkSize of
						true ->
							{reply, {error, chunk_not_found}, State};
						false ->
							case ar_storage:read_chunk(TXRoot, DataPathHash) of
								{ok, {Chunk, DataPath}} ->
									Proof = #{
										tx_root => TXRoot,
										chunk => Chunk,
										data_path => DataPath,
										tx_path => TXPath
									},
									{reply, {ok, Proof}, State};
								not_found ->
									{reply, {error, chunk_not_found}, State};
								{error, Reason} ->
									ar:err([
										{event, failed_to_read_chunk},
										{reason, Reason}
									]),
									{reply, {error, failed_to_read_chunk}, State}
							end
					end
			end
	end;

handle_call({get_tx_data, TXID}, _From, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		chunks_index = ChunksIndex
	} = State,
	case ar_kv:get(TXIndex, TXID) of
		not_found ->
			{reply, {error, not_found}, State};
		{error, Reason} ->
			ar:err([{event, failed_to_get_tx_data}, {reason, Reason}]),
			{reply, {error, failed_to_get_tx_data}, State};
		{ok, Value} ->
			{Offset, Size} = binary_to_term(Value),
			case Size > ?MAX_SERVED_TX_DATA_SIZE of
				true ->
					{reply, {error, tx_data_too_big}, State};
				false ->
					StartKey = << (Offset - Size):?OFFSET_KEY_BITSIZE >>,
					EndKey = << Offset:?OFFSET_KEY_BITSIZE >>,
					case ar_kv:get_range(ChunksIndex, StartKey, EndKey) of
						{error, Reason} ->
							ar:err([
								{event, failed_to_get_chunks_for_tx_data},
								{reason, Reason}
							]),
							{reply, {error, not_found}, State};
						{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
							{reply, {error, not_found}, State};
						{ok, Map} ->
							{reply, get_tx_data_from_chunks(Offset, Size, Map), State}
					end
			end
	end;

handle_call({get_tx_offset, TXID}, _From, State) ->
	#sync_data_state{
		tx_index = TXIndex
	} = State,
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{reply,  {ok, binary_to_term(Value)}, State};
		not_found ->
			{reply, {error, not_found}, State};
		{error, Reason} ->
			ar:err([{event, failed_to_read_tx_offset}, {reason, Reason}]),
			{reply, {error, failed_to_read_offset}, State}
	end;

handle_call(get_sync_record_etf, _From, #sync_data_state{ sync_record = SyncRecord } = State) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	{reply, {ok, ar_intervals:to_etf(SyncRecord, Limit)}, State};

handle_call(get_sync_record_json, _From, #sync_data_state{ sync_record = SyncRecord } = State) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	{reply, {ok, ar_intervals:to_json(SyncRecord, Limit)}, State}.

terminate(_Reason, #sync_data_state{ status = not_joined }) ->
	ok;
terminate(Reason, State) ->
	ar:info([{event, ar_data_sync_terminate}, {reason, Reason}]),
	#sync_data_state{
		chunks_index = {DB, _}
	} = State,
	ar_kv:close(DB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

do_init([{_, WeaveSize, _} | _] = BI) ->
	Opts = [
		{cache_index_and_filter_blocks, true},
		{bloom_filter_policy, 10}, % ~1% false positive probability
		{prefix_extractor, {capped_prefix_transform, 28}},
		{optimize_filters_for_hits, true},
		{max_open_files, 100000}
	],
	ColumnFamilies = [
		"default",
		"chunks_index",
		"data_root_index",
		"data_root_offset_index",
		"tx_index",
		"tx_offset_index"
	],
	ColumnFamilyDescriptors = [{Name, Opts} || Name <- ColumnFamilies],
	{ok, DB, [_, CF1, CF2, CF3, CF4, CF5]} =
		ar_kv:open("ar_data_sync_db", ColumnFamilyDescriptors),
	ChunksIndex = {DB, CF1},
	DataRootIndex = {DB, CF2},
	DataRootOffsetIndex = {DB, CF3},
	TXIndex = {DB, CF4},
	TXOffsetIndex = {DB, CF5},
	State = #sync_data_state{
		chunks_index = ChunksIndex,
		data_root_index = DataRootIndex,
		data_root_offset_index = DataRootOffsetIndex,
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex,
		block_index = lists:sublist(BI, ?TRACK_CONFIRMATIONS),
		weave_size = WeaveSize,
		orphaned_chunks = #{},
		sync_record = ar_intervals:new(),
		peer_sync_records = #{},
		status = joined
	},
	UpdatedState = case ar_storage:read_term(data_sync_state) of
		{ok, {SyncRecord, LastStoredBI}} ->
			case get_intersection(BI, LastStoredBI) of
				{ok, full_intersection, NewBI} ->
					ok = data_root_offset_index_from_block_index(DataRootOffsetIndex, NewBI),
					State#sync_data_state{
						sync_record = SyncRecord
					};
				{ok, no_intersection} ->
					throw(last_stored_block_index_has_no_intersection_with_the_new_one);
				{ok, Offset, NewBI} ->
					{ok, State2} = remove_orphaned_data(State, Offset),
					ok = data_root_offset_index_from_block_index(DataRootOffsetIndex, NewBI),
					State2#sync_data_state{
						sync_record = ar_intervals:cut(SyncRecord, Offset)
					}
			end;
		not_found ->
			ok = data_root_offset_index_from_block_index(DataRootOffsetIndex, BI),
			State
	end,
	ok = store_sync_state(UpdatedState),
	gen_server:cast(self(), update_peer_sync_records),
	gen_server:cast(self(), sync_random_interval),
	timer:apply_after(
		?ORPHANED_DATA_ROOTS_CLEANUP_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), cleanup_orphaned_chunks]
	),
	{noreply, UpdatedState}.

get_intersection(BI, [{BH, _, _} | LastStoredBI]) ->
	case block_index_contains_block(BI, BH) of
		true ->
			{ok, full_intersection, lists:takewhile(fun({H, _, _}) -> H /= BH end, BI)};
		false ->
			get_intersection2(BI, LastStoredBI)
	end.

block_index_contains_block([{BH, _, _} | _], BH) ->
	true;
block_index_contains_block([_ | BI], BH) ->
	block_index_contains_block(BI, BH);
block_index_contains_block([], _BH) ->
	false.

get_intersection2(BI, [{BH, WeaveSize, _} | LastStoredBI]) ->
	case block_index_contains_block(BI, BH) of
		true ->
			{ok, WeaveSize, lists:takewhile(fun({H, _, _}) -> H /= BH end, BI)};
		false ->
			get_intersection2(BI, LastStoredBI)
	end;
get_intersection2(_BI, []) ->
	{ok, no_intersection}.

data_root_offset_index_from_block_index(Index, BI) ->
	data_root_offset_index_from_reversed_block_index(Index, lists:reverse(BI)).

data_root_offset_index_from_reversed_block_index(Index, BI) ->
	data_root_offset_index_from_reversed_block_index(Index, BI, 0).

data_root_offset_index_from_reversed_block_index(
	Index,
	[{_, _WeaveSize, <<>>} | BI],
	StartOffset
) ->
	data_root_offset_index_from_reversed_block_index(Index, BI, StartOffset);
data_root_offset_index_from_reversed_block_index(
	Index,
	[{_, WeaveSize, TXRoot} | BI],
	StartOffset
) ->
	case ar_kv:put(
		Index,
		<< StartOffset:?OFFSET_KEY_BITSIZE >>,
		term_to_binary({TXRoot, WeaveSize - StartOffset, sets:new()})
	) of
		ok ->
			data_root_offset_index_from_reversed_block_index(Index, BI, WeaveSize);
		Error ->
			Error
	end;
data_root_offset_index_from_reversed_block_index(_Index, [], _StartOffset) ->
	ok.	

remove_orphaned_data(State, BlockStartOffset) ->
	ok = remove_orphaned_txs(State, BlockStartOffset),
	case remove_orphaned_chunks(State, BlockStartOffset) of
		{ok, UpdatedState} ->
			ok = remove_orphaned_data_roots(State, BlockStartOffset),
			ok = remove_orphaned_data_root_offsets(State, BlockStartOffset),
			{ok, UpdatedState};
		Error ->
			Error
	end.

remove_orphaned_txs(State, BlockStartOffset) ->
	#sync_data_state{
		tx_offset_index = TXOffsetIndex,
		tx_index = TXIndex,
		weave_size = WeaveSize
	} = State,
	ok = case ar_kv:get_range(TXOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			ok;
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, TXID, ok) ->
						ar_kv:delete(TXIndex, TXID)
				end,
				ok,
				Map
			);
		Error ->
			Error
	end,
	ar_kv:delete_range(
		TXOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< WeaveSize:?OFFSET_KEY_BITSIZE >>
	).

remove_orphaned_chunks(State, BlockStartOffset) ->
	#sync_data_state{
		weave_size = WeaveSize,
		chunks_index = ChunksIndex,
		orphaned_chunks = OrphanedChunks,
		data_root_index = DataRootIndex
	} = State,
	StartKey = << (BlockStartOffset + 1):?OFFSET_KEY_BITSIZE >>,
	case ar_kv:get_range(ChunksIndex, StartKey) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, State};
		{ok, OrphanedChunkMap} ->
			case ar_kv:delete_range(ChunksIndex, StartKey, << WeaveSize:?OFFSET_KEY_BITSIZE >>) of
				ok ->
					UpdatedOrphanedChunks =
						update_orphaned_chunks(OrphanedChunks, DataRootIndex, OrphanedChunkMap),
					{ok, State#sync_data_state{ orphaned_chunks = UpdatedOrphanedChunks }};
				{error, Reason} = Error ->
					ar:err([
						{event, failed_to_delete_chunk_range},
						{reason, Reason},
						{start_offset, BlockStartOffset}
					]),
					Error
			end;
		{error, Reason} = Error ->
			ar:err([
				{event, failed_to_get_chunk_range},
				{reason, Reason},
				{start_offset, BlockStartOffset}
			]),
			Error
	end.

remove_orphaned_data_roots(State, BlockStartOffset) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		data_root_index = DataRootIndex
	} = State,
	case ar_kv:get_range(DataRootOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			ok;
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, Value, ok) ->
						{TXRoot, _BlockSize, DataRootSet} = binary_to_term(Value),
						sets:fold(
							fun (_DataRoot, {error, _} = Error) ->
								Error;
								(DataRoot, ok) ->
									remove_orphaned_data_root(DataRootIndex, DataRoot, TXRoot)
							end,
							ok,
							DataRootSet
						)
				end,
				ok,
				Map
			);	
		Error ->
			Error
	end.

remove_orphaned_data_root(_DataRootIndex, <<>>, _TXRoot) ->
	ok;
remove_orphaned_data_root(DataRootIndex, DataRoot, TXRoot) ->
	case ar_kv:get(DataRootIndex, DataRoot) of
		not_found ->
			ok;
		{ok, Value} ->
			Map = binary_to_term(Value),
			case maps:take(TXRoot, Map) of
				{_, EmptyMap} when map_size(EmptyMap) == 0 ->
					ar_kv:delete(DataRootIndex, DataRoot);
				{_, UpdatedMap} ->
					ar_kv:put(DataRootIndex, DataRoot, term_to_binary(UpdatedMap));
				error ->
					ok
			end
	end.

remove_orphaned_data_root_offsets(State, BlockStartOffset) ->
	#sync_data_state{
		weave_size = WeaveSize,
		data_root_offset_index = DataRootOffsetIndex
	} = State,
	ar_kv:delete_range(
		DataRootOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< WeaveSize:?OFFSET_KEY_BITSIZE >>
	).

update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset) ->
	lists:foldl(
		fun({{TXID, _}, TXEndOffset}, PreviousOffset) ->
			AbsoluteEndOffset = BlockStartOffset + TXEndOffset,
			TXSize = TXEndOffset - PreviousOffset,
			AbsoluteStartOffset = AbsoluteEndOffset - TXSize,
			case ar_kv:put(TXOffsetIndex, << AbsoluteStartOffset:?OFFSET_KEY_BITSIZE >>, TXID) of
				ok ->
					case ar_kv:put(TXIndex, TXID, term_to_binary({AbsoluteEndOffset, TXSize})) of
						ok ->
							TXEndOffset;
						{error, Reason} ->
							ar:err([{event, failed_to_update_tx_index}, {reason, Reason}]),
							TXEndOffset
					end;
				{error, Reason} ->
					ar:err([{event, failed_to_update_tx_offset_index}, {reason, Reason}]),
					TXEndOffset
			end
		end,
		0,
		SizeTaggedTXs
	),
	ok.

pick_up_orphaned_chunks(State, [], _CurrentWeaveSize) ->
	{ok, State};
pick_up_orphaned_chunks(State, SizeTaggedTXs, CurrentWeaveSize) ->
	#sync_data_state{
		chunks_index = ChunksIndex,
		orphaned_chunks = OrphanedChunks,
		data_root_offset_index = DataRootOffsetIndex,
		sync_record = SyncRecord
	} = State,
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	{BlockSize, UpdatedSyncRecord, DataRootSet} = lists:foldl(
		fun({DataRoot, TXEndOffset}, {TXStartOffset, CurrentSyncRecord, CurrentDataRootSet}) ->
			TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
			TXOffset = CurrentWeaveSize + TXStartOffset,
			TXSize = TXEndOffset - TXStartOffset,
			ok = update_data_root_index(
				State,
				DataRoot,
				TXRoot,
				TXOffset,
				TXPath,
				TXSize
			),
			{_, ChunkMap} = maps:get(DataRoot, OrphanedChunks, {none, #{}}),
			UpdatedDataRootSet = sets:add_element(DataRoot, CurrentDataRootSet),
			case ChunkMap of
				EmptyMap when map_size(EmptyMap) == 0 ->
					{TXEndOffset, CurrentSyncRecord, UpdatedDataRootSet};
				_ ->
					{TXEndOffset, maps:fold(
						fun(DataPathHash, {ChunkOffset, ChunkSize}, Acc) ->
							AbsoluteChunkOffset =
								CurrentWeaveSize + TXStartOffset + ChunkOffset,
							case ar_intervals:is_inside(
								Acc,
								AbsoluteChunkOffset
							) of
								true ->
									Acc;
								false ->
									{ok, UpdatedAcc} = update_chunks_index(
										ChunksIndex,
										Acc,
										AbsoluteChunkOffset,
										DataPathHash,
										TXRoot,
										DataRoot,
										TXPath,
										ChunkSize
									),
									UpdatedAcc
							end
						end,
						CurrentSyncRecord,
						ChunkMap
					), UpdatedDataRootSet}
			end
		end,
		{0, SyncRecord, sets:new()},
		SizeTaggedDataRoots
	),
	ok = ar_kv:put(
		DataRootOffsetIndex,
		<< CurrentWeaveSize:?OFFSET_KEY_BITSIZE >>,
		term_to_binary({TXRoot, BlockSize, DataRootSet})
	),
	{ok, State#sync_data_state{ sync_record = UpdatedSyncRecord }}.

update_data_root_index(State, DataRoot, TXRoot, AbsoluteTXStartOffset, TXPath, TXSize) ->
	#sync_data_state{
		data_root_index = DataRootIndex
	} = State,
	TXRootMap = case ar_kv:get(DataRootIndex, DataRoot) of
		not_found ->
			#{};
		{ok, Value} ->
			binary_to_term(Value)
	end,
	OffsetMap = maps:get(TXRoot, TXRootMap, #{}),
	UpdatedValue = term_to_binary(
		TXRootMap#{
			TXRoot => OffsetMap#{ AbsoluteTXStartOffset => {TXPath, TXSize} }
		}
	),
	ar_kv:put(DataRootIndex, DataRoot, UpdatedValue).

update_chunks_index(
	ChunksIndex,
	SyncRecord,
	AbsoluteChunkOffset,
	DataPathHash,
	TXRoot,
	DataRoot,
	TXPath,
	ChunkSize
) ->
	Value = term_to_binary({DataPathHash, TXRoot, DataRoot, TXPath, ChunkSize}),
	case ar_kv:put(ChunksIndex, << AbsoluteChunkOffset:?OFFSET_KEY_BITSIZE >>, Value) of
		ok ->
			AbsoluteChunkStartOffset = AbsoluteChunkOffset - ChunkSize,
			{ok, ar_intervals:add(SyncRecord, AbsoluteChunkOffset, AbsoluteChunkStartOffset)};
		{error, Reason} ->
			ar:err([
				{event, failed_to_update_chunk_index},
				{reason, Reason}
			]),
			{error, Reason}
	end.

store_sync_state(
	#sync_data_state{
		sync_record = SyncRecord,
		block_index = BI
	}
) ->
	ar_storage:write_term(data_sync_state, {SyncRecord, BI}).

update_orphaned_chunks(OrphanedChunks, DataRootIndex, OrphanedChunkMap) ->
	maps:fold(
		fun(Key, Value, Acc) ->
			<< AbsoluteChunkOffset:?OFFSET_KEY_BITSIZE >> = Key,
			{DataPathHash, TXRoot, DataRoot, _TXPath, ChunkSize} = binary_to_term(Value),
			case Acc of
				#{ DataRoot := {_, #{ DataPathHash := _ }} } ->
					Acc;
				_ ->
					case ar_kv:get(DataRootIndex, DataRoot) of
						{ok, DataRootIndexValue} ->
							case binary_to_term(DataRootIndexValue) of
								#{ TXRoot := OffsetMap } ->
									maps:fold(
										fun(AbsoluteTXStartOffset, _, MAcc) ->
											RelativeChunkOffset =
												AbsoluteChunkOffset - AbsoluteTXStartOffset,
											{_, ChunkMap} =
												maps:get(DataRoot, MAcc, {none, #{}}),
											Entry = {RelativeChunkOffset, ChunkSize},
											Timestamp = erlang:monotonic_time(second),
											MAcc#{
												DataRoot => {
													Timestamp,
													ChunkMap#{
														DataPathHash => Entry
													}
												}
											}
										end,
										Acc,
										OffsetMap
									);
								_ ->
									ar:err([
										{event, failed_to_update_orphaned_chunks},
										{reason, tx_root_not_found_in_data_root_index}
									]),
									Acc
							end;
						not_found ->
							ar:err([
								{event, failed_to_update_orphaned_chunks},
								{reason, data_root_not_found_in_data_root_index}
							]),
							Acc
					end							
			end
		end,
		OrphanedChunks,
		OrphanedChunkMap
	).

pick_random_peers(Peers, N) ->
	pick_random_peers(Peers, 0, N, []).

pick_random_peers(_Peers, Picked, N, List) when Picked == N ->
	List;
pick_random_peers([], _Picked, _N, List) ->
	List;
pick_random_peers([Peer | Peers], Picked, N, List) ->
	case rand:uniform() > 0.5 of
		true ->
			pick_random_peers(Peers, Picked + 1, N, [Peer | List]);
		false ->
			pick_random_peers(Peers, Picked, N, List)
	end.

get_random_interval(SyncRecord, PeerSyncRecords, WeaveSize) ->
	%% Try keeping no more than ?MAX_SHARED_SYNCED_INTERVALS_COUNT intervals
	%% in the sync record by choosing the appropriate size of continuous
	%% intervals to sync. The motivation is to keep the record size small for
	%% low traffic overhead. When the size increases ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	%% a random subset of the intervals is served to peers.
	SyncSize = WeaveSize div ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	maps:fold(
		fun (_, _, {ok, {Peer, L, R}}) ->
				{ok, {Peer, L, R}};
			(Peer, PeerSyncRecord, none) ->
				PeerSyncRecordBelowWeaveSize = ar_intervals:cut(PeerSyncRecord, WeaveSize),
				I = ar_intervals:outerjoin(SyncRecord, PeerSyncRecordBelowWeaveSize),
				Sum = ar_intervals:sum(I),
				case Sum of
					0 ->
						none;
					_ ->
						RelativeByte = rand:uniform(Sum) - 1,
						{L, Byte, R} =
							ar_intervals:get_interval_by_nth_inner_number(I, RelativeByte),
						LeftBound = max(L, Byte - SyncSize div 2),
						{ok, {Peer, LeftBound, min(R, LeftBound + SyncSize div 2)}}
				end
		end,
		none,
		PeerSyncRecords
	).

validate_proof(_, _, _, _, Chunk, _) when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
	false;
validate_proof(TXRoot, TXPath, DataPath, Offset, Chunk, BlockSize) ->
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			ChunkOffset = Offset - TXStartOffset,
			TXSize = TXEndOffset - TXStartOffset,
			case ar_merkle:validate_path(DataRoot, ChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, _, ChunkEndOffset} ->
					case ar_tx:generate_chunk_id(Chunk) == ChunkID of
						false ->
							false;
						true ->
							{true, DataRoot, TXStartOffset, ChunkEndOffset}
					end
			end
	end.

validate_data_path(_, _, _, _, Chunk) when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
	false;
validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	case ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath) of
		false ->
			false;
		{ChunkID, _, EndOffset} ->
			case ar_tx:generate_chunk_id(Chunk) == ChunkID of
				false ->
					false;
				true ->
					{true, EndOffset}
			end
	end.

store_chunk(State, DataRootIndexIterator, DataRoot, DataPath, Chunk, EndOffset) ->
	case next(DataRootIndexIterator) of
		none ->
			{reply, ok, State};
		{{TXRoot, TXStartOffset, TXPath, _TXSize}, UpdatedDataRootIndexIterator} ->
			AbsoluteEndOffset = TXStartOffset + EndOffset,
			case store_chunk(
				State,
				AbsoluteEndOffset,
				TXRoot,
				DataRoot,
				DataPath,
				Chunk,
				TXPath
			) of
				{updated, UpdatedState} ->
					ok = store_sync_state(UpdatedState),
					store_chunk(
						UpdatedState,
						UpdatedDataRootIndexIterator,
						DataRoot,
						DataPath,
						Chunk,
						EndOffset
					);
				not_updated ->
					store_chunk(
						State,
						UpdatedDataRootIndexIterator,
						DataRoot,
						DataPath,
						Chunk,
						EndOffset
					);
				{error, _} ->
					{reply, {error, failed_to_store_chunk}, State}
			end
	end.

store_chunk(
	State,
	AbsoluteEndOffset,
	TXRoot,
	DataRoot,
	DataPath,
	Chunk,
	TXPath
) ->
	#sync_data_state{
		chunks_index = ChunksIndex,
		sync_record = SyncRecord
	} = State,
	case ar_intervals:is_inside(SyncRecord, AbsoluteEndOffset) of
		true ->
			not_updated;
		false ->
			DataPathHash = crypto:hash(sha256, DataPath),
			case update_chunks_index(
				ChunksIndex,
				SyncRecord,
				AbsoluteEndOffset,
				DataPathHash,
				TXRoot,
				DataRoot,
				TXPath,
				byte_size(Chunk)
			) of
				{error, _Reason} = Error ->
					Error;
				{ok, UpdatedSyncRecord} ->
					StoreChunk = case ar_storage:has_chunk(TXRoot, DataPathHash) of
						true ->
							%% The chunk may be already stored because the same chunk
							%% might be inside different transactions and different
							%% blocks.
							ok;
						false ->
							ar_storage:write_chunk(
								TXRoot,
								DataPathHash,
								Chunk,
								DataPath
							)
					end,
					case StoreChunk of
						{error, _Reason} = Error ->
							Error;
						ok ->
							UpdatedState = State#sync_data_state{
								sync_record = UpdatedSyncRecord
							},
							{updated, UpdatedState}
					end
			end
	end.

get_tx_data_from_chunks(Offset, Size, Map) ->
	get_tx_data_from_chunks(Offset, Size, Map, <<>>).

get_tx_data_from_chunks(_Offset, 0, _Map, Data) ->
	{ok, iolist_to_binary(Data)};
get_tx_data_from_chunks(Offset, Size, Map, Data) ->
	case maps:get(<< Offset:?OFFSET_KEY_BITSIZE >>, Map, not_found) of
		not_found ->
			{error, not_found};
		Value ->
			{DataPathHash, TXRoot, _, _, ChunkSize} = binary_to_term(Value),
			case ar_storage:read_chunk(TXRoot, DataPathHash) of
				not_found ->
					{error, not_found};
				{error, Reason} ->
					ar:err([{event, failed_to_read_chunk_for_tx_data}, {reason, Reason}]),
					{error, not_found};
				{ok, {Chunk, _}} ->
					get_tx_data_from_chunks(
						Offset - ChunkSize, Size - ChunkSize, Map, [Chunk | Data])
			end
	end.

data_root_index_iterator(TXRootMap) ->
	{maps:iterator(TXRootMap), none}.

next({TXRootMapIterator, none}) ->
	case maps:next(TXRootMapIterator) of
		none ->
			none;
		{TXRoot, OffsetMap, UpdatedTXRootMapIterator} ->
			OffsetMapIterator = maps:iterator(OffsetMap),
			{Offset, {TXPath, TXSize}, UpdatedOffsetMapIterator} = maps:next(OffsetMapIterator),
			UpdatedIterator = {UpdatedTXRootMapIterator, {TXRoot, UpdatedOffsetMapIterator}},
			{{TXRoot, Offset, TXPath, TXSize}, UpdatedIterator}
	end;
next({TXRootMapIterator, {TXRoot, OffsetMapIterator}}) ->
	case maps:next(OffsetMapIterator) of
		none ->
			next({TXRootMapIterator, none});
		{Offset, {TXPath, TXSize}, UpdatedOffsetMapIterator} ->
			UpdatedIterator = {TXRootMapIterator, {TXRoot, UpdatedOffsetMapIterator}},
			{{TXRoot, Offset, TXPath, TXSize}, UpdatedIterator}
	end.
