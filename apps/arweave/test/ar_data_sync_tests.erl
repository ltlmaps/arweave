-module(ar_data_sync_tests).

-include_lib("eunit/include/eunit.hrl").

-include("src/ar.hrl").
-include("src/ar_data_sync.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0, sign_tx/2, sign_v1_tx/2]).
-import(ar_test_node, [wait_until_height/2, slave_wait_until_height/2]).
-import(ar_test_node, [post_and_mine/2, get_tx_anchor/1]).
-import(ar_test_node, [slave_call/3, disconnect_from_slave/0, join_on_master/0]).

rejects_invalid_chunks_test() ->
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"chunk_too_big\"}">>, _, _}},
		post_chunk(jiffy:encode(#{
			chunk => ar_util:encode(crypto:strong_rand_bytes(?DATA_CHUNK_SIZE + 1)),
			data_path => <<>>,
			offset => <<"0">>
		}))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"data_path_too_big\"}">>, _, _}},
		post_chunk(jiffy:encode(#{
			data_path => ar_util:encode(crypto:strong_rand_bytes(?MAX_PATH_SIZE + 1)),
			chunk => <<>>,
			offset => <<"0">>
		}))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"offset_too_big\"}">>, _, _}},
		post_chunk(jiffy:encode(#{
			offset => integer_to_binary(trunc(math:pow(2, 256))),
			data_path => <<>>,
			chunk => <<>>
		}))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"chunk_proof_ratio_not_attractive\"}">>, _, _}},
		post_chunk(jiffy:encode(#{
			chunk => ar_util:encode(<<"a">>),
			data_path => ar_util:encode(<<"bb">>),
			offset => <<"0">>
		}))
	),
	setup_nodes(),
	Chunk = crypto:strong_rand_bytes(500),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
		ar_tx:chunks_to_size_tagged_chunks([Chunk])
	),
	{DataRoot, DataTree} = ar_merkle:generate_tree(SizedChunkIDs),
	DataPath = ar_merkle:generate_path(DataRoot, 0, DataTree),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"data_root_not_found\"}">>, _, _}},
		post_chunk(jiffy:encode(#{
			data_root => ar_util:encode(DataRoot),
			chunk => ar_util:encode(Chunk),
			data_path => ar_util:encode(DataPath),
			offset => <<"0">>
		}))
	),
	?assertMatch(
		{ok, {{<<"413">>, _}, _, <<"Payload too large">>, _, _}},
		post_chunk(<< <<0>> || _ <- lists:seq(1, ?MAX_SERIALIZED_CHUNK_PROOF_SIZE + 1) >>)
	).

accepts_chunk_with_out_of_outer_bounds_offset_test_() ->
	{timeout, 60, fun test_accepts_chunk_with_out_of_outer_bounds_offset/0}.

test_accepts_chunk_with_out_of_outer_bounds_offset() ->
	{Master, _, Wallet} = setup_nodes(),
	DataSize = 10000,
	OutOfBoundsOffsetChunk = crypto:strong_rand_bytes(DataSize),
	ChunkID = ar_tx:generate_chunk_id(OutOfBoundsOffsetChunk),
	{DataRoot, DataTree} = ar_merkle:generate_tree([{ChunkID, DataSize + 1}]),
	TX = sign_tx(
		Wallet,
		#{ last_tx => get_tx_anchor(master), data_size => DataSize, data_root => DataRoot }
	),
	post_and_mine(#{ miner => {master, Master}, await_on => {master, Master} }, [TX]),
	DataPath = ar_merkle:generate_path(DataRoot, 0, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(OutOfBoundsOffsetChunk),
		offset => <<"0">>
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(Proof))
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(DataSize + 1)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		get_chunk(DataSize)
	).

accepts_chunk_with_out_of_inner_bounds_offset_test_() ->
	{timeout, 60, fun test_accepts_chunk_with_out_of_inner_bounds_offset/0}.

test_accepts_chunk_with_out_of_inner_bounds_offset() ->
	{Master, _, Wallet} = setup_nodes(),
	ChunkSize = 1000,
	Chunk = crypto:strong_rand_bytes(ChunkSize),
	FirstChunkID = ar_tx:generate_chunk_id(Chunk),
	FirstHash = hash([hash(FirstChunkID), hash(<< (ChunkSize + 500):256 >>)]),
	SecondHash = crypto:strong_rand_bytes(32),
	InvalidDataPath = iolist_to_binary([
		<< FirstHash/binary, SecondHash/binary, ChunkSize:256>> |
		<< FirstChunkID/binary, (ChunkSize + 500):256 >>
	]),
	DataRoot = hash([hash(FirstHash), hash(SecondHash), hash(<< ChunkSize:256 >>)]),
	TX = sign_tx(
		Wallet,
		#{
			last_tx => get_tx_anchor(master),
			data_root => DataRoot,
			data_size => 2 * ChunkSize
		}
	),
	B1 = post_and_mine(#{ miner => {master, Master}, await_on => {master, Master} }, [TX]),
	InvalidProof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(InvalidDataPath),
		chunk => ar_util:encode(Chunk),
		offset => <<"0">>
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(InvalidProof))
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(ChunkSize + 1)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(ChunkSize + 400)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		get_chunk(ChunkSize)
	),
	FirstHash2 = hash([hash(FirstChunkID), hash(<< ChunkSize:256 >>)]),
	InvalidDataPath2 = iolist_to_binary([
		<< FirstHash2/binary, SecondHash/binary, (2 * ChunkSize + 500):256>> |
		<< FirstChunkID/binary, ChunkSize:256 >>
	]),
	DataRoot2 = hash([hash(FirstHash2), hash(SecondHash), hash(<< (2 * ChunkSize + 500):256 >>)]),
	TX2 = sign_tx(
		Wallet,
		#{
			last_tx => get_tx_anchor(master),
			data_root => DataRoot2,
			data_size => 2 * ChunkSize
		}
	),
	post_and_mine(#{ miner => {master, Master}, await_on => {master, Master} }, [TX2]),
	InvalidProof2 = #{
		data_root => ar_util:encode(DataRoot2),
		data_path => ar_util:encode(InvalidDataPath2),
		chunk => ar_util:encode(Chunk),
		offset => <<"0">>
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(InvalidProof2))
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(B1#block.weave_size + 2 * ChunkSize + 1)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(B1#block.weave_size + 2 * ChunkSize + 400)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		get_chunk(B1#block.weave_size + 2 * ChunkSize)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		get_chunk(B1#block.weave_size + 2 * ChunkSize - ChunkSize + 1)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(B1#block.weave_size + 2 * ChunkSize - ChunkSize)
	).

accepts_chunks_test_() ->
	{timeout, 60, fun test_accepts_chunks/0}.

test_accepts_chunks() ->
	{Master, Slave, Wallet} = setup_nodes(),
	{TX, Chunks} = tx(Wallet, {custom_split, 2}),
	B = post_and_mine(#{ miner => {slave, Slave}, await_on => {master, Master} }, [TX]),
	[{EndOffset, FirstProof}, {_, SecondProof}] = build_proofs(B, TX, Chunks),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(FirstProof))
	),
	%% Expect the chunk to be retrieved by any offset within
	%% (EndOffset - ChunkSize, EndOffset], but not outside of it.
	FirstChunk = ar_util:decode(maps:get(chunk, FirstProof)),
	FirstChunkSize = byte_size(FirstChunk),
	ExpectedProof = jiffy:encode(#{
		data_path => maps:get(data_path, FirstProof),
		tx_path => maps:get(tx_path, FirstProof),
		chunk => ar_util:encode(FirstChunk)
	}),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedProof, _, _}},
		get_chunk(EndOffset)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedProof, _, _}},
		get_chunk(EndOffset - rand:uniform(FirstChunkSize - 2))
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedProof, _, _}},
		get_chunk(EndOffset - FirstChunkSize + 1)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset - FirstChunkSize)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(EndOffset + 1)
	),
	TXSize = byte_size(binary:list_to_bin(Chunks)),
	ExpectedOffsetInfo = jiffy:encode(#{
		offset => integer_to_binary(TXSize),
		size => integer_to_binary(TXSize)
	}),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedOffsetInfo, _, _}},
		get_tx_offset(TX#tx.id)
	),
	%% Expect no transaction data because the second chunk is not synced yet.
	?assertMatch(
		{ok, {{<<"200">>, _}, _, <<>>, _, _}},
		get_tx_data(TX#tx.id)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk(jiffy:encode(SecondProof))
	),
	ExpectedSecondProof = jiffy:encode(#{
		data_path => maps:get(data_path, SecondProof),
		tx_path => maps:get(tx_path, SecondProof),
		chunk => maps:get(chunk, SecondProof)
	}),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, ExpectedSecondProof, _, _}},
		get_chunk(B#block.weave_size)
	),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		get_chunk(B#block.weave_size + 1)
	),
	{ok, {{<<"200">>, _}, _, Data, _, _}} = get_tx_data(TX#tx.id),
	?assertEqual(ar_util:encode(binary:list_to_bin(Chunks)), Data).

syncs_data_test_() ->
	{timeout, 180, fun test_syncs_data/0}.

test_syncs_data() ->
	{Master, _Slave, Wallet} = setup_nodes(),
	Records = post_random_blocks(Master, Wallet),
	RecordsWithProofs = lists:flatmap(
		fun({B, TX, Chunks}) ->
			[{B, TX, Chunks, Proof} || Proof <- build_proofs(B, TX, Chunks)]
		end,
		Records
	),
	lists:foreach(
		fun({_, _, _, {_, Proof}}) ->
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				post_chunk(jiffy:encode(Proof))
			)
		end,
		RecordsWithProofs
	),
	slave_wait_until_syncs_chunks([Proof || {_, _, _, Proof} <- RecordsWithProofs]),
	lists:foreach(
		fun({B, #tx{ id = TXID }, Chunks, {_, Proof}}) ->
			TXSize = byte_size(binary:list_to_bin(Chunks)),
			TXOffset = ar_merkle:extract_note(ar_util:decode(maps:get(tx_path, Proof))),
			AbsoluteTXOffset = B#block.weave_size - B#block.block_size + TXOffset,
			ExpectedOffsetInfo = jiffy:encode(#{
				offset => integer_to_binary(AbsoluteTXOffset),
				size => integer_to_binary(TXSize)
			}),
			ar_util:do_until(
				fun() ->
					case get_tx_offset_from_slave(TXID) of
						{ok, {{<<"200">>, _}, _, ExpectedOffsetInfo, _, _}} ->
							true;
						_ ->
							false
					end
				end,
				100,
				60 * 1000
			)
		end,
		RecordsWithProofs
	),
	lists:foreach(
		fun({_, #tx{ id = TXID }, Chunks, _}) ->
			ExpectedData = ar_util:encode(binary:list_to_bin(Chunks)),
			ar_util:do_until(
				fun() ->
					case get_tx_data_from_slave(TXID) of
						{ok, {{<<"200">>, _}, _, ExpectedData, _, _}} ->
							true;
						_ ->
							false
					end
				end,
				100,
				60 * 1000
			)
		end,
		RecordsWithProofs
	).

fork_recovery_test_() ->
	{timeout, 180, fun test_fork_recovery/0}.

test_fork_recovery() ->
	{Master, Slave, Wallet} = setup_nodes(),
	{TX1, Chunks1} = tx(Wallet, {custom_split, 3}),
	B1 = post_and_mine(#{ miner => {master, Master}, await_on => {slave, Slave} }, [TX1]),
	Proofs1 = post_proofs_to_master(B1, TX1, Chunks1),
	slave_wait_until_syncs_chunks(Proofs1),
	disconnect_from_slave(),
	{SlaveTX2, SlaveChunks2} = tx(Wallet, {custom_split, 5}),
	SlaveB2 = post_and_mine(
		#{ miner => {slave, Slave}, await_on => {slave, Slave} },
		[SlaveTX2]
	),
	_SlaveProofs2 = post_proofs_to_slave(SlaveB2, SlaveTX2, SlaveChunks2),
	{MasterTX2, MasterChunks2} = tx(Wallet, {custom_split, 4}),
	MasterB2 = post_and_mine(
		#{ miner => {master, Master}, await_on => {master, Master} },
		[MasterTX2]
	),
	MasterProofs2 = post_proofs_to_master(MasterB2, MasterTX2, MasterChunks2),
	connect_to_slave(),
	{MasterTX3, MasterChunks3} = tx(Wallet, {custom_split, 6}),
	MasterB3 = post_and_mine(
		#{ miner => {master, Master}, await_on => {master, Master} },
		[MasterTX3]
	),
	MasterProofs3 = post_proofs_to_master(MasterB3, MasterTX3, MasterChunks3),
	slave_wait_until_syncs_chunks(MasterProofs2),
	slave_wait_until_syncs_chunks(MasterProofs3),
	slave_wait_until_syncs_chunks(Proofs1),
	MasterB4 = post_and_mine(
		#{ miner => {master, Master}, await_on => {master, Master} },
		[SlaveTX2]
	),
	Proofs4 = build_proofs(MasterB4, SlaveTX2, SlaveChunks2),
	%% We did not submit proofs for SlaveTX2 - they are supposed to be still stored
	%% by the slave as orphaned chunks.
	slave_wait_until_syncs_chunks(Proofs4),
	wait_until_syncs_chunks(Proofs4).

syncs_after_joining_test_() ->
	{timeout, 180, fun test_syncs_after_joining/0}.

test_syncs_after_joining() ->
	{Master, Slave, Wallet} = setup_nodes(),
	{TX1, Chunks1} = tx(Wallet, {custom_split, 1}),
	B1 = post_and_mine(#{ miner => {master, Master}, await_on => {slave, Slave} }, [TX1]),
	Proofs1 = post_proofs_to_master(B1, TX1, Chunks1),
	slave_wait_until_syncs_chunks(Proofs1),
	disconnect_from_slave(),
	{MasterTX2, MasterChunks2} = tx(Wallet, {custom_split, 7}),
	MasterB2 = post_and_mine(
		#{ miner => {master, Master}, await_on => {master, Master} },
		[MasterTX2]
	),
	MasterProofs2 = post_proofs_to_master(MasterB2, MasterTX2, MasterChunks2),
	Slave2 = join_on_master(),
	slave_wait_until_height(Slave2, 2),
	connect_to_slave(),
	slave_wait_until_syncs_chunks(MasterProofs2),
	slave_wait_until_syncs_chunks(Proofs1).

post_chunk(Body) ->
	post_chunk({127, 0, 0, 1, ar_meta_db:get(port)}, Body).

post_chunk(Peer, Body) ->
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/chunk",
		body => Body
	}).

setup_nodes() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	{Master, Slave, Wallet}.

tx(Wallet, SplitType) ->
	tx(Wallet, SplitType, v2).

v1_tx(Wallet) ->
	tx(Wallet, original_split, v1).

tx(Wallet, SplitType, Format) ->
	case {SplitType, Format} of
		{original_split, v1} ->
			Data = crypto:strong_rand_bytes(10 * 1024),
			{_, Chunks} = original_split(Data),
			{sign_v1_tx(Wallet, #{ data => Data, last_tx => get_tx_anchor(master) }), Chunks};
		{{custom_split, ChunkNumber}, v2} ->
			Chunks = lists:foldl(
				fun(_, Chunks) ->
					OneThird = ?DATA_CHUNK_SIZE div 3,
					RandomSize = OneThird + rand:uniform(?DATA_CHUNK_SIZE - OneThird) - 1,
					Chunk = crypto:strong_rand_bytes(RandomSize),
					[Chunk | Chunks]
				end,
				[],
				lists:seq(
					1,
					case ChunkNumber of random -> rand:uniform(5); _ -> ChunkNumber end
				)
			),
			SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
				ar_tx:chunks_to_size_tagged_chunks(Chunks)
			),
			{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
			TX = sign_tx(Wallet, #{
				data_size => byte_size(binary:list_to_bin(Chunks)),
				last_tx => get_tx_anchor(master),
				data_root => DataRoot
			}),
			{TX, Chunks};
		{original_split, v2} ->
			Data = crypto:strong_rand_bytes(11 * 1024),
			{DataRoot, Chunks} = original_split(Data),
			TX = sign_tx(Wallet, #{
				data_size => byte_size(Data),
				last_tx => get_tx_anchor(master),
				data_root => DataRoot
			}),
			{TX, Chunks}
	end.

original_split(Data) ->
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, Data),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
		ar_tx:chunks_to_size_tagged_chunks(Chunks)
	),
	{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
	{DataRoot, Chunks}.

build_proofs(B, TX, Chunks) ->
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(B#block.txs),
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{value, {_, TXOffset}} =
		lists:search(fun({{TXID, _}, _}) -> TXID == TX#tx.id end, SizeTaggedTXs),
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	TXPath = ar_merkle:generate_path(TXRoot, TXOffset - 1, TXTree),
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks)
	),
	DataSize = byte_size(binary:list_to_bin(Chunks)),
	lists:foldl(
		fun
			({<<>>, _}, Proofs) ->
				Proofs;
			({Chunk, ChunkOffset}, Proofs) ->
				BlockStartOffset = B#block.weave_size - B#block.block_size,
				TXStartOffset = TXOffset - DataSize,
				AbsoluteChunkEndOffset = BlockStartOffset + TXStartOffset + ChunkOffset,
				Proof = #{
					tx_path => ar_util:encode(TXPath),
					data_root => ar_util:encode(DataRoot),
					data_path =>
						ar_util:encode(
							ar_merkle:generate_path(DataRoot, ChunkOffset - 1, DataTree)
						),
					chunk => ar_util:encode(Chunk),
					offset => integer_to_binary(ChunkOffset - 1)
				},
				Proofs ++ [{AbsoluteChunkEndOffset, Proof}]
		end,
		[],
		SizeTaggedChunks
	).

get_chunk(Offset) ->
	get_chunk({127, 0, 0, 1, ar_meta_db:get(port)}, Offset).

get_chunk(Peer, Offset) ->
	ar_http:req(#{
		method => get,
		peer => Peer,
		path => "/chunk/" ++ integer_to_list(Offset)
	}).

get_tx_offset(TXID) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1984},
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/offset"
	}).

get_tx_data(TXID) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1984},
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data"
	}).

get_tx_offset_from_slave(TXID) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1983},
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/offset"
	}).

get_tx_data_from_slave(TXID) ->
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, 1983},
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data"
	}).

post_random_blocks(Master, Wallet) ->
	BlockMap = [
		[v1],
		empty,
		[v2, v1],
		[v2, v2_original_split, v2],
		empty,
		[v1, v2, v2, v2_original_split],
		[v2],
		empty,
		empty,
		[v2_original_split, v2_no_data, v2, v1, v2],
		empty,
		empty,
		empty,
		empty
	],
	lists:foldl(
		fun
			({empty, Height}, Acc) ->
				ar_node:mine(Master),
				wait_until_height(Master, Height),
				Acc;
			({TXMap, _Height}, Acc) ->
				TXsWithChunks = lists:map(
					fun
						(v1) ->
							{v1_tx(Wallet), v1};
						(v2) ->
							{tx(Wallet, {custom_split, random}), v2};
						(v2_no_data) -> % same as v2 but its data won't be submitted
							{tx(Wallet, {custom_split, random}), v2_no_data};
						(v2_original_split) ->
							{tx(Wallet, original_split), v2_original_split}
					end,
					TXMap
				),
				B = post_and_mine(
					#{ miner => {master, Master}, await_on => {master, Master} },
					[TX || {{TX, _}, _} <- TXsWithChunks]
				),
				lists:foreach(
					fun
						({{#tx{ format = 2 } = TX, Chunks}, v2}) ->
							post_proofs_to_master(B, TX, Chunks);
						(_) ->
							ok
					end,
					TXsWithChunks
				),
				[{B, TX, C} || {{TX, C}, Type} <- TXsWithChunks, Type /= v2_no_data] ++ Acc
		end,
		[],
		lists:zip(BlockMap, lists:seq(1, length(BlockMap)))
	).

post_proofs_to_master(B, TX, Chunks) ->
	post_proofs({127, 0, 0, 1, ar_meta_db:get(port)}, B, TX, Chunks).

post_proofs_to_slave(B, TX, Chunks) ->
	post_proofs({127, 0, 0, 1, slave_call(ar_meta_db, get, [port])}, B, TX, Chunks).

post_proofs(Peer, B, TX, Chunks) ->
	Proofs = build_proofs(B, TX, Chunks),
	lists:foreach(
		fun({_, Proof}) ->
			{ok, {{<<"200">>, _}, _, _, _, _}} =
				post_chunk(Peer, jiffy:encode(Proof))
		end,
		Proofs
	),
	Proofs.

wait_until_syncs_chunks(Proofs) ->
	wait_until_syncs_chunks({127, 0, 0, 1, ar_meta_db:get(port)}, Proofs).

slave_wait_until_syncs_chunks(Proofs) ->
	wait_until_syncs_chunks({127, 0, 0, 1, slave_call(ar_meta_db, get, [port])}, Proofs).

wait_until_syncs_chunks(Peer, Proofs) ->
	lists:foreach(
		fun({EndOffset, Proof}) ->
			ar_util:do_until(
				fun() ->
					case get_chunk(Peer, EndOffset) of
						{ok, {{<<"200">>, _}, _, EncodedProof, _, _}} ->
							FetchedProof = ar_serialize:json_map_to_chunk_proof(
								jiffy:decode(EncodedProof, [return_maps])
							),
							ExpectedProof = #{
								chunk => ar_util:decode(maps:get(chunk, Proof)),
								tx_path => ar_util:decode(maps:get(tx_path, Proof)),
								data_path => ar_util:decode(maps:get(data_path, Proof))
							},
							compare_proofs(FetchedProof, ExpectedProof);
						_ ->
							false
					end
				end,
				5 * 1000,
				60 * 1000
			)
		end,
		Proofs
	).

compare_proofs(
	#{ chunk := C, data_path := D, tx_path := T },
	#{ chunk := C, data_path := D, tx_path := T }
) ->
	true;
compare_proofs(_, _) ->
	false.

hash(Parts) when is_list(Parts) ->
	crypto:hash(sha256, binary:list_to_bin(Parts));
hash(Binary) ->
	crypto:hash(sha256, Binary).
