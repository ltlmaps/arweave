%% @doc The time to wait after the whole weave is synced
%% before looking for new chunks.
-ifdef(DEBUG).
-define(PAUSE_AFTER_COULD_NOT_FIND_CHUNK_MS, 2000).
-else.
-define(PAUSE_AFTER_COULD_NOT_FIND_CHUNK_MS, 30000).
-endif.

%% @doc The number of peer sync records to consult each time we look for an interval to sync.
-define(CONSULT_PEER_RECORDS_COUNT, 5).
%% @doc The number of best peers to pick ?CONSULT_PEER_RECORDS_COUNT from, to fetch the
%% corresponding number of sync records.
-define(PICK_PEERS_OUT_OF_RANDOM_N, 20).

%% @doc The frequency of updating best peers' sync records.
-ifdef(DEBUG).
-define(PEER_SYNC_RECORDS_FREQUENCY_MS, 2000).
-else.
-define(PEER_SYNC_RECORDS_FREQUENCY_MS, 2 * 60 * 1000).
-endif.

%% @doc The time to wait between syncing different portions of
%% the weave.
-ifdef(DEBUG).
-define(SYNC_FREQUENCY_MS, 1000).
-else.
-define(SYNC_FREQUENCY_MS, 200).
-endif.

%% @doc The size in bits of the offset key in kv databases.
-define(OFFSET_KEY_BITSIZE, (?NOTE_SIZE * 8)).

%% @doc The number of block confirmations to track. When the node
%% joins the network or a chain reorg occurs, it uses its record about
%% the last ?TRACK_CONFIRMATIONS blocks and the new block index to
%% determine the orphaned portion of the weave.
-define(TRACK_CONFIRMATIONS, ?STORE_BLOCKS_BEHIND_CURRENT * 2).

%% @doc The maximum number of synced intervals shared with peers.
-ifdef(DEBUG).
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 100).
-else.
-define(MAX_SHARED_SYNCED_INTERVALS_COUNT, 10000).
-endif.

%% @doc The upper limit for the size of a sync record serialized using Erlang Term Format.
-define(MAX_ETF_SYNC_RECORD_SIZE, 80 * ?MAX_SHARED_SYNCED_INTERVALS_COUNT).

%% @doc The upper size limit for a serialized chunk with its proof
%% as it travels around the network.
%%
%% It is computed as ?MAX_PATH_SIZE (data_path) + DATA_CHUNK_SIZE (chunk) +
%% 32 * 1000 (tx_path, considering the 1000 txs per block limit),
%% multiplied by 1.34 (Base64), rounded to the nearest 50000 -
%% the difference is sufficient to fit an offset, a data_root,
%% and special JSON chars.
-define(MAX_SERIALIZED_CHUNK_PROOF_SIZE, 750000).

%% @doc Transaction data bigger than this limit is not served in
%% GET /tx/<id>/data endpoint. Clients interested in downloading
%% such data should fetch it chunk by chunk.
-define(MAX_SERVED_TX_DATA_SIZE, 12 * 1024 * 1024).

%% @doc The frequency of removing the old entries from the orphaned data roots record.
-define(ORPHANED_DATA_ROOTS_CLEANUP_FREQUENCY_MS, 60 * 60 * 1000).

%% @doc The expiration time for the information about an orhpaned data root.
-define(ORPHANED_DATA_ROOT_EXPIRATION_TIME_S, 2 * 60 * 60).

%% @doc The state of the server managing data synchronization.
-record(sync_data_state, {
	%% @doc The mapping absolute_end_offset -> absolute_start_offset
	%% sorted by absolute_end_offset.
	%%
	%% Every such pair denotes a synced half-closed interval on the [0, weave size)
	%% half-closed interval. This mapping serves as a compact map
	%% of what is synced by the node. No matter how big the weave is
	%% or how much of it the node stores, this record can remain very small,
	%% compared to storing all chunk and transaction identifiers,
	%% whose number can effectively grow unlimited with time.
	%%
	%% Every time a chunk is written to sync_record, it is also
	%% written to the chunk_index.
	sync_record,
	%% @doc The mapping peer -> sync_record containing sync records of the best peers.
	peer_sync_records,
	%% @doc The last ?TRACK_CONFIRMATIONS entries of the block index.
	%% Used to determine orphaned data upon startup.
	block_index,
	%% @doc The current weave size. The upper limit for the absolute chunk offsets.
	weave_size,
	%% @doc A reference to the on-disk key-value storage mapping
	%% absolute_chunk_end_offset -> {data_path_hash, tx_root, data_root, tx_path, chunk_size}
	%% for all synced chunks.
	%%
	%% Chunks themselves and their data_paths are stored separately
	%% in files (identified by data_path hashes) inside folders identified
	%% by tx_roots. This is made to avoid moving potentially huge data
	%% around during chain reorgs. Orphaned chunks can be removed by tx_root
	%% at any later point after a reorg.
	%%
	%% The index is used to look up the chunk by a random offset when a peer
	%% asks about it and to look up chunks of a transaction.
	%%
	%% Every time a chunk is written to sync_record, it is also
	%% written to the chunks_index.
	chunks_index,
	%% @doc A reference to the on-disk key-value storage mapping
	%% data_root -> tx_root -> absolute_tx_start_offset -> {tx_path, tx_size}.
	%% The index is used to look up tx_root for a submitted chunk and
	%% to compute absolute_chunk_end_offset for the accepted chunk.
	%% We need the index because users should be able to submit their
	%% data without monitoring the chain, otherwise chain reorganisations
	%% might make the experience very unnerving. The index is NOT used
	%% for serving random chunks therefore it is possible to develop
	%% a lightweight client which would sync and serve random portions
	%% of the weave without maintaining this index.
	data_root_index,
	%% @doc A reference to the on-disk key-value storage mapping
	%% absolute_block_start_offset -> {tx_root, block_size, data_root_set}.
	%% Used to remove orphaned entries from data_root_index and to determine
	%% tx_root when syncing random offsets of the weave.
	%% data_root_set may be empty - in this case, the corresponding index entry
	%% is only used to for syncing the weave.
	data_root_offset_index,
	%% @doc The mapping
	%% data_root -> {timestamp, data_path_hash -> {relative_chunk_offset, chunk_size}}.
	%% The mapping is used to keep track of the orphaned chunks. When a
	%% chain reorg happens, the information about every chunk above the base
	%% weave offset is moved from chunks_index to orphans_index. Then, chunks_index
	%% is updated with chunks found in orphans_index, with their absolute
	%% offsets recomputed.
	orphaned_chunks,
	%% @doc A reference to the on-disk key value storage mapping
	%% tx_id -> {absolute_end_offset, tx_size}.
	%% It is used to serve transaction data by TXID.
	tx_index,
	%% @doc A reference to the on-disk key value storage mapping
	%% absolute_tx_start_offset -> tx_id. It is used to cleanup orphaned
	%% transactions from tx_index.
	tx_offset_index,
	%% @doc not_joined | joined. Not stored.
	status
}).
