-module(app_internet_archive).
-export([store/2]).
-export([generate_items/0]).
-include("ar.hrl").

%%% An application that archives hashes and (optionally) torrents from the
%%% Internet Archive, allowing for
%%% A). Verifiability and trustless timestamping of data stored in the archive.
%%% B). Where torrent are available, fully decentralised access to the IA.

%% Represents an item from the IA to be stored inside the Arweave.
-record(item, {
    id,
    hash,
    meta_data = [],
    torrent = <<>>
}).

%% @doc Start an archiving node. Takes a list of Items to archive and the
%% location of a wallet.
store(Wallet, Items) ->
    store(whereis(http_entrypoint_node), Wallet, Items).
store(Node, WalletLoc, Items) when not is_tuple(WalletLoc) ->
    store(Node, ar_wallet:load_keyfile(WalletLoc), Items);
store(Node, Wallet, Items) ->
    ssl:start(),
    Queue = app_queue:start(Node, Wallet),
    lists:foreach(
        fun(I) -> app_queue:add(Queue, item_to_tx(I)) end,
        Items
    ),
    Queue.

%% @doc Generate an Arweave transaction for an IA item.
item_to_tx(I) ->
    #tx {
        tags =
            [
                {"app_name", "InternetArchive"},
                {"id", I#item.id},
                {"hash", ar_util:encode(I#item.hash)},
                {"Content-Type", "application/x-bittorrent"}
            ] ++ I#item.meta_data,
        data =
            case I#item.torrent of
                undefined -> <<>>;
                URL ->
                    case httpc:request(URL) of
                        {ok, {{_, 200, _}, _, Body}} ->
                            list_to_binary(Body);
                        _ ->
                            ar:report_console(
                                [
                                    {could_not_get_torrent, I#item.id},
                                    {url, URL}
                                ]
                            )
                    end
            end
    }.

%% @doc Generate a list of items to store in the Arweave.
%% For the moment, this is static test data, until we have a way to dynamically
%% extract it.
%% TODO: Generate this list from an index.
generate_items() ->
    [
        #item {
            id = "NineteenEightyFour1984ByGeorgeOrwell",
            hash = crypto:strong_rand_bytes(32),
            torrent =
                "https://archive.org/download"
                    "/NineteenEightyFour1984ByGeorgeOrwell"
                    "/NineteenEightyFour1984ByGeorgeOrwell_archive.torrent",
            meta_data =
                [
                    {"Identifier-ark", "ark:/13960/t4jm8s98h"},
                    {"Ocr", "ABBYY FineReader 11.0 (Extended OCR)"},
                    {"Ppi", "300"},
                    {"Scanner", "Internet Archive HTML5 Uploader 1.6.3"}
                ]
        }
    ].