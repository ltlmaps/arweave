-module(app_net_explore).
-export([graph/0, graph/1]).
-export([get_all_nodes/0, get_live_nodes/0]).
-export([filter_offline_nodes/1]).
-export([get_nodes_connectivity/0]).
-export([generate_gephi_csv/0]).
-export([get_nodes_clock_diff/0]).
-export([get_nodes_info/1]).

-export([get_nodes_clock_diff/1, generate_gephi_csv1/1, csv_row/1]).
-export([get_nodes_info/2]).

%%% Tools for building a map of connected peers.
%%% Requires graphviz for visualisation.

%% The directory generated files should be saved to.
-define(OUTPUT_DIR, "net-explore-output").

%% @doc Build a snapshot graph in PNG form of the current state of the network.
graph() ->
    io:format("Getting live peers...~n"),
    graph(get_live_nodes()).
graph(Nodes) ->
    io:format("Generating connection map...~n"),
    Map = generate_map(Nodes),
    ar:d(Map),
    io:format("Generating dot file...~n"),
    Timestamp = erlang:timestamp(),
    DotFile = filename(Timestamp, "graph", "dot"),
    ok = filelib:ensure_dir(DotFile),
    PngFile = filename(Timestamp, "graph", "png"),
    ok = filelib:ensure_dir(PngFile),
    ok = generate_dot_file(DotFile, Map),
    io:format("Generating PNG image...~n"),
    os:cmd("dot -Tpng " ++ DotFile ++ " -o " ++ PngFile),
    io:format("Done! Image written to: '" ++ PngFile ++ "'~n").


%% @doc Return a list of nodes that are active and connected to the network.
get_live_nodes() ->
    filter_offline_nodes(get_all_nodes()).

%% @doc Return a list of all nodes that are claimed to be in the network.
get_all_nodes() ->
    get_all_nodes([], ar_bridge:get_remote_peers(whereis(http_bridge_node))).
get_all_nodes(Acc, []) -> Acc;
get_all_nodes(Acc, [Peer|Peers]) ->
    io:format("Getting peers from ~s... ", [ar_util:format_peer(Peer)]),
    MorePeers = ar_http_iface:get_peers(Peer),
    io:format(" got ~w!~n", [length(MorePeers)]),
    get_all_nodes(
        [Peer|Acc],
        (ar_util:unique(Peers ++ MorePeers)) -- [Peer|Acc]
    ).

%% @doc Remove offline nodes from a list of peers.
filter_offline_nodes(Peers) ->
    lists:filter(
        fun(Peer) ->
            ar_http_iface:get_info(Peer) =/= info_unavailable
        end,
        Peers
    ).

%% @doc Return a three-tuple with every live host in the network, it's average
%% position by peers connected to it, the number of peers connected to it.
get_nodes_connectivity() ->
    nodes_connectivity(generate_map(get_live_nodes())).

%% @doc Create a CSV file with all connections in the network suitable for
%% importing into Gephi - The Open Graph Viz Platform (https://gephi.org/). The
%% weight is based on the Wildfire ranking.
generate_gephi_csv() ->
    generate_gephi_csv(get_live_nodes()).

get_nodes_clock_diff() ->
    get_nodes_clock_diff(get_all_nodes()).

get_nodes_info(Fields) ->
    get_nodes_info([], get_all_nodes()).

%% @doc Return a map of every peers connections.
%% Returns a list of tuples with arity 2. The first element is the local peer,
%% the second element is the list of remote peers it talks to.
generate_map(Peers) ->
    lists:map(
        fun(Peer) ->
            {
                Peer,
                lists:filter(
                    fun(RemotePeer) ->
                        lists:member(RemotePeer, Peers)
                    end,
                    ar_http_iface:get_peers(Peer)
                )
            }
        end,
        Peers
    ).

%% @doc Generate a filename with path for storing files generated by this module.
filename(Type, Extension) ->
    filename(erlang:timestamp(), Type, Extension).

filename(Timestamp, Type, Extension) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} =
        calendar:now_to_datetime(Timestamp),
    StrTime =
        lists:flatten(
            io_lib:format(
                "~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",
                [Year, Month, Day, Hour, Minute, Second]
            )
        ),
    lists:flatten(
        io_lib:format(
            "~s/~s-~s.~s",
            [?OUTPUT_DIR, Type, StrTime, Extension]
        )
    ).

%% @doc Generate a dot file that can be rendered into a PNG.
generate_dot_file(File, Map) ->
    case file:open(File, [write]) of
        {ok, IoDevice} ->
            io:fwrite(IoDevice, "digraph network_map { ~n", []),
            io:fwrite(IoDevice,
                      "    init [style=filled,color=\".7 .3 .9\"];~n", []),
            do_generate_dot_file(Map, IoDevice),
            ok;
        _ ->
            io:format("Failed to open file for writing.~n"),
            io_error
    end.

do_generate_dot_file([], File) ->
    io:fwrite(File, "} ~n", []),
    file:close(File);
do_generate_dot_file([Host|Rest], File) ->
    {IP, Peers} = Host,
    lists:foreach(
        fun(Peer) ->
            io:fwrite(
                File,
                "\t\"~s\"  ->  \"~s\";  ~n",
                [ar_util:format_peer(IP), ar_util:format_peer(Peer)])
        end,
        Peers
    ),
    do_generate_dot_file(Rest, File).

%% @doc Takes a host-to-connections map and returns a three-tuple with every
%% live host in the network, it's average position by peers connected to it, the
%% number of peers connected to it.
nodes_connectivity(ConnectionMap) ->
    WithoutScore = [{Host, empty_score} || {Host, _} <- ConnectionMap],
    WithoutScoreMap = maps:from_list(WithoutScore),
    WithScoreMap = avg_connectivity_score(add_connectivity_score(WithoutScoreMap,
                                                                 ConnectionMap)),
    WithScore = [{Host, SumPos, Count} || {Host, {SumPos, Count}}
                                          <- maps:to_list(WithScoreMap)],
    lists:keysort(2, WithScore).

%% @doc Updates the connectivity intermediate scores according the connection
%% map.
add_connectivity_score(ScoreMap, []) ->
    ScoreMap;
add_connectivity_score(ScoreMap, [{_, Connections} | ConnectionMap]) ->
    NewScoreMap = add_connectivity_score1(ScoreMap, add_list_position(Connections)),
    add_connectivity_score(NewScoreMap, ConnectionMap).

%% @doc Updates the connectivity scores according the connection map.
add_connectivity_score1(ScoreMap, []) ->
    ScoreMap;
add_connectivity_score1(ScoreMap, [{Host, Position} | Connections]) ->
    Updater = fun
        (empty_score) ->
            {Position, 1};
        ({PositionSum, Count}) ->
            {PositionSum + Position, Count + 1}
    end,
    NewScoreMap = maps:update_with(Host, Updater, ScoreMap),
    add_connectivity_score1(NewScoreMap, Connections).

%% @doc Wraps each element in the list in a two-tuple where the second element
%% is the element's position in the list.
add_list_position(List) ->
    add_list_position(List, 1, []).

add_list_position([], _, Acc) ->
    lists:reverse(Acc);
add_list_position([Item | List], Position, Acc) ->
    NewAcc = [{Item, Position} | Acc],
    add_list_position(List, Position + 1, NewAcc).

%% @doc Replace the intermediate score (the sum of all positions and the number
%% of connections) with the average position and the number of connections.
avg_connectivity_score(Hosts) ->
    Mapper = fun (_, {PositionSum, Count}) ->
        {PositionSum / Count, Count}
    end,
    maps:map(Mapper, Hosts).

%% @doc Like generate_gephi_csv/0 but takes a list of the nodes to use in the
%% export.
generate_gephi_csv(Nodes) ->
    generate_gephi_csv1(generate_map(Nodes)).

%% @doc Like generate_gephi_csv/0 but takes the host-to-peers map to use in the
%% export.
generate_gephi_csv1(Map) ->
    CsvHeader = [<<"Source">>, <<"Target">>, <<"Weight">>],
    CsvBody = gephi_csv_body(gephi_edges(Map)),
    CsvRows = [CsvHeader | CsvBody],
    write_csv_file("gephi", CsvRows).

write_csv_file(NamePrefix, CsvRows) ->
    {IoDevice, File} = create_csv_file(NamePrefix),
    write_csv_rows(CsvRows, IoDevice),
    close_csv_file(IoDevice, File, NamePrefix).

%% @doc Create the new CSV file in write mode and return the IO device and the
%% filename.
create_csv_file(NamePrefix) ->
    CsvFile = filename(NamePrefix, "csv"),
    ok = filelib:ensure_dir(CsvFile),
    {ok, IoDevice} = file:open(CsvFile, [write]),
    {IoDevice, CsvFile}.

%% @doc Transform the host to peers map into a list of all connections where
%% each connection is a three-tuple of host, peer, weight.
gephi_edges(Map) ->
    gephi_edges(Map, []).

gephi_edges([], Acc) ->
    Acc;
gephi_edges([{Host, Peers} | Map], Acc) ->
    PeersWithPosition = add_list_position(Peers),
    Folder = fun ({Peer, Position}, FolderAcc) ->
        [{Host, Peer, 1 / Position} | FolderAcc]
    end,
    NewAcc = lists:foldl(Folder, Acc, PeersWithPosition),
    gephi_edges(Map, NewAcc).

gephi_csv_body(Edges) ->
    Mapper = fun ({Host, Peer, Weight}) ->
        [ar_util:format_peer(Host),
         ar_util:format_peer(Peer),
         io_lib:format("~f", [Weight])]
    end,
    lists:map(Mapper, Edges).

%% @doc TBD.
write_csv_rows([], _) ->
    done;
write_csv_rows([Row | Rows], IoDevice) ->
    ok = file:write(IoDevice, csv_row(Row)),
    write_csv_rows(Rows, IoDevice).

%% @doc TBD
csv_row(Row) ->
    iolist_to_binary(csv_row(Row, [])).

csv_row([], _) ->
    [];
csv_row([LastValue], Acc) ->
    lists:reverse(["\n", LastValue | Acc]);
csv_row([Value | Values], Acc) ->
    csv_row(Values, [<<",">>, Value | Acc]).

%% @doc TBD
close_csv_file(IoDevice, File, NamePrefix) ->
    ok = file:close(IoDevice),
    io:format("~s CSV file written to: '~s'~n", [NamePrefix, File]).

get_nodes_clock_diff(Peers) ->
    [{Peer, get_node_clock_diff(Peer)} || Peer <- Peers].

get_node_clock_diff(Peer) ->
    Start = os:system_time(second),
    PeerTime = ar_http_iface:get_time(Peer, 3000),
    End = os:system_time(second),
    node_clock_diff(Start, PeerTime, End).

node_clock_diff(_, unknown, _) ->
    unknown;
node_clock_diff(CheckStart, PeerTime, _) when PeerTime < CheckStart ->
    PeerTime - CheckStart;
node_clock_diff(_, PeerTime, CheckEnd) when PeerTime > CheckEnd ->
    PeerTime - CheckEnd;
node_clock_diff(_, _, _) ->
    0.

get_nodes_info(Fields, Nodes) ->
    CsvRows = [info_csv_header(Fields) | info_csv_body(Fields, Nodes)],
    write_csv_file("node_info", CsvRows).

info_csv_header(Fields) ->
    lists:map(fun (Field) -> erlang:atom_to_binary(Field, utf8) end, Fields).

info_csv_body(Fields, Nodes) ->
    Mapper = fun (Peer) ->
        FieldCsvValues = case ar_http_iface:get_info(Peer) of
            info_unavailable -> lists:duplicate(length(Fields), <<"info_unavailable">>);
            FieldValues -> lists:map(fun to_binary/1, FieldValues)
        end,
        [ar_util:format_peer(Peer) | FieldCsvValues]
    end,
    lists:map(Mapper, Nodes).

to_binary(Atom) when is_atom(Atom) -> erlang:atom_to_binary(Atom, utf8);
to_binary(Int) when is_integer(Int) -> integer_to_binary(Int);
to_binary(Float) when is_float (Float) ->
    list_to_binary(io_lib:format("~f", [Float])).
to_binary(Value) -> iolist_to_binary(Value).
