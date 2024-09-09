import csv
import network_decentralization.helper as hlp
import networkx as nx
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


LEDGERS = ['bitcoin', 'bitcoin_cash', 'dogecoin', 'litecoin', 'zcash']
LEDGERS = ['bitcoin_cash', 'dogecoin', 'litecoin', 'zcash']


network_edge_dir = hlp.get_output_directory() / 'network_edges'

for ledger in LEDGERS:
    logging.info(f'Analyzing {ledger}')

    output_dir = hlp.get_output_directory()
    edges = []
    nodes = set()
    try:
        with open(output_dir / 'network_edges' / f'{ledger}.csv') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            for source, dest in csv_reader:
                nodes.add(source)
                nodes.add(dest)
                if source != dest:
                    edges.append((source, dest))
    except FileNotFoundError:
        continue

    G = nx.DiGraph()
    G.add_edges_from(edges)

    all_nodes = list(G.nodes())
    logging.info(f'\t Nodes: {len(nodes):,} - Edges: {len(edges):,}')
    logging.info(f'\t     Isolated nodes (no in/out edges): {len(nodes)-len(all_nodes):,}')

    degrees = G.degree()
    avg_degree = sum([i[1] for i in degrees]) / len(degrees)
    logging.info(f'\t Average node degree: {avg_degree:,}')

    is_strongly_connected = nx.is_strongly_connected(G)
    logging.info(f'\t Is strongly connected: {is_strongly_connected}')

    if is_strongly_connected:
        diameter = nx.diameter(G)
        logging.info(f'\t Diameter (largest component): {diameter:,}')
    else:
        diameter = {}
        for node in all_nodes:
            shortest_paths = nx.shortest_path(G, source=node)
            longest_shortest_path = max(shortest_paths.items(), key=lambda x: len(x[1]))[1]
            if longest_shortest_path == [node]:
                diameter[node] = -1
            else:
                diameter[node] = len(longest_shortest_path)
        logging.info(f'\t Diameter of known graph: {max(diameter.values())}')
        logging.info(f'\t Nodes without outgoing edges: {len([i for i in diameter if diameter[i] == -1])}')
