import network_decentralization.helper as hlp
import matplotlib.pyplot as plt
import numpy as np
import csv
import json
import networkx as nx
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def network_edges():
    """
    Generates network graphs using edge lists from CSV files.
    """
    logging.info('Plotting edge graphs')

    for ledger in LEDGERS:
        output_dir = hlp.get_output_directory()
        edges = []
        try:
            with open(output_dir / 'network_edges' / f'{ledger}.csv') as f:
                csv_reader = csv.reader(f)
                next(csv_reader)
                for source, dest in csv_reader:
                    if source != dest:
                        edges.append((source, dest))
        except FileNotFoundError:
            return

        G = nx.DiGraph()
        G.add_edges_from(edges)

        fig, ax = plt.subplots(figsize=(20, 15))

        pos = nx.spring_layout(G)
        nx.draw_networkx_nodes(G, pos, node_size=50)
        logging.info(f'{ledger} - nodes')
        # nx.draw_networkx_labels(G, pos)
        # logging.info('labels')
        nx.draw_networkx_edges(G, pos, edgelist=G.edges())
        logging.info(f'{ledger} - edges')

        # plt.show()

        plt.savefig(f'output/graph_{ledger}.png', bbox_inches='tight', dpi=100)
        plt.close(fig)

        logging.info(f'{ledger} - finished')

def geo_plot(plot_type):
    """
    Generates pie charts representing node distribution by a given category
    :param plot_type: Geography, ASN, or Org
    """
    if 'bitcoin' in LEDGERS:
        LEDGERS.append('bitcoin_without_tor')
    for ledger in LEDGERS:
        logging.info(f'Plotting {ledger} {plot_type}')
        entries = []
        try:
            with open(f'output/{plot_type.lower()}_{ledger}.csv') as f:
                csv_reader = csv.reader(f)
                next(csv_reader)
                for line in csv_reader:
                    entries.append([line[0], int(line[1])])
        except FileNotFoundError:
            logging.info(f'plot.py: FileNotFoundError: output/{plot_type.lower()}_{ledger}.csv')
            continue

        total_nodes = sum([i[1] for i in entries])

        labels, sizes = [], []
        for entry in entries:
            if entry[1] / total_nodes > 0.01:
                labels.append(f'{entry[0]} ({entry[1]:,})')
            else:
                labels.append('')
            sizes.append(entry[1])
        legend_labels = [i for i in labels if i]

        fig, ax = plt.subplots()
        # ax.pie(sizes, labels=labels, textprops={'fontsize': 80}, counterclock=False, startangle=90)
        ax.pie(sizes, textprops={'fontsize': 20}, counterclock=False, startangle=90)
        plt.legend(legend_labels, loc='upper right', fontsize=12)

        plt.title(f'{ledger.replace("_", " ").title()} Nodes {plot_type} (Total: {sum(sizes):,})', fontsize=32)

        figure = plt.gcf()  # get current figure
        figure.set_size_inches(20, 15)

        plt.savefig(f'output/{plot_type.lower()}_{ledger}.png', bbox_inches='tight', dpi=100)
        plt.close(fig)

def version_plot():
    """
    Generates pie charts showing the distribution of client versions
    """
    for ledger in LEDGERS:
        logging.info(f'Plotting {ledger} version')
        entries = []
        with open(f'output/version_{ledger}.csv') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            for line in csv_reader:
                entries.append([line[0], int(line[1])])

        total_nodes = sum([i[1] for i in entries])

        labels, sizes = [], []
        for entry in entries:
            if entry[1] / total_nodes > 0.01:
                labels.append(f'{entry[0]} ({entry[1]:,})')
            else:
                labels.append('')
            sizes.append(entry[1])
        legend_labels = [i for i in labels if i]

        fig, ax = plt.subplots()
        # ax.pie(sizes, labels=labels, textprops={'fontsize': 80}, counterclock=False, startangle=90)
        ax.pie(sizes, textprops={'fontsize': 20}, counterclock=False, startangle=90)
        plt.legend(legend_labels, loc='upper right', fontsize=12)

        plt.title(f'{ledger.replace("_", " ").title()} nodes version (Total: {sum(sizes):,})', fontsize=32)

        figure = plt.gcf()  # get current figure
        figure.set_size_inches(20, 15)

        plt.savefig(f'output/version_{ledger}.png', bbox_inches='tight', dpi=100)
        plt.close(fig)

def ip_type_plot():
    """
    Generates a bar chart of node IP address types (IPv4, IPv6, onion)
    """
    logging.info('Plotting by ip type')

    with open('output/ip_type.csv') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)

        ledgers, ipv4, ipv6, onion = [], [], [], []
        for line in csv_reader:
            ledgers.append(line[0])
            ipv4.append(int(line[1]))
            ipv6.append(int(line[2]))
            onion.append(int(line[3]))

    # set width of bar
    barWidth = 0.25
    fig, ax = plt.subplots()

    # Set position of bar on X axis
    br1 = np.arange(len(ipv4))
    br2 = [x + barWidth for x in br1]
    br3 = [x + barWidth for x in br2]

    # Make the plot
    plt.bar(br1, ipv4, color='r', width=barWidth, edgecolor='grey', label='IPv4')
    plt.bar(br2, ipv6, color='g', width=barWidth, edgecolor='grey', label='IPv6')
    plt.bar(br3, onion, color='b', width=barWidth, edgecolor='grey', label='onion')

    # Adding Xticks
    plt.xlabel('Ledger', fontsize=20)
    plt.ylabel('Number of nodes', fontsize=20)
    plt.xticks([r + barWidth for r in range(len(ipv4))], [ledger.replace('_', ' ').title() for ledger in ledgers], fontsize=17)
    plt.yticks(fontsize=17)
    plt.legend(fontsize=20)

    plt.title('Address types (reachable nodes)', fontsize=32)

    figure = plt.gcf()  # get current figure
    figure.set_size_inches(20, 15)

    plt.savefig('output/ip_type.png', bbox_inches='tight', dpi=100)
    plt.close(fig)

def response_length_plot():
    """
    Generates bar charts showing the distribution of response lengths (number of addresses in response)
    """
    logging.info('Plotting by response length')

    with open('output/response_length.json') as f:
        data = json.load(f)

    for ledger in LEDGERS:
        logging.info(f'Plotting {ledger}')
        ledger_data = data[ledger]

        x = [i[0] for i in ledger_data]
        y = [i[1] for i in ledger_data]

        fig, ax = plt.subplots()

        ax.bar(x, y, width=5)

        plt.title(f'{ledger.replace("_", " ").title()} average response sizes', fontsize=32)
        plt.xlabel('Addresses in response', fontsize=20)
        plt.xticks(fontsize=15)
        plt.ylabel('Nodes', fontsize=20)
        plt.yticks(fontsize=15)

        figure = plt.gcf()  # get current figure
        figure.set_size_inches(20, 15)

        plt.savefig(f'output/response_size_{ledger}.png', bbox_inches='tight', dpi=100)
        plt.close(fig)

LEDGERS = hlp.get_ledgers()
MODES = hlp.get_mode()

def main():
    for mode in MODES:
        geo_plot(mode)

if __name__ == '__main__':
    main()
