import network_decentralization.helper as hlp
import matplotlib.pyplot as plt
import numpy as np
import csv
import json
import networkx as nx
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def network_edges():
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

        pos = nx.spring_layout(G)
        nx.draw_networkx_nodes(G, pos, node_size=50)
        logging.info(f'{ledger} - nodes')
        # nx.draw_networkx_labels(G, pos)
        # logging.info('labels')
        nx.draw_networkx_edges(G, pos, edgelist=G.edges())
        logging.info(f'{ledger} - edges')

        # plt.show()
        figure = plt.gcf()  # get current figure
        figure.set_size_inches(80, 60)

        plt.savefig(f'output/graph_{ledger}.png', bbox_inches='tight', dpi=100)

        logging.info(f'{ledger} - finished')


def geo_plot(plot_type):
    for ledger in LEDGERS:
        logging.info(f'Plotting {ledger} {plot_type}')
        entries = []
        with open(f'output/{plot_type.lower()}_{ledger}.csv') as f:
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
        ax.pie(sizes, textprops={'fontsize': 80}, counterclock=False, startangle=90)
        plt.legend(legend_labels, loc='upper right', fontsize=50)

        plt.title(f'{ledger.replace("_", " ").title()} Nodes {plot_type} (Total: {sum(sizes):,})', fontsize=130)

        figure = plt.gcf()  # get current figure
        figure.set_size_inches(80, 60)

        plt.savefig(f'output/{plot_type.lower()}_{ledger}.png', bbox_inches='tight', dpi=100)


def version_plot():
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
        ax.pie(sizes, textprops={'fontsize': 80}, counterclock=False, startangle=90)
        plt.legend(legend_labels, loc='upper right', fontsize=50)

        plt.title(f'{ledger.replace("_", " ").title()} nodes version (Total: {sum(sizes):,})', fontsize=130)

        figure = plt.gcf()  # get current figure
        figure.set_size_inches(80, 60)

        plt.savefig(f'output/version_{ledger}.png', bbox_inches='tight', dpi=100)


def ip_type_plot():
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
    plt.subplots()

    # Set position of bar on X axis
    br1 = np.arange(len(ipv4))
    br2 = [x + barWidth for x in br1]
    br3 = [x + barWidth for x in br2]

    # Make the plot
    plt.bar(br1, ipv4, color='r', width=barWidth, edgecolor='grey', label='IPv4')
    plt.bar(br2, ipv6, color='g', width=barWidth, edgecolor='grey', label='IPv6')
    plt.bar(br3, onion, color='b', width=barWidth, edgecolor='grey', label='onion')

    # Adding Xticks
    plt.xlabel('Ledger', fontsize=80)
    plt.ylabel('Number of nodes', fontsize=80)
    plt.xticks([r + barWidth for r in range(len(ipv4))], [ledger.replace('_', ' ').title() for ledger in ledgers], fontsize=70)
    plt.yticks(fontsize=70)
    plt.legend(fontsize=80)

    plt.title('Address types (reachable nodes)', fontsize=130)

    figure = plt.gcf()  # get current figure
    figure.set_size_inches(80, 60)

    plt.savefig('output/ip_type.png', bbox_inches='tight', dpi=100)


def response_length_plot():
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

        plt.title(f'{ledger.replace("_", " ").title()} average response sizes', fontsize=130)
        plt.xlabel('Addresses in response', fontsize=80)
        plt.xticks(fontsize=60)
        plt.ylabel('Nodes', fontsize=80)
        plt.yticks(fontsize=60)

        figure = plt.gcf()  # get current figure
        figure.set_size_inches(80, 60)

        plt.savefig(f'output/response_size_{ledger}.png', bbox_inches='tight', dpi=100)

LEDGERS = hlp.get_ledgers()

def main():
    geo_plot('Geography')
    geo_plot('ASN')
    ip_type_plot()
    version_plot()
    #network_edges()
    response_length_plot()

if __name__ == '__main__':
    main()
