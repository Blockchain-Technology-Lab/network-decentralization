import helper as hlp
import matplotlib.pyplot as plt
import numpy as np
import csv
import json
import networkx as nx
import logging
import time

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def geo_plot(plot_type):
    logging.info(f'Plotting {plot_type}')
    entries = []
    with open(f'results/{plot_type.lower()}.csv') as f:
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

    plt.title(f'Nodes {plot_type} (Total: {sum(sizes):,})', fontsize=32)

    figure = plt.gcf()  # get current figure
    figure.set_size_inches(20, 15)

    plt.savefig(f'results/{plot_type.lower()}.png', bbox_inches='tight', dpi=100)
    plt.close(fig)


def main():
    geo_plot('Geography')
#    geo_plot('ASN')
#    geo_plot('Org')
#    ip_type_plot()
#    version_plot()
  # network_edges()
#    response_length_plot()

#        time.sleep(30)

if __name__ == '__main__':
    main()
