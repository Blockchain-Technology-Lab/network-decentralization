import helper as hlp
import matplotlib.pyplot as plt
import csv
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def geo_plot(plot_type, layer):
    """
    Generates pie charts representing node distribution by a given category
    :param plot_type: Countries or Organizations
    :param layer: Consensus or Execution
    """
    logging.info(f'Plotting {plot_type} - {layer}')
    entries = []
    output_dir = hlp.get_output_directory()
    with open(f'{output_dir}/{plot_type.lower()}_{layer}.csv') as f:
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

    plt.title(f'Nodes {plot_type} - {layer} (Total: {sum(sizes):,})', fontsize=32)

    figure = plt.gcf()  # get current figure
    figure.set_size_inches(20, 15)

    plt.savefig(f'{output_dir}/{plot_type.lower()}_{layer}.png', bbox_inches='tight', dpi=100)
    plt.close(fig)

LAYERS = hlp.get_layers()
MODES = hlp.get_mode()

def main():
    for layer in LAYERS:
        for mode in MODES:
            geo_plot(mode, layer)

if __name__ == '__main__':
    main()
