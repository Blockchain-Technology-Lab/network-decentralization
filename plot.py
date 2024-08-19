import matplotlib.pyplot as plt
import numpy as np
import csv
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

LEDGERS = ['bitcoin', 'dogecoin', 'litecoin', 'zcash']


def geo_plot(plot_type):
    for ledger in LEDGERS:
        logging.info(f'Plotting {ledger} {plot_type}')
        labels, sizes = [], []
        with open(f'output/{plot_type}_{ledger}.csv') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            for line in csv_reader:
                label_threshold = 11
                if len(labels) < label_threshold:
                    labels.append(f'{line[0]} ({line[1]})')
                else:
                    labels.append('')
                sizes.append(int(line[1]))

        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, textprops={'fontsize': 80}, counterclock=False, startangle=90)

        plt.title(f'{ledger.title()} nodes {plot_type} (Total: {sum(sizes)})', fontsize=130)

        figure = plt.gcf()  # get current figure
        figure.set_size_inches(80, 60)

        plt.savefig(f'output/{plot_type}_{ledger}.png', bbox_inches='tight', dpi=100)


def version_plot():
    for ledger in LEDGERS:
        logging.info(f'Plotting {ledger} version')
        labels, sizes = [], []
        with open(f'output/version_{ledger}.csv') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            for line in csv_reader:
                label_threshold = 11
                if len(labels) < label_threshold:
                    labels.append(f'{line[0]} ({line[1]})')
                else:
                    labels.append('')
                sizes.append(int(line[1]))

        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, textprops={'fontsize': 80}, counterclock=False, startangle=90)

        plt.title(f'{ledger.title()} nodes version (Total: {sum(sizes)})', fontsize=130)

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
    plt.xticks([r + barWidth for r in range(len(ipv4))], [ledger.title() for ledger in ledgers], fontsize=70)
    plt.yticks(fontsize=70)
    plt.legend(fontsize=80)

    plt.title('Node address types', fontsize=130)

    figure = plt.gcf()  # get current figure
    figure.set_size_inches(80, 60)

    plt.savefig('output/ip_type.png', bbox_inches='tight', dpi=100)


# geo_plot('geography')
# geo_plot('asn')
# ip_type_plot()
version_plot()
