"""
Plot Cardano distribution charts.
"""
import matplotlib.pyplot as plt
import csv
import logging
import helper as hlp

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def plot_pie_chart(plot_type='Countries'):
    """Create pie chart for country/organization/ASN distribution."""
    ledger = 'cardano'
    output_dir = hlp.get_output_directory()
    
    csv_file = output_dir / f'{plot_type.lower()}_{ledger}.csv'
    
    if not csv_file.exists():
        logging.error(f'CSV file not found: {csv_file}')
        logging.info(f'Please run parse_cardano.py first!')
        return
    
    logging.info(f'Plotting {ledger} {plot_type}')
    
    # Read data from CSV
    entries = []
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        for line in csv_reader:
            try:
                # Get the latest date column (last column)
                count = int(line[-1]) if line[-1] else 0
                if count > 0:
                    entries.append([line[0], count])
            except (ValueError, IndexError) as e:
                logging.debug(f'Skipping row: {line} ({e})')
                continue
    
    if not entries:
        logging.warning(f'No data found in {csv_file}')
        return
    
    total_nodes = sum([i[1] for i in entries])
    logging.info(f'Total nodes: {total_nodes}')
    
    # Prepare labels and sizes
    labels, sizes = [], []
    for entry in entries:
        # Only label if > 1% of total
        if entry[1] / total_nodes > 0.01:
            labels.append(f'{entry[0]} ({entry[1]:,})')
        else:
            labels.append('')
        sizes.append(entry[1])
    
    # Create pie chart
    fig, ax = plt.subplots(figsize=(20, 15))
    wedges, texts = ax.pie(sizes, textprops={'fontsize': 20}, counterclock=False, startangle=90)
    
    # Create legend with matching colors
    legend_labels = []
    legend_wedges = []
    for idx, label in enumerate(labels):
        if label:  # Only include non-empty labels
            legend_labels.append(label)
            legend_wedges.append(wedges[idx])
    
    plt.legend(legend_wedges, legend_labels, loc='upper right', fontsize=12)
    
    plt.title(f'Cardano Relay Nodes {plot_type} (Total: {total_nodes:,})', fontsize=32)
    
    # Save figure
    output_file = output_dir / f'{plot_type.lower()}_{ledger}.png'
    plt.savefig(output_file, bbox_inches='tight', dpi=100)
    plt.close(fig)
    
    logging.info(f'Saved plot to {output_file}')


def main():
    modes = hlp.get_mode()
    
    for mode in modes:
        try:
            plot_pie_chart(mode)
        except Exception as e:
            logging.error(f'Error plotting {mode}: {e}')
    
    logging.info('\nPlotting complete! Check the output/ directory for PNG files.')


if __name__ == '__main__':
    main()
