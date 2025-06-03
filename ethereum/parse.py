import json
import csv
from pathlib import Path
import helper as hlp
from collections import defaultdict
import logging
import pandas as pd
from datetime import datetime


logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def get_geodata(layer, nodes, mode):
    """
    Groups nodes by geolocation information.
    :param layer: the layer to analyse
    :param nodes: dictionary mapping ledger names to nodes information
    :param mode: Grouping mode: 'Countries', 'ASN' or 'Organisations'
    :return: dictionary grouping node IPs under corresponding geographic/organisational keys.
    """
    output_dir = hlp.get_output_directory()
    countries = defaultdict(list)

    with open(output_dir / 'geodata.json') as f:
        geodata = json.load(f)

    for node in nodes:
        ip_addr = node[0]
        if ip_addr in geodata:
            ip_info = geodata[ip_addr]
            if 'error' in ip_info and ip_info['error']:
                continue
            if mode == 'Countries':
                try:
                    countries[ip_info['country']].append(ip_addr)
                except KeyError:
                    countries[ip_info['location']['country']].append(ip_addr) # The API used to geolocate IP addresses has been changed, so the fields no longer have the same name.
            elif mode == 'ASN':
                 try:
                     asn = (ip_info['as'].split())[0]
                     countries[f'{asn}'].append(ip_addr)
                 except KeyError:
                     asn = "AS" + ip_info['asn']['asn']
                     countries[f'{asn}'].append(ip_addr) # The API used to geolocate IP addresses has been changed, so the fields no longer have the same name.
            elif mode == 'Organizations':
                 try:
                     countries[f"{ip_info['org']}"].append(ip_addr)
                 except KeyError:
                     countries[ip_info['asn']['org']].append(ip_addr)
        elif ip_addr.endswith('onion'):
            countries['Tor'].append(ip_addr)
        else:
            countries['Unknown'].append(ip_addr)

    return countries


def geography(nodes, layer, mode):
    """
    Analyses geographic or organisational distribution of nodes
    :param nodes: dictionary mapping each ledger to nodes information
    :param layer: the layer to analyse
    :param mode: Grouping mode: 'Countries' or 'Organisations'
    """
    logging.info(f'parse.py: Analyzing {layer} {mode}')
    geodata = get_geodata(layer, nodes, mode)
    logging.info(f'parse.py: {layer} - Total nodes: {sum([len(val) for val in geodata.values()])}')
    geodata_counter = {}

    for key, val in sorted(geodata.items(), key=lambda x: len(x[1]), reverse=True):
        if key:
            geodata_counter[key] = len(val)
        else: # if the API used for the IP addresses doesn't return any value for the country or the organisation
            geodata_counter["Unknown"] = geodata_counter.get("Unknown", 0) + len(val)

    filename = Path(f'./output/{mode.lower()}_{layer}.csv')

    if filename.is_file():
        df = pd.read_csv(filename)
        geodata_csv = df[f'{mode}'].tolist()
        geodata_in_order = [0] * len(geodata_csv)
        for geodata in geodata_counter.keys():
            if geodata in geodata_csv:
                geodata_in_order[geodata_csv.index(geodata)] = geodata_counter[geodata]
            else:
                rows, columns = df.shape
                df.loc[rows] = [geodata] + [0]*(columns-1)
                geodata_in_order.append(geodata_counter[geodata])
        df[datetime.today().strftime('%Y-%m-%d')] = geodata_in_order
        df.to_csv(f'./output/{mode.lower()}_{layer}.csv', index = False)
    else:
        geodata_df = pd.DataFrame.from_dict(geodata_counter, orient='index', columns=[datetime.today().strftime('%Y-%m-%d')])
        geodata_df.to_csv(f'./output/{mode.lower()}_{layer}.csv', index_label = mode)

def cluster_organizations(layer):
    """
    Clusters organizations in CSV files.
    :param layer: the layer to analyse
    """
    cluster_totals = defaultdict(int)
    header = None
    output_dir = hlp.get_output_directory()


    # Read and process input
    with open(output_dir / f'organizations_{layer}.csv', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Read the original header

        for row in reader:
            # Remove surrounding quotes (if any) and extra whitespace
            count = int(row[1])
            name = row[0].replace('"','').replace(',', '')

            # Special rule for specific entries
            if 'hetzner' in name.lower():
                first_word = 'Hetzner'
            elif 'netcup' in name.lower(): 
                first_word = 'netcup'
            elif 'telus-fibre' in name.lower(): 
                first_word = 'TELUS-FIBRE'
            elif 'alicloud' in name.lower(): 
                first_word = 'ALICLOUD'
            elif 'ovh' in name.lower(): 
                first_word = 'OVH'
            else:
                first_word = name.split()[0]

            cluster_totals[first_word] += count

    # Sort by count descending
    sorted_clusters = sorted(cluster_totals.items(), key=lambda x: x[1], reverse=True)

    # Write output manually (to avoid any quotes)
    with open(output_dir / f'organizations_{layer}.csv', mode='w', newline='', encoding='utf-8') as outfile:
        outfile.write(f"{header[0]},{header[1]}\n")
        for org, total in sorted_clusters:
            outfile.write(f"{org},{total}\n")

    print(f"parse.py: Cleaned and sorted CSV written to: output_dir / organizations_{layer}.csv")


LAYERS = hlp.get_layers()
MODES = hlp.get_mode()

def main():
    logging.info('Start parsing')

    for layer in LAYERS:
        logging.info(f'parse.py: Getting {layer} nodes')
        nodes = hlp.get_nodes(layer)
        for mode in MODES:
            geography(nodes, layer, mode)
        if 'Organizations' in MODES:
            cluster_organizations(layer)

if __name__ == '__main__':
    main()
