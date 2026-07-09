import json
import csv
from pathlib import Path
import helper as hlp
from collections import defaultdict
import logging
import pandas as pd
from datetime import datetime


logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def normalise_client_name(client_value):
    """
    Normalise a client label to its family name.
    Examples: 'Geth/v1.15.0' -> 'Geth', 'Lighthouse:4.6.0' -> 'Lighthouse'.
    """
    if client_value is None:
        return 'Unknown'

    client = str(client_value).strip().strip("'\"")
    if not client or client.lower() in {'nan', 'none', 'unknown'}:
        return 'Unknown'

    for separator in ('|', '/', ':'):
        client = client.split(separator, 1)[0].strip()

    client = client[:1].upper() + client[1:]

    return client or 'Unknown'


def group_nodes(layer, nodes, mode):
    """
    Groups nodes by geolocation information.
    :param layer: the layer to analyse
    :param nodes: dictionary mapping ledger names to nodes information
    :param mode: Grouping mode: 'Countries', 'ASN' or 'Organisations'
    :return: dictionary grouping node IPs under corresponding geographic/organisational keys.
    """
    output_dir = hlp.get_output_directory()
    groups = defaultdict(list)

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
                    groups[ip_info['country']].append(ip_addr)
                except KeyError:
                    groups[ip_info['location']['country']].append(ip_addr) # The API used to geolocate IP addresses has been changed, so the fields no longer have the same name.
            elif mode == 'ASN':
                 try:
                     asn = (ip_info['as'].split())[0]
                     groups[f'{asn}'].append(ip_addr)
                 except KeyError:
                     asn = "AS" + ip_info['asn']['asn']
                     groups[f'{asn}'].append(ip_addr) # The API used to geolocate IP addresses has been changed, so the fields no longer have the same name.
            elif mode == 'Organizations':
                 try:
                     groups[f"{ip_info['org']}"].append(ip_addr)
                 except KeyError:
                     groups[ip_info['asn']['org']].append(ip_addr)
        elif ip_addr.endswith('onion'):
            groups['Tor'].append(ip_addr)
        else:
            groups['Unknown'].append(ip_addr)

    return groups


def analyse_distribution(nodes, layer, mode):
    """
    Analyses geographic or organisational distribution of nodes
    :param nodes: dictionary mapping each ledger to nodes information
    :param layer: the layer to analyse
    :param mode: Grouping mode: 'Countries' or 'Organisations'
    """
    logging.info(f'parse.py: Analyzing {layer} {mode}')

    geodata_counter = {}
    output_dir = hlp.get_output_directory()
    if mode == 'Clients': # Client distribution is derived from agents.csv
        peerfile = output_dir / 'peerstore.csv'
        agentsfile = output_dir / 'agents.csv'
        if peerfile.is_file():
            peer_df = pd.read_csv(
                peerfile,
                engine='python',
                on_bad_lines='skip',
                quotechar="'",
                dtype=str,
                keep_default_na=False,
            )

            if 'node_id' not in peer_df.columns:
                logging.warning(f'parse.py: peerstore.csv has no node_id column; client counts will be empty')
                peer_df = pd.DataFrame(columns=['node_id'])

            # Filter to the requested layer using the same rule as helper.get_layer().
            if ' enr' in peer_df.columns:
                enr_source = peer_df[' enr'].fillna('')
            elif 'enr' in peer_df.columns:
                enr_source = peer_df['enr'].fillna('')
            else:
                enr_source = pd.Series([''] * len(peer_df), index=peer_df.index)

            layer_mask = enr_source.apply(hlp.get_layer).eq(layer)
            peer_subset = peer_df[layer_mask].copy()

            if peer_subset.empty:
                geodata_counter = {'Unknown': 0}
            else:
                if agentsfile.is_file():
                    agents_df = pd.read_csv(
                        agentsfile,
                        engine='python',
                        on_bad_lines='skip',
                        quotechar="'",
                        dtype=str,
                        keep_default_na=False,
                    )
                else:
                    agents_df = pd.DataFrame(columns=['node_id', 'agent_version'])

                if 'node_id' not in agents_df.columns:
                    agents_df['node_id'] = ''
                if 'agent_version' not in agents_df.columns:
                    agents_df['agent_version'] = ''

                agent_lookup = (
                    agents_df[['node_id', 'agent_version']]
                    .dropna(subset=['node_id'])
                    .drop_duplicates('node_id')
                    .set_index('node_id')['agent_version']
                    .to_dict()
                )

                resolved_clients = (
                    peer_subset['node_id']
                    .fillna('')
                    .map(lambda node_id: normalise_client_name(agent_lookup.get(node_id, 'Unknown')))
                )
                geodata_counter = resolved_clients.value_counts().to_dict()
                if 'Unknown' not in geodata_counter:
                    geodata_counter['Unknown'] = 0
        else:
            logging.warning(f'parse.py: peerstore.csv not found in {output_dir}; client counts will be empty')
    else:
        geodata = group_nodes(layer, nodes, mode)
        logging.info(f'parse.py: {layer} - Total nodes: {sum([len(val) for val in geodata.values()])}')

        for key, val in sorted(geodata.items(), key=lambda x: len(x[1]), reverse=True):
            if key:
                geodata_counter[key] = len(val)
            else: # if the API used for the IP addresses doesn't return any value for the country or the organisation
                geodata_counter["Unknown"] = geodata_counter.get("Unknown", 0) + len(val)

    output_dir = output_dir if 'output_dir' in locals() else hlp.get_output_directory()
    filename = output_dir / f'{mode.lower()}_{layer}.csv'

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
        df.to_csv(filename, index = False)
    else:
        geodata_df = pd.DataFrame.from_dict(geodata_counter, orient='index', columns=[datetime.today().strftime('%Y-%m-%d')])
        geodata_df.to_csv(filename, index_label = mode)

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
            if len(row) < 2:
                continue

            # Remove surrounding quotes (if any) and extra whitespace
            try:
                count = int(row[1])
            except (TypeError, ValueError):
                continue

            name = row[0].replace('"','').replace(',', '')
            if not name.strip():
                continue

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
                parts = name.split()
                if not parts:
                    continue
                first_word = parts[0]

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
            analyse_distribution(nodes, layer, mode)
        if 'Organizations' in MODES:
            cluster_organizations(layer)

if __name__ == '__main__':
    main()
