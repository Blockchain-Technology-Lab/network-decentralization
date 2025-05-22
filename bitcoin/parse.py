import json
import re
import csv
from pathlib import Path
import network_decentralization.helper as hlp
from collections import defaultdict
import logging
import pandas as pd
from datetime import datetime


logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def network_edges():
    """
    Parses node connection data from the past 7 days and constructs a network edge list for each ledger, saving the results as CSV files.
    """
    network_edge_dir = hlp.get_output_directory() / 'network_edges'
    if not network_edge_dir.is_dir():
        network_edge_dir.mkdir()

    past_week = hlp.get_last_days(7)

    for ledger in LEDGERS:
        reachable_nodes = set()
        output_data = [['source', 'dest']]
        edges = set()
        logging.info(f'Parsing {ledger} graph edges')
        output_dir = hlp.get_output_directory(ledger)
        filenames = list(pathlib.Path(output_dir).iterdir())
        for idx, filename in enumerate(filenames):
            print(f'{ledger} - parsed {idx:,}/{len(filenames):,} files ({100*idx/len(filenames):.2f}%)', end='\r')
            node_ip = str(filename).split('/')[-1]
            try:
                with open(filename) as f:
                    entries = json.load(f)
            except json.decoder.JSONDecodeError:
                continue
            for entry in entries:
                if entry['date'].split()[0] in past_week:
                    if entry['status']:
                        reachable_nodes.add(node_ip)
                    for addr in entry['addresses']:
                        edges.add((node_ip, addr[0]))
        for source, dest in edges:
            if dest in reachable_nodes:
                output_data.append([source, dest])

        with open(hlp.get_output_directory() / 'network_edges' / f'{ledger}.csv', 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(output_data)


def ip_type(reachable_nodes):
    """
    Classifies nodes by IP address type (IPv4, IPv6, .onion) and saves the results to a CSV file.
    :param reachable_nodes: dictionary mapping each ledger to nodes information
    """
    output_data = [['ledger', 'ipv4', 'ipv6', 'onion']]
    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} ip types')
        ipv4, ipv6, onion = set(), set(), set()
        for node in reachable_nodes[ledger]:
            node_ip = node[0]
            if ':' in node_ip:
                ipv6.add(node_ip)
            elif 'onion' in node_ip:
                onion.add(node_ip)
            else:
                ipv4.add(node_ip)

        logging.info(f'{ledger} ipv4: {len(ipv4)} ipv6: {len(ipv6)} onion: {len(onion)}')
        output_data.append([ledger, len(ipv4), len(ipv6), len(onion)])
    with open('output/ip_type.csv', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(output_data)


def response_length():
    """
    Analyses the average number of addresses returned by each node. The results are saved in a JSON file.
    """
    output = {}

    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} response lengths')
        output_dir = hlp.get_output_directory(ledger)

        response_length = defaultdict(list)
        filenames = list(pathlib.Path(output_dir).iterdir())
        for idx, filename in enumerate(filenames):
            print(f'{ledger} - parsed {idx:,}/{len(filenames):,} files ({100*idx/len(filenames):.2f}%)', end='\r')
            node_ip = str(filename).split('/')[-1]

            received_addrs = []
            if filename.is_file() and not node_ip.endswith('.swp'):
                try:
                    with open(filename) as f:
                        entries = json.load(f)
                except json.decoder.JSONDecodeError:
                    continue
                for entry_idx, entry in enumerate(entries):
                    if entry['addresses']:
                        received_addrs.append(len(entry['addresses']))

            if received_addrs:
                avg_responses = sum(received_addrs) / len(received_addrs)
                response_length[int(avg_responses)].append(node_ip)

        logging.info(ledger)
        output[ledger] = []
        for key, val in sorted(response_length.items(), key=lambda x: len(x[1]), reverse=True):
            output[ledger].append([key, len(val)])
            # logging.info(f'\t {key} {len(val)}')
    output_dir = hlp.get_output_directory()
    with open(output_dir / 'response_length.json', 'w') as f:
        json.dump(output, f, indent=4)


def convergence():
    """
    Determines convergence behaviour for each node based on the uniqueness of addresses received over time. Results are saved in a JSON file.
    """
    CONVERGENCE_PARAM = 0.1

    output = {}

    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} convergence')
        output_dir = hlp.get_output_directory(ledger)

        convergence = defaultdict(list)
        filenames = list(pathlib.Path(output_dir).iterdir())
        for idx, filename in enumerate(filenames):
            print(f'{ledger} - parsed {idx:,}/{len(filenames):,} files ({100*idx/len(filenames):.2f}%)', end='\r')
            node_ip = str(filename).split('/')[-1]

            received_addrs = set()
            converged = False
            if filename.is_file() and not node_ip.endswith('.swp'):
                try:
                    with open(filename) as f:
                        entries = json.load(f)
                except json.decoder.JSONDecodeError:
                    continue
                for entry_idx, entry in enumerate(entries):
                    if entry['addresses']:
                        new_addrs = set()
                        for addr in entry['addresses']:
                            if addr[0] not in received_addrs:
                                new_addrs.add(addr[0])
                                received_addrs.add(addr[0])
                        if 100*len(new_addrs) / len(entry['addresses']) < CONVERGENCE_PARAM:
                            convergence[entry_idx].append(node_ip)
                            converged = True
                            break
            if received_addrs and not converged:
                convergence[-1].append(node_ip)

        logging.info(ledger)
        output[ledger] = []
        for key, val in sorted(convergence.items(), key=lambda x: len(x[1]), reverse=True):
            output[ledger].append([key, len(val)])
            # logging.info(f'\t {key} {len(val)}')
    output_dir = hlp.get_output_directory()
    with open(output_dir / 'convergence.json', 'w') as f:
        json.dump(output, f, indent=4)


def get_geodata(ledger, reachable_nodes, mode):
    """
    Groups reachable nodes by geolocation information.
    :param ledger: the ledger to analyse
    :param reachable_nodes: dictionary mapping ledger names to nodes information
    :param mode: Grouping mode: 'Countries', 'ASN' or 'Organisations'
    :return: dictionary grouping node IPs under corresponding geographic/organisational keys.
    """
    output_dir = hlp.get_output_directory() / 'geodata'
    countries = defaultdict(list)

    with open(output_dir / f'{ledger}.json') as f:
        geodata = json.load(f)

    for node in reachable_nodes[ledger]:
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


def geography(reachable_nodes, ledger, mode):
    """
    Analyses geographic or organisational distribution of nodes
    :param reachable_nodes: dictionary mapping each ledger to nodes information
    :param ledger: the ledger to analyse
    :param mode: Grouping mode: 'Countries' or 'Organisations'
    """
    logging.info(f'parse.py: Analyzing {ledger} {mode}')
    geodata = get_geodata(ledger, reachable_nodes, mode)
    logging.info(f'parse.py: {ledger} - Total nodes: {sum([len(val) for val in geodata.values()])}')
    geodata_counter = {}

    for key, val in sorted(geodata.items(), key=lambda x: len(x[1]), reverse=True):
        if key:
            geodata_counter[key] = len(val)
        else: # if the API used for the IP addresses doesn't return any value for the country or the organisation
            geodata_counter["Unknown"] = geodata_counter.get("Unknown", 0) + len(val)

    filename = Path(f'./output/{mode.lower()}_{ledger}.csv')

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
        df.to_csv(f'./output/{mode.lower()}_{ledger}.csv', index = False)
    else:
        geodata_df = pd.DataFrame.from_dict(geodata_counter, orient='index', columns=[datetime.today().strftime('%Y-%m-%d')])
        geodata_df.to_csv(f'./output/{mode.lower()}_{ledger}.csv', index_label = mode)


def network(reachable_nodes):
    """
    Groups nodes by ASN and writes the counts to a CSV file.
    :param reachable_nodes: dictionary mapping each ledger to nodes information
    """
    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} network')
        countries = get_geodata(ledger, reachable_nodes, 2)
        logging.info(f'{ledger} - Total nodes: {sum([len(val) for val in countries.values()])}')
        with open(f'./output/asn_{ledger}.csv', 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(['asn', 'node_count'])
            for key, val in sorted(countries.items(), key=lambda x: len(x[1]), reverse=True):
                csv_writer.writerow([key, len(val)])


def version(reachable_nodes, mode):
    """
    Analyses and records the distribution of client or protocol versions used by nodes.
    :param reachable_nodes: dictionary mapping ledgers to nodes info.
    :param mode: 1 for client versions, 2 for protocol versions.
    """
    name = ''
    if mode == 1:
        name = 'Clients'
    if mode == 2:
        name = 'Protocols'

    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} {name}')
        versions = defaultdict(int)
        for node in reachable_nodes[ledger]:
            if mode == 1:
                version = node[2]
                if ledger == 'bitcoin':
                    expr = re.search(r'Satoshi:\d{1,2}\.\d{1,2}\.\d{1,2}', version)
                    if expr:
                        version = expr.group(0)
                elif ledger == 'litecoin':
                    expr = re.search(r'LitecoinCore:\d{1,2}\.\d{1,2}\.\d{1,2}', version)
                    if expr:
                        version = expr.group(0)
                elif ledger == 'zcash':
                    expr = re.search(r'MagicBean:\d{1,2}\.\d{1,2}\.\d{1,2}', version)
                    if expr:
                        version = expr.group(0)
                elif ledger == 'dogecoin':
                    expr = re.search(r'Shibetoshi:\d{1,2}\.\d{1,2}\.\d{1,2}', version)
                    if expr:
                        version = expr.group(0)
                versions[version] += 1
            elif mode == 2:
                version = node[3]
                versions[version] += 1

        filename = Path(f'./output/version_{ledger}.csv')

        if filename.is_file():
            df = pd.read_csv(filename)
            versions_csv = df[name].tolist()
            versions_in_order = [0] * len(versions_csv)
            logging.info(f'{versions}')
            for version in versions.keys():
                if version in versions_csv:
                    versions_in_order[versions_csv.index(version)] = versions[version]
                else:
                    rows, columns = df.shape
                    df.loc[rows] = [version] + [0]*(columns-1)
                    versions_in_order.append(versions[version])
            df[datetime.today().strftime('%Y-%m-%d')] = versions_in_order
            df.to_csv(f'./output/{name.lower()}_{ledger}.csv', index = False)
        else:
            versions_df = pd.DataFrame.from_dict(versions, orient='index', columns=[datetime.today().strftime('%Y-%m-%d')])
            versions_df.to_csv(f'./output/{name.lower()}_{ledger}.csv', index_label = name)


def cluster_organizations(ledger):
    """
    Clusters organizations in CSV files.
    :param ledger: the ledger to analyse
    """
    cluster_totals = defaultdict(int)
    header = None
    output_dir = hlp.get_output_directory()


    # Read and process input
    with open(output_dir / f'organizations_{ledger}.csv', newline='', encoding='utf-8') as infile:
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
    with open(output_dir / f'organizations_{ledger}.csv', mode='w', newline='', encoding='utf-8') as outfile:
        outfile.write(f"{header[0]},{header[1]}\n")
        for org, total in sorted_clusters:
            outfile.write(f"{org},{total}\n")

    print(f"parse.py: Cleaned and sorted CSV written to: output_dir / organizations_{ledger}.csv")


LEDGERS = hlp.get_ledgers()
MODES = hlp.get_mode()

def main():
    logging.info('Start parsing')

    reachable_nodes = {}
    for ledger in LEDGERS:
        logging.info(f'parse.py: Getting {ledger} reachable nodes')
        reachable_nodes[ledger] = hlp.get_reachable_nodes(ledger)
        for mode in MODES:
            geography(reachable_nodes, ledger, mode)
        if 'Organizations' in MODES:
            cluster_organizations(ledger)

if __name__ == '__main__':
    main()
