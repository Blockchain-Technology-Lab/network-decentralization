import json
import re
import csv
import pathlib
import network_decentralization.helper as hlp
from collections import defaultdict
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

LEDGERS = ['bitcoin', 'dogecoin', 'litecoin', 'zcash']


def ip_type(reachable_nodes):
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


def convergence():
    CONVERGENCE_PARAM = 0.1

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
                with open(filename) as f:
                    entries = json.load(f)
                    for entry_idx, entry in enumerate(entries):
                        if entry['addresses']:
                            new_addrs = set()
                            for addr in entry['addresses']:
                                if addr[0] not in received_addrs:
                                    new_addrs.add(addr[0])
                                    received_addrs.add(addr[0])
                            # if node_ip == 'ofcpvq2qmvn3itcx4ljqpghxendxqlk7b52mpv7ughbkmr5773nmrnad.onion':
                            #     print(entry['date'], len(entry['addresses']), len(new_addrs), len(received_addrs))
                            if 100*len(new_addrs) / len(entry['addresses']) < CONVERGENCE_PARAM:
                                convergence[entry_idx].append(node_ip)
                                converged = True
                                break
            if received_addrs and not converged:
                convergence[-1].append(node_ip)

        logging.info(ledger)
        for key, val in sorted(convergence.items(), key=lambda x: len(x[1]), reverse=True):
            logging.info(f'\t {key} {len(val)}')


def get_geodata(ledger, reachable_nodes, geography=True):
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
            if geography:
                countries[ip_info['country_name']].append(ip_addr)
            else:
                countries[f"{ip_info['asn']} ({ip_info['org']})"].append(ip_addr)
        elif ip_addr.endswith('onion'):
            countries['Tor'].append(ip_addr)
        else:
            countries['Unknown'].append(ip_addr)

    return countries


def geography(reachable_nodes):
    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} geography')
        countries = get_geodata(ledger, reachable_nodes)
        logging.info(f'{ledger} - Total nodes: {sum([len(val) for val in countries.values()])}')
        with open(f'./output/geography_{ledger}.csv', 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(['country', 'node_count'])
            for key, val in sorted(countries.items(), key=lambda x: len(x[1]), reverse=True):
                csv_writer.writerow([key, len(val)])


def network(reachable_nodes):
    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} network')
        countries = get_geodata(ledger, reachable_nodes, False)
        logging.info(f'{ledger} - Total nodes: {sum([len(val) for val in countries.values()])}')
        with open(f'./output/asn_{ledger}.csv', 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(['asn', 'node_count'])
            for key, val in sorted(countries.items(), key=lambda x: len(x[1]), reverse=True):
                csv_writer.writerow([key, len(val)])


def version(reachable_nodes):
    for ledger in LEDGERS:
        logging.info(f'Analyzing {ledger} versions')
        versions = defaultdict(int)
        for node in reachable_nodes[ledger]:
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
        with open(f'./output/version_{ledger}.csv', 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(['version', 'node_count'])
            for key, val in sorted(versions.items(), key=lambda x: x[1], reverse=True):
                csv_writer.writerow([key, val])


logging.info('Start parsing')

reachable_nodes = {}
for ledger in LEDGERS:
    logging.info(f'Getting {ledger} reachable nodes')
    reachable_nodes[ledger] = hlp.get_reachable_nodes(ledger)

# convergence()
geography(reachable_nodes)
network(reachable_nodes)
ip_type(reachable_nodes)
version(reachable_nodes)
