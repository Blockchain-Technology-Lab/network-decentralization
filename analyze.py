import json
import pathlib
import network_decentralization.helper as hlp
from collections import defaultdict
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

LEDGERS = ['litecoin', 'dogecoin', 'zcash', 'bitcoin']
# LEDGERS = ['bitcoin']


def ip_type(reachable=False):
    for ledger in LEDGERS:
        ipv4, ipv6, onion = set(), set(), set()
        if reachable:
            nodelist = hlp.get_reachable_nodes(ledger)
        else:
            nodelist = hlp.get_all_nodes(ledger)
        for node in nodelist:
            node_ip = node[0]
            if ':' in node_ip:
                ipv6.add(node_ip)
            elif 'onion' in node_ip:
                onion.add(node_ip)
            else:
                ipv4.add(node_ip)

        logging.info(f'{ledger} ipv4: {len(ipv4)} ipv6: {len(ipv6)} onion: {len(onion)}')


def convergence():
    CONVERGENCE_PARAM = 0.1

    for ledger in LEDGERS:
        output_dir = hlp.get_output_directory(ledger)

        convergence = defaultdict(list)
        for filename in pathlib.Path(output_dir).iterdir():
            node_ip = str(filename).split('/')[-1]

            received_addrs = set()
            converged = False
            if filename.is_file():
                with open(filename) as f:
                    entries = json.load(f)
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
        for key, val in sorted(convergence.items(), key=lambda x: len(x[1]), reverse=True):
            logging.info(f'\t {key} {len(val)}')


def geography():
    output_dir = hlp.get_output_directory() / 'geodata'
    for ledger in LEDGERS:
        countries = defaultdict(list)
        with open(output_dir / f'{ledger}.json') as f:
            geodata = json.load(f)
            for ip_addr, ip_info in geodata.items():
                countries[ip_info['country_name']].append(ip_addr)
        logging.info(ledger)
        for key, val in sorted(countries.items(), key=lambda x: len(x[1]), reverse=True):
            logging.info(f'\t {key} {len(val)}')


def network():
    output_dir = hlp.get_output_directory() / 'geodata'
    for ledger in LEDGERS:
        countries = defaultdict(list)
        with open(output_dir / f'{ledger}.json') as f:
            geodata = json.load(f)
            for ip_addr, ip_info in geodata.items():
                countries[f"{ip_info['asn']} ({ip_info['org']})"].append(ip_addr)
        logging.info(ledger)
        for key, val in sorted(countries.items(), key=lambda x: len(x[1]), reverse=True):
            logging.info(f'\t {key} {len(val)}')


logging.info('start')

# convergence()
# geography()
# network()
ip_type(False)
