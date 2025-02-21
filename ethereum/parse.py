import json
import re
import csv
import pathlib
import helper as hlp
from collections import defaultdict
import logging
import time

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

def get_geodata(nodes, mode):
    output_dir = hlp.get_output_directory()
    countries = defaultdict(list)

    with open(output_dir / f'geodata.json') as f:
        geodata = json.load(f)

    for node in nodes:
        ip_addr = node[0]
        if ip_addr in geodata:
            ip_info = geodata[ip_addr]
            if 'error' in ip_info and ip_info['error']:
                continue
            if mode == 1:
                try:
                    countries[ip_info['country_name']].append(ip_addr)
                except KeyError:
                    countries[ip_info['country']].append(ip_addr) # The API used to geolocate IP addresses has been changed, so the fields no longer have the same name.
            elif mode == 2:
                 try:
                     countries[f"{ip_info['asn']}"].append(ip_addr)
                 except KeyError:
                     asn = (ip_info['as'].split())[0]
                     countries[f"{asn}"].append(ip_addr) # The API used to geolocate IP addresses has been changed, so the fields no longer have the same name.
            elif mode == 3:
                 countries[f"{ip_info['org']}"].append(ip_addr)
        elif ip_addr.endswith('onion'):
            countries['Tor'].append(ip_addr)
        else:
            countries['Unknown'].append(ip_addr)

    return countries

def geography(nodes, layer):
    logging.info(f'Analyzing geography')
    countries = get_geodata(nodes, 1)
    logging.info(f'{layer} - Total nodes: {sum([len(val) for val in countries.values()])}')
    with open(f'./results/geography_{layer}.csv', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(['country', 'node_count'])
        for key, val in sorted(countries.items(), key=lambda x: len(x[1]), reverse=True):
            csv_writer.writerow([key, len(val)])

def main():
    logging.info('Start parsing')

    logging.info(f'parse.py: Getting nodes')
    for layer in ('Consensus', 'Execution', 'Optimism execution'):
        nodes = hlp.get_nodes(layer)
        geography(nodes, layer)
#    network(reachable_nodes)
#    org(reachable_nodes)
#    ip_type(reachable_nodes)
#    version(reachable_nodes)

#    convergence()
#    response_length()

#    network_edges()

#        time.sleep(24*60*60)

if __name__ == '__main__':
    main()
