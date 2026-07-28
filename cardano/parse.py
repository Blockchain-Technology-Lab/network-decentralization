"""
Parse Cardano geodata and create CSV files for plotting.

This script categorises relay nodes into three dimensions:
1. Countries: Geographic location based on IP geolocation data
2. Organizations: Cluster organization names using keyword matching (e.g., 'hetzner', 'digitalocean')
   with fallback to the first word of the organization name
3. ASN: Autonomous System Numbers extracted from IP data

Nodes are handled as follows:
- Onion addresses (.onion): categorised as 'Tor'
- Unresolved DNS names: categorised as 'Unknown' (addresses that couldn't be resolved to IPs)
- Missing geodata: categorised as 'Unknown' (IPs with no geolocation data available)
- Geodata errors: categorised as 'Unknown' (error entries in the geodata file)
- Valid entries: categorised by their country, organization, or ASN

Output: Creates three CSV files with historical data (countries_cardano.csv, organizations_cardano.csv, asn_cardano.csv)
"""
import json
import logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import pandas as pd
import helper as hlp

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def load_cardano_nodes():
    """Load Cardano nodes from blockfrost_pools_relays.json."""
    relays_file = Path(__file__).parent / 'blockfrost_pools_relays.json'
    dns_resolved_file = Path(__file__).parent / 'output' / 'dns_resolved.json'
    nodes = []
    # Build DNS to IP mapping
    dns_to_ip = {}
    if dns_resolved_file.exists():
        with open(dns_resolved_file, 'r') as f:
            for entry in json.load(f):
                dns_name = entry.get('dns_name')
                ip_address = entry.get('ip_address')
                port = entry.get('port', 3001)
                if dns_name and ip_address and ip_address != 'Unresolved':
                    dns_to_ip[(dns_name, port)] = ip_address

    with open(relays_file, 'r') as f:
        pool_relays = json.load(f)

    for _, relays in pool_relays.items():
        for relay in relays:
            ip = relay.get('ipv4') or relay.get('ipv6')
            port = relay.get('port', 3001)
            if ip:
                nodes.append((ip, port))
            elif relay.get('dns'):
                dns_name = relay['dns']
                resolved_ip = dns_to_ip.get((dns_name, port))
                if resolved_ip:
                    nodes.append((resolved_ip, port))
                else:
                    # Keep unresolved DNS names so they can later be categorised as Unknown.
                    nodes.append((dns_name, port))
    return nodes


def cluster_org_name(name):
    """Apply clustering rules to organization names."""
    if not name or name == 'Unknown':
        return 'Unknown'
    
    # Remove trailing punctuation (commas, periods, etc.)
    name = name.rstrip('.,;:')
    
    # Provider mapping for clustering
    provider_map = {
        'hetzner': 'Hetzner',
        'netcup': 'netcup',
        'telus-fibre': 'TELUS-FIBRE',
        'alicloud': 'ALICLOUD',
        'ovh': 'OVH',
        'contabo': 'Contabo',
        'digitalocean': 'DigitalOcean',
        'google': 'Google',
        'amazon': 'Amazon',
        'aws': 'Amazon',
    }
    
    name_lower = name.lower()
    for keyword, canonical_name in provider_map.items():
        if keyword in name_lower:
            return canonical_name
    
    # Default: use first word, remove trailing punctuation, and capitalise
    first_word = name.split()[0] if name.split() else name
    first_word = first_word.rstrip('.,;:')
    # Capitalise first letter, keep rest as-is to preserve acronyms like LLC, GmbH
    return first_word[0].upper() + first_word[1:] if first_word else 'Unknown'


def get_geodata(mode='Countries'):
    """Extract geographic data from geodata JSON."""
    ledger = 'cardano'
    output_dir = hlp.get_output_directory()
    geodata_file = output_dir / 'geodata' / f'{ledger}.json'

    if not geodata_file.exists():
        logging.error(f'Geodata file not found: {geodata_file}')
        logging.info('Please run collect_cardano_geodata.py first!')
        return {}

    with open(geodata_file) as f:
        geodata = json.load(f)

    # Deduplicate nodes by (ip/dns, port)
    nodes = list(set(load_cardano_nodes()))
    categories = defaultdict(list)

    # Load unresolved DNS names
    dns_resolved_file = Path(__file__).parent / 'output' / 'dns_resolved.json'
    unresolved_dns = set()
    if dns_resolved_file.exists():
        with open(dns_resolved_file, 'r') as f:
            for entry in json.load(f):
                if entry.get('ip_address') == 'Unresolved':
                    unresolved_dns.add((entry.get('dns_name'), entry.get('port', 3001)))

    for ip_addr, port in nodes:

        # DNS names that could not be resolved are treated as Unknown
        if (ip_addr, port) in unresolved_dns:
            categories['Unknown'].append(ip_addr)
            continue

        if ip_addr.endswith('.onion'):
            categories['Tor'].append(ip_addr)
            continue

        if ip_addr not in geodata:
            categories['Unknown'].append(ip_addr)
            continue

        ip_info = geodata[ip_addr]

        # Error entries
        if ip_info.get('error'):
            categories['Unknown'].append(ip_addr)
            continue

        if mode == 'Countries':
            country = (
                ip_info.get('country')
                or ip_info.get('location', {}).get('country')
            )

            categories[country if country else 'Unknown'].append(ip_addr)

        elif mode == 'ASN':
            asn = None

            if ip_info.get('as'):
                asn = ip_info['as'].split(None, 1)[0]

            elif ip_info.get('asn'):
                asn_num = ip_info['asn'].get('asn')
                if asn_num:
                    asn = f'AS{asn_num}'

            categories[asn if asn else 'Unknown'].append(ip_addr)

        elif mode == 'Organizations':
            org_name = (
                ip_info.get('org')
                or ip_info.get('asn', {}).get('org')
            )

            if not org_name and ip_info.get('as'):
                parts = ip_info['as'].split(None, 1)
                if len(parts) > 1:
                    org_name = parts[1]

            if org_name:
                categories[cluster_org_name(org_name)].append(ip_addr)
            else:
                categories['Unknown'].append(ip_addr)

    return categories


def parse_geography(mode='Countries'):
    """Parse geography data and save to CSV."""
    ledger = 'cardano'
    logging.info(f'Parsing {ledger} {mode}')

    geodata = get_geodata(mode)

    if not geodata:
        logging.error('No geodata available!')
        return

    total_nodes = sum(len(val) for val in geodata.values())
    logging.info(f'{ledger} - Total nodes: {total_nodes}')

    unknown_count = len(geodata.get('Unknown', []))
    if unknown_count > 0:
        logging.info(f'{ledger} - Unknown entries: {unknown_count}')

    # Count by category
    geodata_counter = {}
    for key, val in sorted(geodata.items(), key=lambda x: len(x[1]), reverse=True):
        if key:
            geodata_counter[key] = len(val)
        else:
            geodata_counter['Unknown'] = geodata_counter.get('Unknown', 0) + len(val)

    output_dir = hlp.get_output_directory()
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = output_dir / f'{mode.lower()}_{ledger}.csv'

    # Create or update CSV with timestamp
    if filename.is_file():
        df = pd.read_csv(filename)
        geodata_csv = df[mode].tolist()
        geodata_in_order = [0] * len(geodata_csv)

        for category, count in geodata_counter.items():
            if category in geodata_csv:
                geodata_in_order[geodata_csv.index(category)] = count
            else:
                rows, columns = df.shape
                df.loc[rows] = [category] + [0] * (columns - 1)
                geodata_in_order.append(count)

        df[datetime.today().strftime('%Y-%m-%d')] = geodata_in_order

        # Sort by the latest date column in descending order
        latest_date_col = df.columns[-1]
        df = df.sort_values(by=latest_date_col, ascending=False)
        df.to_csv(filename, index=False)
    else:
        geodata_df = pd.DataFrame.from_dict(
            geodata_counter,
            orient='index',
            columns=[datetime.today().strftime('%Y-%m-%d')]
        )
        geodata_df.to_csv(filename, index_label=mode)

    logging.info(f'Saved to {filename}')

    # Print top 10
    logging.info(f'\nTop 10 {mode}:')
    for idx, (key, count) in enumerate(
        sorted(geodata_counter.items(), key=lambda x: x[1], reverse=True)[:10], 1
    ):
        logging.info(
            f'  {idx}. {key}: {count} nodes ({100 * count / total_nodes:.1f}%)'
        )


def main():
    modes = hlp.get_mode()
    
    for mode in modes:
        try:
            parse_geography(mode)
        except Exception as e:
            logging.error(f'Error parsing {mode}: {e}')
    
    logging.info('\nParsing complete! Ready for plotting.')


if __name__ == '__main__':
    main()
