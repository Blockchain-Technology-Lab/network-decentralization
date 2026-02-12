"""
Collect geodata for Cardano relay nodes.
"""
import json
import os
import time
import logging
from pathlib import Path
import helper as hlp

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def load_cardano_nodes():
    """Load extracted Cardano nodes from JSON."""
    nodes_file = Path(__file__).parent / 'output' / 'cardano_extracted_nodes.json'
    
    if not nodes_file.exists():
        logging.error(f'Cardano nodes file not found: {nodes_file}')
        logging.info('Please run extract_ips.py first!')
        return []
    
    with open(nodes_file, 'r') as f:
        data = json.load(f)
    
    logging.info(f"Loaded {data['total_count']} Cardano nodes from {data['source']}")
    return data['reachable_nodes']


def load_blockfrost_relays():
    """Load relays from blockfrost_pools_relays.json."""
    relays_file = Path(__file__).parent / 'blockfrost_pools_relays.json'
    if not relays_file.exists():
        logging.error(f'Blockfrost relays file not found: {relays_file}')
        return {}
    with open(relays_file, 'r') as f:
        return json.load(f)

def load_dns_resolved():
    """Load DNS resolved database as a list of dicts."""
    dns_file = Path(__file__).parent / 'output' / 'dns_resolved.json'
    if not dns_file.exists():
        logging.error(f'DNS resolved file not found: {dns_file}')
        return []
    with open(dns_file, 'r') as f:
        return json.load(f)

def collect_cardano_geodata():
    """Collect geodata for Cardano relay nodes."""
    ledger = 'cardano'
    logging.info(f'{ledger} - Collecting geodata')
    
    # Setup output paths
    output_dir = Path(__file__).parent / 'output'
    geodata_dir = output_dir / 'geodata'
    geodata_dir.mkdir(parents=True, exist_ok=True)
    
    filename = geodata_dir / f'{ledger}.json'
    
    # Load existing geodata if available
    try:
        with open(filename) as f:
            geodata = json.load(f)
        logging.info(f'Loaded existing geodata with {len(geodata)} entries')
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        geodata = {}
        logging.info('Starting fresh geodata collection')
    
    # Load relays and DNS resolved DB
    pool_relays = load_blockfrost_relays()
    dns_db = load_dns_resolved()
    # Build a lookup for (pool_id, dns_name, port) -> ip_address
    dns_lookup = {(entry['pool_id'], entry['dns_name'], entry['port']): entry['ip_address'] for entry in dns_db}
    # Build list of all relay IPs to process (unique per IP)
    relay_targets = set()
    for pool_id, relays in pool_relays.items():
        for relay in relays:
            ip = relay.get('ipv4')
            if ip:
                relay_targets.add(ip)
            else:
                dns_name = relay.get('dns')
                port = relay.get('port', 3001)
                if dns_name:
                    ip = dns_lookup.get((pool_id, dns_name, port))
                    if ip:
                        relay_targets.add(ip)
    relay_targets = list(relay_targets)  # Unique IPs only
    if not relay_targets:
        logging.error('No relay IPs to process!')
        return
    logging.info(f'{ledger} - Processing {len(relay_targets)} relay IPs')
    processed = 0
    skipped = 0
    new_entries = 0
    for node_ip in relay_targets:
        if node_ip.endswith('onion'):
            skipped += 1
            continue
        if node_ip in geodata:
            skipped += 1
            processed += 1
            if processed % 100 == 0:
                logging.info(f'Progress: {processed}/{len(relay_targets)} ({100*processed/len(relay_targets):.1f}%) - {new_entries} new, {skipped} skipped')
            continue
        if node_ip == 'Unresolved':
            # Add to 'Unresolved' category in geodata
            if 'Unresolved' not in geodata:
                geodata['Unresolved'] = []
            geodata['Unresolved'].append(node_ip)
            skipped += 1
            processed += 1
            continue
        try:
            geodata[node_ip] = hlp.get_ip_geodata(node_ip)
            new_entries += 1
            with open(filename, 'w') as f:
                json.dump(geodata, f, indent=4)
            logging.debug(f'{ledger} - Collected geodata for {node_ip}')
            processed += 1
            if processed % 10 == 0:
                logging.info(f'Progress: {processed}/{len(relay_targets)} ({100*processed/len(relay_targets):.1f}%) - {new_entries} new, {skipped} skipped')
            time.sleep(1.5)
        except Exception as e:
            logging.error(f'Error processing {node_ip}: {e}')
            processed += 1
            continue
    logging.info(f'{ledger} - Complete! Processed {processed}/{len(relay_targets)} relay IPs')
    logging.info(f'{ledger} - New entries: {new_entries}, Skipped: {skipped}')
    logging.info(f'{ledger} - Total geodata entries: {len(geodata)}')


def main():
    start_time = time.time()
    
    collect_cardano_geodata()
    
    elapsed = time.time() - start_time
    hours = int(elapsed / 3600)
    mins = int((elapsed - hours*3600) / 60)
    secs = int(elapsed - mins*60 - hours*3600)
    
    print(f'\nTotal time: {hours:02}h {mins:02}m {secs:02}s')


if __name__ == '__main__':
    main()

