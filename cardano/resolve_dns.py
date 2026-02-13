"""
Resolves DNS names from blockfrost_pools_relays.json.
Tries default DNS first, then falls back to Google DNS (8.8.8.8) and Cloudflare DNS (1.1.1.1).
"""
import json
import logging
import socket
from pathlib import Path
import dns.resolver
from tqdm import tqdm

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

DEFAULT_PORT = 3001


def resolve_dns(hostname, dns_server=None):
    """
    Resolve DNS hostname to IP address.
    
    Args:
        hostname: DNS name to resolve
        dns_server: Optional DNS server IP to use (e.g., '8.8.8.8')
    
    Returns:
        IP address string or None if resolution fails
    """
    try:
        if dns_server:
            # Use specific DNS server
            resolver = dns.resolver.Resolver()
            resolver.nameservers = [dns_server]
            answers = resolver.resolve(hostname, 'A')
            if answers:
                return str(answers[0])
        else:
            # Use default system DNS
            return socket.gethostbyname(hostname)
    except (socket.gaierror, dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, 
            dns.resolver.NoNameservers, dns.exception.Timeout, Exception):
        return None


def load_existing_dns_db(output_path):
    if output_path.exists():
        with open(output_path, 'r') as f:
            try:
                return json.load(f)
            except Exception:
                return []
    return []


def save_dns_db(output_path, db):
    with open(output_path, 'w') as f:
        json.dump(db, f, indent=2)


def resolve_unresolved_entries():
    """Attempt to resolve unresolved DNS entries."""
    relays_file = Path(__file__).parent / 'blockfrost_pools_relays.json'
    output_path = Path(__file__).parent / 'output' / 'dns_resolved.json'
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Load DNS DB
    dns_db = load_existing_dns_db(output_path)
    if not relays_file.exists():
        logging.error(f'Blockfrost relays file not found: {relays_file}')
        return
    with open(relays_file, 'r') as f:
        pool_relays = json.load(f)
    # Build a set for fast lookup: (dns_name, port)
    existing_keys = set((entry['dns_name'], entry.get('port', DEFAULT_PORT)) for entry in dns_db)
    # Collect all unique DNS entries from all pools
    seen_dns = set()
    unresolved_entries = []
    for pool_id, relays in pool_relays.items():
        for relay in relays:
            dns_name = relay.get('dns')
            port = relay.get('port', DEFAULT_PORT)
            if dns_name:
                key = (dns_name, port)
                if key not in seen_dns and key not in existing_keys:
                    unresolved_entries.append({'dns_name': dns_name, 'pool_id': pool_id, 'port': port})
                    seen_dns.add(key)

    if not unresolved_entries:
        logging.info('No new DNS entries to resolve!')
        return
    logging.info(f'Attempting to resolve {len(unresolved_entries)} new DNS names from blockfrost_pools_relays.json...')
    
    # Track results
    newly_resolved = []
    still_unresolved = []
    
    dns_servers = [
        (None, 'default'),
        ('8.8.8.8', 'Google DNS'),
        ('1.1.1.1', 'Cloudflare DNS'),
    ]
    
    for entry in tqdm(unresolved_entries, desc='Resolving DNS', unit='dns'):
        dns_name = entry['dns_name']
        pool_id = entry['pool_id']
        port = entry['port']
        resolved_ip = None
        resolver_used = None
        for dns_server, server_name in dns_servers:
            resolved_ip = resolve_dns(dns_name, dns_server)
            if resolved_ip:
                resolver_used = server_name
                break
        if resolved_ip:
            new_entry = {
                'dns_name': dns_name,
                'ip_address': resolved_ip,
                'pool_id': pool_id,
                'port': port,
                'resolver': resolver_used
            }
            newly_resolved.append(new_entry)
            dns_db.append(new_entry)
        else:
            unresolved_entry = {
                'dns_name': dns_name,
                'ip_address': 'Unresolved',
                'pool_id': pool_id,
                'port': port,
                'resolver': None
            }
            still_unresolved.append(entry)
            dns_db.append(unresolved_entry)
    
    # Update data
    logging.info(f'\nResolution complete!')
    logging.info(f'Newly resolved: {len(newly_resolved)}')
    logging.info(f'Still unresolved: {len(still_unresolved)}')
    
    if newly_resolved:
        save_dns_db(output_path, dns_db)
        logging.info(f'\nAppended newly resolved IPs to {output_path}')
        # Show breakdown by DNS server
        resolver_counts = {}
        for resolved in newly_resolved:
            resolver = resolved['resolver']
            resolver_counts[resolver] = resolver_counts.get(resolver, 0) + 1
        logging.info('\nResolution breakdown:')
        for resolver, count in sorted(resolver_counts.items(), key=lambda x: x[1], reverse=True):
            logging.info(f'  {resolver}: {count} IPs')
        logging.info('\n✓ You can now use dns_resolved.json for further processing!')
    else:
        logging.info('No new IPs were resolved. All DNS names remain unresolved or already present.')


def main():
    resolve_unresolved_entries()


if __name__ == '__main__':
    main()
