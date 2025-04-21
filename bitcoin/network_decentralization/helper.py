from network_decentralization.constants import DEFAULT_PORTS
import shutil
import datetime
import dns.resolver
from yaml import safe_load
import json
import pathlib
import requests
import time
import nmap3
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
with open(ROOT_DIR / "config.yaml") as f:
    config = safe_load(f)


def get_config_data():
    """
    Reads the configuration data of the project. This data is read from a file named "config.yaml" located at the
    root directory of the project.
    :returns: a dictionary of configuration keys and values
    """
    return config


def get_ledgers():
    """
    Retrieves data regarding the ledgers to use
    :returns: a list of strings that correspond to the ledgers that will be used (unless overriden by the relevant cmd
    arg)
    """
    return get_config_data()['ledgers']

def get_active():
    """
    Retrieves data regarding the packets to clean up 
    :returns: an integer that corresponds to the number of packets that will be used by cleanup_dead_nodes.py
    """
    return get_config_data()['last_time_active']

def get_concurrency():
    """
    Retrieves the concurrency parameter that defines how many processes in parallel can be executed
    :returns: integer
    """
    return get_config_data()['execution_parameters']['concurrency']


def get_output_directory(ledger=None, dead=False):
    """
    Reads the config file and retrieves the output directory
    :param ledger: optional, if set then it returns the subdirectory for the given ledger
    :returns: a directory that will contain the output files
    """
    config = get_config_data()

    output_dir = [pathlib.Path(db_dir).resolve() for db_dir in config['output_directories']][0]
    if not output_dir.is_dir():
        output_dir.mkdir()
        for subdir_type in ['osdata', 'geodata']:
            subdir = output_dir / subdir_type
            subdir.mkdir()

    if dead:
        output_dir = output_dir / "dead_nodes"
        if not output_dir.is_dir():
            output_dir.mkdir()
        output_dir = output_dir / ledger
        if not output_dir.is_dir():
            output_dir.mkdir()
        return output_dir

    if ledger:
        output_dir = output_dir / ledger
        if not output_dir.is_dir():
            output_dir.mkdir()

    return output_dir


def update_node(ledger, ip, port, version, addresses, protocol=0):
    output_dir = get_output_directory(ledger)

    try:
        with open(output_dir / ip) as f:
            entries = json.load(f)
    except FileNotFoundError:
        entries = []

    if version is None:
        status = False
        if entries == []:
            return  # If the node has never been reachable then don't record now
        version = ''
        addresses = []
    else:
        status = True

    entries.append({
        'date': datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'port': port,
        'version': version,
        'protocol': protocol,
        'status': status,
        'addresses': [list(addr) for addr in addresses],
    })

    # Write output in two steps to avoid broken files in case of abrupt interruption
    with open(output_dir / f'{ip}.backup', 'w') as f:
        json.dump(entries, f)

    shutil.move(output_dir / f'{ip}.backup', output_dir / ip)


def get_last_days(days):
    day = datetime.date.today()
    past_week = set()
    while len(past_week) < days:
        past_week.add(day.strftime('%d/%m/%Y'))
        day -= datetime.timedelta(1)
    return past_week


def get_nodes(ledger, reachable_only=False, time_window=0):
    if time_window > 0:
        dates_in_time_window = get_last_days(time_window)
    output_dir = get_output_directory(ledger)
    filenames = list(pathlib.Path(output_dir).iterdir())

    nodes = set()
    for ctr, filename in enumerate(filenames):
        print(f'{ledger} - parsed {ctr:,}/{len(filenames):,} files ({100*ctr/len(filenames):.2f}%)', end='\r')

        if filename.is_file():
            with open(filename) as f:
                entries = json.load(f)
                for entry in reversed(entries):
                    if entry['status'] and ((time_window == 0) or (time_window > 0 and entry['date'].split()[0] in dates_in_time_window)):
                        node_ip = str(filename).split('/')[-1]
                        node_port = entry['port']
                        node_version = entry['version']
                        try:
                            node_protocol = entry['protocol']
                        except KeyError:
                            node_protocol = 0
                        nodes.add((node_ip, node_port, node_version, node_protocol))
                        if reachable_only:
                            break
                        else:
                            for addr in entry['addresses']:
                                nodes.add((addr[0], addr[1], node_version, node_protocol))
    return nodes


def get_all_nodes(ledger, time_window=0):
    return get_nodes(ledger, time_window)


def get_reachable_nodes(ledger, time_window=0):
    return get_nodes(ledger, True, time_window)


def get_ipv6_nodes(ledger):
    addresses = set()
    output_dir = get_output_directory(ledger)
    for filename in pathlib.Path(output_dir).iterdir():
        if filename.is_file():
            with open(filename) as f:
                entries = json.load(f)
                for entry in entries:
                    for addr in entry['addresses']:
                        if ':' in addr[0]:
                            addresses.add((addr[0], addr[1]))
    return addresses


def get_seed_nodes(ledger):
    with open(ROOT_DIR / f'seed_info/{ledger}.json') as f:
        seeds = json.load(f)

    nodes = set()

    dns_list = seeds['dns']
    for dns_url in dns_list:
        try:
            answers = dns.resolver.resolve(dns_url)
            for rdata in answers:
                nodes.add((rdata.to_text(), DEFAULT_PORTS[ledger]))
        except Exception:
            pass

    for seed in seeds['seed_list']:
        nodes.add((seed['ip'], seed['port']))

    return nodes


def get_known_nodes(ledger, time_window=0):
    return get_all_nodes(ledger, time_window).union(get_seed_nodes(ledger))


def get_ip_geodata(ip_addr):
    data = None
    while not data:
        r = requests.get(f'http://ip-api.com/json/{ip_addr}') # Max 45 HTTP requests per minute
        try:
            data = r.json()
            if not data.get('org') and data.get('as'):
                data['org'] = data['as'][data['as'].find(' ')+1:]
            if not data.get('org') or not data.get('country'):
                new_r = requests.get(f'https://api.ipapi.is/?q={ip_addr}') # Max 1000 HTTP requests per day
                data = new_r.json()
            if 'error' in data:
                data = None
        except requests.exceptions.JSONDecodeError:
            pass
        if data is None:
            logging.error('Geodata rate limited, sleeping for 1 min...')
            time.sleep(60)
    return data


def get_os_info(ip_addr):
    nmap = nmap3.Nmap()
    os_info = nmap.nmap_os_detection(ip_addr)
    os_matches = []
    for os_match in os_info[ip_addr]['osmatch']:
        accuracy = os_match['accuracy']
        if int(accuracy) >= 90:
            os_matches.append(os_match['name'])
    return os_matches
