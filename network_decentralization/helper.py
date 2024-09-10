from network_decentralization.constants import DEFAULT_PORTS
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


def get_concurrency():
    """
    Retrieves the concurrency parameter that defines how many processes in parallel can be executed
    :returns: integer
    """
    return get_config_data()['execution_parameters']['concurrency']


def get_output_directory(ledger=None):
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

    if ledger:
        output_dir = output_dir / ledger
        if not output_dir.is_dir():
            output_dir.mkdir()

    return output_dir


def update_node(ledger, ip, port, version, addresses):
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
        'status': status,
        'addresses': [list(addr) for addr in addresses],
    })

    with open(output_dir / ip, 'w') as f:
        json.dump(entries, f, indent=4)


def get_past_week():
    day = datetime.date.today()
    past_week = set()
    while len(past_week) < 7:
        past_week.add(day.strftime('%d/%m/%Y'))
        day -= datetime.timedelta(1)
    return past_week


def get_nodes(ledger, reachable_only=False):
    past_week = get_past_week()
    output_dir = get_output_directory(ledger)
    filenames = list(pathlib.Path(output_dir).iterdir())

    nodes = set()
    for ctr, filename in enumerate(filenames):
        print(f'{ledger} - parsed {ctr:,}/{len(filenames):,} files ({100*ctr/len(filenames):.2f}%)', end='\r')

        if filename.is_file():
            with open(filename) as f:
                entries = json.load(f)
                for entry in entries:
                    if entry['date'].split()[0] in past_week and entry['status']:
                        node_ip = str(filename).split('/')[-1]
                        node_port = entry['port']
                        node_version = entry['version']
                        nodes.add((node_ip, node_port, node_version))
                        if reachable_only:
                            break
                        else:
                            for addr in entry['addresses']:
                                nodes.add((addr[0], addr[1], node_version))
    return nodes


def get_all_nodes(ledger):
    return get_nodes(ledger)


def get_reachable_nodes(ledger):
    return get_nodes(ledger, True)


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


def get_known_nodes(ledger):
    return get_all_nodes(ledger).union(get_seed_nodes(ledger))


def get_ip_geodata(ip_addr):
    data = None
    while not data:
        r = requests.get(f'https://ipapi.co/{ip_addr}/json/')
        try:
            data = r.json()
            if 'error' in data and data['reason'] == 'RateLimited':
                data = None
        except requests.exceptions.JSONDecodeError:
            pass
        if data is None:
            logging.error('Geodata rate limited, sleeping for 10 mins...')
            time.sleep(10*60)
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
