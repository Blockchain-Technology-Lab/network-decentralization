import network_decentralization.protocol as network_proto
from network_decentralization.constants import MAGIC_NUMBERS, PROTOCOL_VERSIONS
import network_decentralization.helper as hlp
import socket
import json
import time
from itertools import repeat
import multiprocessing
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def get_node_addresses(sema, ledger, node_ip, node_port):
    network_types = {
        1: 'ipv4',
        2: 'ipv6',
        3: 'onion',
        4: 'onion',
    }

    proxy = None
    if node_ip.endswith('onion'):
        proxy = ('127.0.0.1', 9050)

    version, addresses = None, set()
    try:
        if proxy:
            conn = network_proto.Connection((node_ip, node_port), proxy=proxy)
        else:
            conn = network_proto.Connection((node_ip, node_port))
        conn.open()
        version_msg = conn.handshake()
        addr_msgs = conn.getaddr()
        conn.ping()

        version = version_msg['user_agent']
        if addr_msgs:
            for msg in addr_msgs[0]['addr_list']:
                network_id = msg['network_id']
                ip_type = network_types[network_id]
                addresses.add((msg[ip_type], msg['port'], msg['services'], ip_type))

        logging.info(f'{node_ip}:{node_port} - Version {version}, Addresses {len(addresses)}')
    except network_proto.UnsupportedNetworkIdError as err:
        version = 'unknown'
        logging.error(f'{node_ip}:{node_port} - {err}')
    except (network_proto.ProtocolError, ConnectionError, socket.error) as err:
        logging.error(f'{node_ip}:{node_port} - {err}')
    except network_proto.RemoteHostClosedConnection:
        logging.error(f'{node_ip}:{node_port} - Connection closed.')
    except network_proto.ProxyRequired:
        logging.error(f'{node_ip}:{node_port} - Tor node, ignoring...')
    except KeyError:
        logging.error(f'{node_ip}:{node_port} - Could not connect.')
    finally:
        conn.close()

    hlp.update_node(ledger, node_ip, node_port, version, addresses)

    sema.release()  # Release the semaphore s.t. the loop in the calling function can continue


def crawl_network(ledger):
    network_proto.MAGIC_NUMBER = MAGIC_NUMBERS[ledger]
    network_proto.PROTOCOL_VERSION = PROTOCOL_VERSIONS[ledger]

    concurrency = hlp.get_concurrency()

    logging.info(f'Collecting {ledger} known nodes')
    known_nodes = hlp.get_all_nodes(ledger).union(hlp.get_seed_nodes(ledger))
    logging.info(f'{len(known_nodes)} {ledger} nodes found')

    parsed_nodes = set()

    sema = multiprocessing.Semaphore(concurrency)
    jobs = []
    for node_ip, node_port in known_nodes:
        logging.info(f'Processed {ledger} nodes: {100*len(parsed_nodes)/len(known_nodes):.2f}%')
        if (node_ip, node_port) not in parsed_nodes:
            sema.acquire()  # Loop stops here while the active processes are as many as the semaphore's limit
            p = multiprocessing.Process(target=get_node_addresses, args=(sema, ledger, node_ip, node_port))
            jobs.append(p)
            p.start()
            parsed_nodes.add((node_ip, node_port))
            if len(jobs) >= concurrency:
                for proc in jobs:
                    proc.join()
                jobs = []
    for proc in jobs:
        proc.join()


def collect_geodata(ledger):
    logging.info(f'{ledger} - Collecting geodata')
    filename = hlp.get_output_directory() / 'geodata' / f'{ledger}.json'
    try:
        with open(filename) as f:
            geodata = json.load(f)
    except FileNotFoundError:
        geodata = {}

    nodes = hlp.get_reachable_nodes(ledger)
    logging.info(f'{ledger} - Got {len(nodes)} nodes')
    for node_ip, _ in nodes:
        logging.info(f'{ledger} - Collecting geodata for {node_ip}')
        if node_ip not in geodata.keys() and not node_ip.endswith('onion'):
            geodata[node_ip] = hlp.get_ip_geodata(node_ip)

            time.sleep(5)  # Sleep to avoid getting rate limited

            with open(filename, 'w') as f:
                json.dump(geodata, f, indent=4)


def get_os_info(node, osdata, ledger, all_nodes):
    node_ip = node[0]
    computed_percentage = 100*len(osdata) / all_nodes
    if node_ip not in osdata:
        try:
            osdata[node_ip] = hlp.get_os_info(node_ip)
            logging.info(f'{ledger} - Collected {node_ip} ({computed_percentage:.2f}%)')
        except Exception as e:
            osdata[node_ip] = None
            logging.info(f'get_os_info error: {e}')
    else:
        osdata[node_ip] = None
        logging.info(f'{ledger} - {node_ip} already exists ({computed_percentage:.2f}%)')


def collect_osdata(ledger, timestamp=None):

    logging.info(f'{ledger} - Collecting osdata')

    if timestamp:
        filename = hlp.get_output_directory() / 'osdata' / f'{ledger}_{timestamp}.json'
    else:
        filename = hlp.get_output_directory() / 'osdata' / f'{ledger}.json'

    concurrency = hlp.get_concurrency()

    manager = multiprocessing.Manager()
    osdata = manager.dict()
    try:
        with open(filename) as f:
            data = json.load(f)
        for key, val in data.items():
            osdata[key] = val
    except FileNotFoundError:
        pass

    logging.info(f'{ledger} - Getting list of nodes')
    nodes = hlp.get_reachable_nodes(ledger)
    logging.info(f'{ledger} - Got {len(nodes)} nodes')

    with multiprocessing.Pool(processes=concurrency) as pool:
        pool.starmap(get_os_info, zip(nodes, repeat(osdata), repeat(ledger), repeat(len(nodes))))

    data = {}
    for key, val in osdata.items():
        data[key] = val
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)
