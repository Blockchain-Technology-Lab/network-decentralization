import network_decentralization.protocol as network_proto
from network_decentralization.constants import MAGIC_NUMBERS, PROTOCOL_VERSIONS
import network_decentralization.helper as hlp
import socket
import json
import time
from itertools import repeat
import multiprocessing
import logging
import os

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def get_node_addresses(ledger, node_ip, node_port):
    """
    Connects to the node, retrieves information about it from received packets and updates the node's file.
    :param ledger: the ledger of the node
    :param node_ip: the ip address of the node
    :param node_port: the port of the node
    """
    network_types = {
        1: 'ipv4',
        2: 'ipv6',
        3: 'onion',
        4: 'onion',
    }

    proxy = None
    if node_ip.endswith('onion'):
        proxy = ('127.0.0.1', 9050)

    version, protocol, addresses = None, None, set()
    try:
        if proxy:
            conn = network_proto.Connection((node_ip, node_port), proxy=proxy)
        else:
            conn = network_proto.Connection((node_ip, node_port))
        conn.open()
        version_msg = conn.handshake()
        version = version_msg['user_agent']
        protocol = version_msg['version']

        addr_msgs = conn.getaddr()
        conn.ping()

        if addr_msgs:
            for msg in addr_msgs[0]['addr_list']:
                network_id = msg['network_id']
                ip_type = network_types[network_id]
                addresses.add((msg[ip_type], msg['port'], msg['services'], msg['timestamp'], ip_type))

        logging.debug(f'{ledger} {node_ip}:{node_port} - Version {version}, Addresses {len(addresses)}')
    except (network_proto.ProtocolError, network_proto.ConnectionError, socket.error) as err:
        logging.debug(f'{ledger} {node_ip}:{node_port} - {err}')
    except network_proto.UnsupportedNetworkIdError as err:
        version = 'unknown'
        logging.debug(f'{ledger} {node_ip}:{node_port} - {err}')
    except network_proto.RemoteHostClosedConnection:
        logging.debug(f'{ledger} {node_ip}:{node_port} - Connection closed.')
    except network_proto.ProxyRequired:
        logging.debug(f'{ledger} {node_ip}:{node_port} - Tor node, ignoring...')
    except KeyError:
        logging.debug(f'{ledger} {node_ip}:{node_port} - Could not connect.')
    finally:
        conn.close()

    hlp.update_node(ledger, node_ip, node_port, version, addresses, protocol)


def crawl_network(ledger):
    """
    Crawls the network. Connects to nodes to collect information about them. 
    :param ledger: the ledger to crawl
    """
    network_proto.MAGIC_NUMBER = MAGIC_NUMBERS[ledger]
    network_proto.PROTOCOL_VERSION = PROTOCOL_VERSIONS[ledger]

    concurrency = hlp.get_concurrency()

    logging.info(f'Collecting {ledger} known nodes')
    known_nodes = hlp.get_known_nodes(ledger)
    logging.info(f'{len(known_nodes)} {ledger} nodes found')

    parsed_nodes = set()

    pool = multiprocessing.Pool(processes=concurrency)
    jobs = []
    for node in known_nodes:
        node_ip = node[0]
        node_port = node[1]
        if (node_ip, node_port) not in parsed_nodes:
            p = pool.apply_async(get_node_addresses, args=(ledger, node_ip, node_port))
            jobs.append(p)

            parsed_nodes.add((node_ip, node_port))
    [p.wait() for p in jobs]


def collect_geodata(ledger):
    """
    Retrieves the geolocation of the nodes.
    :param ledger: the ledger of the nodes
    """
    logging.info(f'{ledger} - Collecting geodata')
    filename = hlp.get_output_directory() / 'geodata' / f'{ledger}.json'
    try:
        with open(filename) as f:
            geodata = json.load(f)
    except FileNotFoundError:
        if not os.path.isdir(hlp.get_output_directory() / 'geodata'):
            os.mkdir(hlp.get_output_directory() / 'geodata')
        geodata = {}
    except json.decoder.JSONDecodeError:
        logging.info(f'JSONDecodeError: {filename}')
        geodata = {}

    nodes = hlp.get_reachable_nodes(ledger)
    logging.info(f'{ledger} - Got {len(nodes)} nodes')
    for node in nodes:
        node_ip = node[0]
        if node_ip not in geodata.keys() and not node_ip.endswith('onion'):
            geodata[node_ip] = hlp.get_ip_geodata(node_ip)
            with open(filename, 'w') as f:
                json.dump(geodata, f, indent=4)

            logging.debug(f'{ledger} - Collected geodata for {node_ip}')
            time.sleep(5)  # Sleep to avoid getting rate limited


def get_os_info(node, osdata, ledger, all_nodes):
    """
    (not in use)
    Retrieves the OS used by the node.
    :param node: information on the node (e.g. ip address)
    :param osdata: dictionary mapping node IP addresses to their OS information
    :param ledger: the ledger of the node
    :param all_nodes: total number of nodes
    """
    node_ip = node[0]
    computed_percentage = 100*len(osdata) / all_nodes
    if node_ip not in osdata:
        try:
            osdata[node_ip] = hlp.get_os_info(node_ip)
            logging.info(f'{ledger} - Collected {node_ip} ({computed_percentage:.2f}%)')
        except Exception as e:
            osdata[node_ip] = None
            logging.info(f'{ledger} get_os_info error: {e}')
    else:
        osdata[node_ip] = None
        logging.debug(f'{ledger} - {node_ip} already exists ({computed_percentage:.2f}%)')


def collect_osdata(ledger, timestamp=None):
    """
    (not in use)
    Retrieves the OS used by nodes.
    :param ledger: the ledger of the node
    :param timestamp: optional, timestamp string to name the output file
    """

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
        logging.info(f'FileNotFoundError: {filename}')
        geodata = {}
    except json.decoder.JSONDecodeError:
        logging.info(f'JSONDecodeError: {filename}')
        geodata = {}


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
