import helper as hlp
import json
import time
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def collect_geodata(layers):
    """
    Retrieves the geolocation of the nodes.
    :param layers: the layers of the nodes
    """
    logging.info(f'Collecting geodata')
    filename = hlp.get_output_directory() / f'geodata.json'
    try:
        with open(filename) as f:
            geodata = json.load(f)
    except FileNotFoundError:
        logging.info(f'FileNotFoundError: {filename}')
        geodata = {}
    except json.decoder.JSONDecodeError:
        logging.info(f'JSONDecodeError: {filename}')
        geodata = {}

    nodes = hlp.get_nodes(layers)
    logging.info(f'Got {len(nodes)} nodes')
    for node in nodes:
        node_ip = node[0]
        if node_ip not in geodata.keys() and not node_ip.endswith('onion'):
            geodata[node_ip] = hlp.get_ip_geodata(node_ip)
            with open(filename, 'w') as f:
                json.dump(geodata, f, indent=4)

            logging.debug(f'Collected geodata for {node_ip}')
            time.sleep(5)  # Sleep to avoid getting rate limited
