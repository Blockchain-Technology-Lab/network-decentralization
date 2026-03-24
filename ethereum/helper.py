from yaml import safe_load
import pathlib
import requests
import time
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
with open("config.yaml") as f:
    config = safe_load(f)


def get_config_data():
    """
    Reads the configuration data of the project. This data is read from a file named "config.yaml" located at the root directory of the project.
    :returns: a dictionary of configuration keys and values
    """
    return config


def get_layers():
    """
    Retrieves data regarding the layers to use
    :returns: a list of strings that correspond to the layers that will be used
    """
    return get_config_data()['layers']


def get_mode():
    """
    Retrieves data regarding the mode to use
    :returns: 'Countries' or 'Organizations', or both
    """
    return get_config_data()['mode']


def get_output_directory():
    """
    Reads the config file and retrieves the output directory
    :returns: a directory that will contain the output files
    """
    config = get_config_data()

    output_dir = [pathlib.Path(db_dir).resolve() for db_dir in config['output_directories']][0]
    if not output_dir.is_dir():
        output_dir.mkdir()
        for subdir_type in ['osdata', 'geodata']:
            subdir = output_dir / subdir_type
            subdir.mkdir()

    return output_dir


def get_metrics_network():
    """
    Retrieves the list of metrics to compute for network analysis (organizations).
    Supports either a list (e.g., ['hhi', 'nakamoto']) or a dictionary
    (e.g., {'concentration_ratio': [1, 3]}), which is expanded to tokens like
    'concentration_ratio=1' and 'concentration_ratio=3'.
    :returns: a list of metric tokens to compute
    """
    return _expand_metric_config(get_config_data().get('network_metrics'))


def get_metrics_geo():
    """
    Retrieves the list of metrics to compute for geographic analysis (countries).
    Supports either a list (e.g., ['hhi', 'nakamoto']) or a dictionary
    (e.g., {'concentration_ratio': [1, 3]}), which is expanded to tokens like
    'concentration_ratio=1' and 'concentration_ratio=3'.
    :returns: a list of metric tokens to compute
    """
    return _expand_metric_config(get_config_data().get('geo_metrics'))


def _expand_metric_config(raw_metrics):
    """
    Expands metric configuration into a flat list of metric tokens.
    Example: {'entropy': [1, 2]} -> ['entropy=1', 'entropy=2']
    """
    metrics = raw_metrics

    if metrics is None:
        return []

    if isinstance(metrics, list):
        return [str(metric).strip() for metric in metrics if str(metric).strip()]

    if not isinstance(metrics, dict):
        return []

    expanded = []
    for metric_name, parameter_values in metrics.items():
        name = str(metric_name).strip()
        if not name:
            continue

        if parameter_values is None:
            expanded.append(name)
            continue

        if isinstance(parameter_values, list):
            values = parameter_values
        else:
            values = [parameter_values]

        unique_values = []
        for value in values:
            rendered = None if value is None else str(value).strip()
            if rendered is not None and rendered not in unique_values:
                unique_values.append(rendered)

        if not unique_values:
            expanded.append(name)
            continue

        for rendered in unique_values:
            expanded.append(f"{name}={rendered}")

    return expanded


def get_layer(line):
    """
    Retrieves the layer in the given line
    :param line: a line of peerstore.csv
    :returns: a layer
    """
    if 'eth2:' in line:
        return 'Consensus'
    elif 'eth:' in line:
        return 'Execution'
    else:
        return "Unknown"


def get_nodes(layers):
    """
    Retrieves nodes.
    :param layers: the layer(s) of the nodes
    :returns: a set containing information on all corresponding nodes
    """
    filename = get_output_directory() / 'peerstore.csv'
    nodes = set()

    if filename.is_file():
        with open(filename) as f:
            for line in f.readlines():
                layer_node = get_layer(line)
                if layer_node in layers:
                    values = line.rsplit(',')
                    node_ip = values[2][:values[2].rfind(":")]
                    node_port = values[2][values[2].rfind(":")+1:]
                    nodes.add((node_ip, node_port))
    return nodes


def get_ip_geodata(ip_addr):
    """
    Retrieves the node geolocation using ip-api.com or api.ipapi.is.
    :param ip_addr: the ip address of the node
    :returns: geolocation information
    """
    data = None
    while not data:
        r = requests.get(f'http://ip-api.com/json/{ip_addr}') # Max 45 HTTP requests per minute
        try:
            data = r.json()
            if not data.get('org') and data.get('as'): # if the API didn't return an organisation but returned an ASN
                data['org'] = data['as'][data['as'].find(' ')+1:] # the organisation field takes the value of the ASN field
            if not data.get('org') or not data.get('country'): # if there's still no organisation or if the API didn't return a country
                new_r = requests.get(f'https://api.ipapi.is/?q={ip_addr}') # try another API (max 1000 HTTP requests per day)
                data = new_r.json()
            if 'error' in data:
                data = None
        except requests.exceptions.JSONDecodeError:
            pass
        if data is None:
            logging.error('Geodata rate limited, sleeping for 1 min...')
            time.sleep(60)
    return data
