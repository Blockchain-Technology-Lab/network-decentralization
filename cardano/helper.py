import requests
import time
import logging
import pathlib
from yaml import safe_load

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


ROOT_DIR = pathlib.Path(__file__).resolve().parent
with open(ROOT_DIR / "config.yaml") as f:
    config = safe_load(f)


def get_config_data():
    """
    Reads Cardano configuration data from config.yaml.
    :returns: dictionary of configuration keys and values
    """
    return config


def get_mode():
    """
    Retrieves selected parsing/plotting modes.
    :returns: list of mode strings (e.g., Countries, Organizations, ASN)
    """
    return get_config_data().get('mode', ['Countries', 'Organizations', 'ASN'])


def get_metrics_network():
    """
    Retrieves the list of metrics to compute for network analysis (organizations).
    Supports either a list (e.g., ['hhi', 'nakamoto_coefficient']) or a dictionary
    (e.g., {'concentration_ratio': [1, 3]}), which is expanded to tokens like
    'concentration_ratio=1' and 'concentration_ratio=3'.
    :returns: a list of metric tokens to compute
    """
    return _expand_metric_config(get_config_data().get('network_metrics'))


def get_metrics_geo():
    """
    Retrieves the list of metrics to compute for geographic analysis (countries).
    Supports either a list (e.g., ['hhi', 'nakamoto_coefficient']) or a dictionary
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


def get_output_directory():
    """
    Retrieves and creates the configured output directory.
    :returns: pathlib.Path to output directory
    """
    output_dir = pathlib.Path(get_config_data().get('output_directory', './output')).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def get_ip_geodata(ip_addr):
    """
    Retrieves geolocation and organization data for a given IP address using ip-api.com and ipapi.is. Handles rate limiting and retries until successful.
    :param ip_addr: The IP address to query
    :returns: A dictionary with geodata fields
    """
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
            logging.error('Geodata rate limited, sleeping for 2 min...')
            time.sleep(120)
    return data
