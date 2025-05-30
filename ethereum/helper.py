import shutil
import datetime
from yaml import safe_load
import json
import pathlib
import requests
import time
import logging
import pandas as pd

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
with open("config.yaml") as f:
    config = safe_load(f)


def get_config_data():
    """
    Reads the configuration data of the project. This data is read from a file named "config.yaml" located at the
    root directory of the project.
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


def get_last_days(days):
    day = datetime.date.today()
    past_week = set()
    while len(past_week) < days:
        past_week.add(day.strftime('%d/%m/%Y'))
        day -= datetime.timedelta(1)
    return past_week


def get_layer(line):
    if ' eth2:' in line:
        return 'Consensus'
    elif ' eth:' in line:
        return 'Execution'
    else:
        return "Unknown"


def get_nodes(layers, time_window=0):
    if time_window > 0:
        dates_in_time_window = get_last_days(time_window)
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
#                logging.info(f'ip: {node_ip} port: {node_port}')
    return nodes

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
