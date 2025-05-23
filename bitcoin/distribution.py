# This script is only used for the Bitcoin ledger. If the Bitcoin ledger is not selected in the config.yaml file, this script does nothing.

import network_decentralization.helper as hlp
import logging
import pandas as pd
from pathlib import Path

def redistribute_tor_nodes(name, ledger, df, mode):
    """
    Redistributes Tor node count proportionally across non-Tor rows.
    :param name: lowercase version of the mode ('countries' or 'organizations') used in file naming.
    :param ledger: the ledger name.
    :param df: the dataframe in which the Tor nodes must be reditributed.
    :param mode: Countries or Organizations.
    """
    tor_row = df[df[mode] == 'Tor']
    if tor_row.empty:
        logging.info(f"No Tor nodes found in {ledger}.")
        return

    number_of_tor_nodes = tor_row[date].values[0] # extract the number of Tor nodes for the given date
    number_of_total_nodes_without_tor = df[df[f'{mode}'] != 'Tor'][f'{date}'].sum() # sum of node counts excluding the Tor row
    number_of_total_nodes = number_of_total_nodes_without_tor + number_of_tor_nodes
    df['Distribution'] = df.apply(lambda row: round((row[f'{date}'] / number_of_total_nodes_without_tor) * number_of_tor_nodes) if row[f'{mode}'] != 'Tor' else 0, axis=1) # create a new column 'Distribution' that distributes the Tor nodes proportionally to non-Tor rows
    df[date] = df[date] + df['Distribution']
    df_without_tor = df[df[f'{mode}'] != 'Tor'] # filter out the Tor row
    df_without_tor[[mode, date]].to_csv(f'./output/{name}_{ledger}_without_tor.csv', index=False) # save the updated DataFrame to a new CSV


def without_tor():
    """
    Loads a CSV file and calls the redistribute_tor_nodes function.
    """
    ledger = 'bitcoin'
    for mode in MODES:
        logging.info(f'distribution.py: Removing Tor from {ledger} {mode}')
        name = mode.lower()
        filename = Path(f'./output/{name}_{ledger}.csv')
        if not filename.is_file():
            logging.warning(f"File not found: {filename}")
            return None
        df = pd.read_csv(filename)
        redistribute_tor_nodes(name, ledger, df, mode)

LEDGERS = hlp.get_ledgers()   
MODES = hlp.get_mode()
date = hlp.get_date()

def main():
    if 'bitcoin' in LEDGERS:
        without_tor()

if __name__ == '__main__':
    main()
