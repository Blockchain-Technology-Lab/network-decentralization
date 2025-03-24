import network_decentralization.helper as hlp
import logging
import pandas as pd
from pathlib import Path

def org_without_tor(mode, date):
    for ledger in LEDGERS:
        logging.info(f'distribution.py: Removing Tor from {ledger} {mode}')
        name = mode.lower()
        filename = Path(f'./output/{name}_{ledger}.csv')
        if filename.is_file():
            try:
                df = pd.read_csv(filename)
                tor_nodes = df[df[f'{mode}'] == 'Tor'][f'{date}'].values[0]
                total_nodes_without_tor = df[df[f'{mode}'] != 'Tor'][f'{date}'].sum()
                total_nodes = total_nodes_without_tor + tor_nodes
                df['Distribution'] = df.apply(lambda row: round((row[f'{date}'] / total_nodes_without_tor) * tor_nodes) if row[f'{mode}'] != 'Tor' else 0, axis=1)
                df['New'] = df[f'{date}'] + df['Distribution']
                df_without_tor = df[df[f'{mode}'] != 'Tor']
                df_without_tor[[f'{mode}', 'New']].to_csv(f'./output/{name}_{ledger}_without_tor.csv', index=False)
            except IndexError:
                logging.info(f'No nodes using Tor')

LEDGERS = hlp.get_ledgers()

def main():
    org_without_tor('Countries', '2025-03-24')

if __name__ == '__main__':
    main()
