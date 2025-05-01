import json
import pathlib
import os
import network_decentralization.helper as hlp
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

def main():
    LEDGERS = hlp.get_ledgers()
    last_time_active = hlp.get_active()

    for ledger in LEDGERS:
        active = set()
        logging.info(f'Parsing {ledger}')
        output_dir = hlp.get_output_directory(ledger)
        filenames = list(pathlib.Path(output_dir).iterdir())
        logging.info(f'{ledger} - {len(filenames):,} total nodes')
        for idx, filename in enumerate(filenames):
            print(f'{ledger} - parsed {idx:,}/{len(filenames):,} files ({100*idx/len(filenames):.2f}%)', end='\r')
            if filename.is_file() and not str(filename).endswith('.swp'):
                with open(filename) as f:
                    entries = json.load(f)
                    len_entries = len(entries)
                    if len_entries < last_time_active:
                        for nb in range(len_entries):
                            if (entries[len_entries-nb-1])['status']:
                                active.add(filename)
                                break
                    else:
                        for nbr in range(last_time_active):
                            if (entries[len_entries-nbr-1])['status']:
                                active.add(filename)
                                break
        non_active = set(filenames) - active
        logging.info(f'cleanup_dead_nodes.py: {ledger} - {len(active):,} active nodes')
        logging.info(f'cleanup_dead_nodes.py: {ledger} - {len(non_active):,} never active nodes')
        for filename in non_active: # move inactive node files to dead_nodes folder
            f_name = os.path.basename(filename)
            new_path = hlp.get_output_directory(ledger, True) / f_name
            os.rename(filename, new_path)


if __name__ == '__main__':
    main()
