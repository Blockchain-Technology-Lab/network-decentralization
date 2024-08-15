from network_decentralization.collect import collect_geodata
import network_decentralization.helper as hlp
import time
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def main():
    ledgers = hlp.get_ledgers()
    while True:
        timings = {}
        for ledger in ledgers:
            start = time.time()
            collect_geodata(ledger)
            total_time = time.time() - start
            timings[ledger] = total_time

        print(7*'----------------\n')
        for ledger in hlp.get_ledgers():
            total_time = timings[ledger]
            days = int(total_time / 86400)
            hours = int((total_time - days*86400) / 3600)
            mins = int((total_time - hours*3600 - days*86400) / 60)
            secs = int(total_time - mins*60 - hours*3600 - days*86400)
            print(f'\t{ledger} total time: {hours:02} hours, {mins:02} mins, {secs:02} secs')
        print(7*'----------------\n')

        time.sleep(60*60)


if __name__ == '__main__':
    main()
