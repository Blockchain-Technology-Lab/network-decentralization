from network_decentralization.collect import crawl_network
from random import randint, shuffle
import network_decentralization.helper as hlp
import time
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def main():
    ledgers = hlp.get_ledgers()
    shuffle(ledgers)

    while True:
        timings = {}
        for ledger in ledgers:
            start = time.time()
            crawl_network(ledger)
            total_time = time.time() - start
            timings[ledger] = total_time

        print(7*'----------------\n')
        for ledger in ledgers:
            total_time = timings[ledger]
            days = int(total_time / 86400)
            hours = int((total_time - days*86400) / 3600)
            mins = int((total_time - hours*3600 - days*86400) / 60)
            secs = int(total_time - mins*60 - hours*3600 - days*86400)
            print(f'\t{ledger} total time: {hours:02} hours, {mins:02} mins, {secs:02} secs')
        print(7*'----------------\n')

        sleep_duration = randint(0, int(max(0, 24*60*60 - sum(timings.values()))))
        hours = int(sleep_duration/(60*60))
        minutes = int((sleep_duration - hours*60*60)/60)
        seconds = sleep_duration - minutes*60 - hours*60*60
        print(f'Sleeping for {hours} hours, {minutes} mins, {seconds} secs')
        time.sleep(sleep_duration)


if __name__ == '__main__':
    main()
