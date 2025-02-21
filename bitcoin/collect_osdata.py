from network_decentralization.collect import collect_osdata
import network_decentralization.helper as hlp
import time
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def main():
    ledgers = hlp.get_ledgers()
    timings = {}
    for ledger in ledgers:
        start = time.time()
        collect_osdata(ledger, time.strftime('%Y-%m-%d'))
        total_time = time.time() - start
        timings[ledger] = total_time

    print(2*'----------------\n')
    for ledger in hlp.get_ledgers():
        total_time = timings[ledger]
        days = int(total_time / 86400)
        hours = int((total_time - days*86400) / 3600)
        mins = int((total_time - hours*3600 - days*86400) / 60)
        secs = int(total_time - mins*60 - hours*3600 - days*86400)
        print(f'\tcollect_osdata.py: {ledger} total time: {hours:02} hours, {mins:02} mins, {secs:02} secs')
    print(2*'----------------\n')


if __name__ == '__main__':
    main()
