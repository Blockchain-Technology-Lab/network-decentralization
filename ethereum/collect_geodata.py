from collect import collect_geodata
import helper as hlp
import time
import logging

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)


def main():
    start = time.time()
    collect_geodata()
    total_time = time.time() - start

    print(2*'----------------\n')
    days = int(total_time / 86400)
    hours = int((total_time - days*86400) / 3600)
    mins = int((total_time - hours*3600 - days*86400) / 60)
    secs = int(total_time - mins*60 - hours*3600 - days*86400)
    print(f'\tcollect_geodata.py: Total time: {hours:02} hours, {mins:02} mins, {secs:02} secs')
    print(2*'----------------\n')

#        time.sleep(60*60)


if __name__ == '__main__':
    main()
