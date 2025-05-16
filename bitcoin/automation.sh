#!/bin/bash

source venv/bin/activate # venv is the Python virtual environment where all dependencies must be installed

while true
do

python3 crawl.py # comment this line if new data must not be gathered
python3 cleanup_dead_nodes.py
python3 collect_geodata.py
#python3 collect_osdata.py # not in use
python3 parse.py
python3 distribution.py # called only if the Bitcoin ledger is selected
python3 plot.py

# The following 2 lines create a folder and move all png and csv files to it
mkdir output/"$(date +%d-%m-%y)"
mv -t output/"$(date +%d-%m-%y)" output/*.png output/*.csv

sleep 7d # will repeat the whole process every X days

done
