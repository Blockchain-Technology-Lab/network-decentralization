#!/bin/bash

source lavenv/bin/activate

while true
do

python3 crawl.py
python3 cleanup_dead_nodes.py
python3 collect_geodata.py
python3 collect_osdata.py
python3 parse.py
python3 plot.py

mkdir output/"$(date +%d-%m-%y)"
mv -t output/"$(date +%d-%m-%y)" output/*.png output/*.csv output/*.json

sleep 7d

done
