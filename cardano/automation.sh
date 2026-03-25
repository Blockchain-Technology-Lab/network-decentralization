#!/bin/bash

source venv/bin/activate # venv is the Python virtual environment where all dependencies must be installed

while true
do

python3 run.py

# The following 2 lines create a folder and move all png and csv files to it
mkdir output/"$(date +%Y-%m-%d)"
mv -t output/"$(date +%Y-%m-%d)" output/*.png output/countries_cardano.csv output/organizations_cardano.csv output/asn_cardano.csv 2>/dev/null || true

sleep 7d # will repeat the whole process every X days

done
