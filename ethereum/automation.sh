#!/bin/bash

source venv/bin/activate # venv is the Python virtual environment where all dependencies must be installed

declare -i DAYS=7

BOOTNODE="enr:-Ku4QHqVeJ8PPICcWk1vSn_XcSkjOkNiTg6Fmii5j6vUQgvzMc9L1goFnLKgXqBJspJjIsB91LTOleFmyWWrFVATGngBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAMRHkWJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyg"

OUTPUTDIR="output"
[ ! -d "/path/to/dir" ] && mkdir -p "$OUTPUTDIR"

while true
do

crawler/build/dcrawl --bootnode="$BOOTNODE" "$@" # comment this line if new data must not be gathered
mv -t "$OUTPUTDIR" *.csv # the output is moved to the output directory
python3 collect_geodata.py
python3 parse.py
python3 plot.py

# The following 2 lines create a folder and move all png and csv files to it
mkdir "$OUTPUTDIR"/"$(date +%Y-%m-%d)"
mv -t "$OUTPUTDIR"/"$(date +%Y-%m-%d)" output/*.png output/*.csv

echo "The tool will run again in "$DAYS" days."

sleep "$DAYS"d # will repeat the whole process every DAYS days

done
