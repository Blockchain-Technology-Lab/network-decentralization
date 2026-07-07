#!/bin/bash

source venv/bin/activate # venv is the Python virtual environment where all dependencies must be installed

declare -i DAYS=7

CRAWLER_DIR="crawler"

while true
do

( cd "$CRAWLER_DIR" && ./run.sh --guess --identify "$@" ) # comment this line if new data must not be gathered

latest_run="$(ls -td "$CRAWLER_DIR"/results/* 2>/dev/null | head -1 || true)"
if [ -z "$latest_run" ]; then
	echo "Error: no crawler results directory was created." >&2
	exit 1
fi
export OUTPUT_DIRECTORY="$latest_run"

python3 collect_geodata.py
python3 parse.py
python3 plot.py
python3 compute_metrics.py

# Push files to GitHub
#python3 push_to_github.py # script not on GitHub

echo "The tool will run again in "$DAYS" days."

sleep "$DAYS"d # will repeat the whole process every DAYS days

done