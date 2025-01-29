# Network decentralization

# crawl.py
Crawls the network to find all nodes and stores all data in the corresponding folders in the output folder.

# collect_geodata.py
Collects geographical data using the data collected by the crawl.py script. Uses ipapi to geolocate the IP addresses.

# collect_osdata.py
Collects OS data using the data collected by the crawl.py script.

# parse.py
Creates csv files in the output folder from the data collected by the crawl.py, collect_geodata.py and collect_osdata.py scripts.

# plot.py
Converts csv files created by the parse.py script into png files in the output folder.

# requirements.txt
To install all requirements: python3 -m pip install -r requirements.txt
