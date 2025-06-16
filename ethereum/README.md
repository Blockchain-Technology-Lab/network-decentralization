# Network decentralisation

This component of the project analyses the decentralisation of the Ethereum network by exploring it, collecting information about participating nodes and visualising this information through different graphs. To run the tool, please see the 'Requirements' section, then use the following command:
```bash
./automation.sh
```
`automation.sh` uses a Python virtual environment. To run it, it is therefore recommended to create one to install all dependencies. The name of this environment is, by default, 'venv', but it can be modified in `automation.sh`. Other parameters can be modified in `config.yaml`.

---

## Requirements

Python 3.9 or higher is required. You also need `git`, a C compiler and `make`.

To clone this repository, please use:
```bash
git clone --recurse-submodules git@github.com:Blockchain-Technology-Lab/network-decentralization.git
```
Then, to download Nim dependencies and build the crawler, in the 'ethereum' folder, run:
```bash
make -j4 update
make -j4
```
Please note that it may take some time.  
Install Python dependencies - preferably in a Python virtual environment - using:
```bash
python3 -m pip install -r requirements.txt
```
Also, you may need to change the permissions of the automation file:
```bash
chmod +x automation.sh
```

---

## Workflow Overview

1. **Network Crawling:** `dcrawl.nim` tries to discover all nodes participating in the network. This script comes from the [Fast Ethereum Crawler](https://github.com/cskiraly/fast-ethereum-crawler.git).
2. **Data Collection:** `collect_geodata.py` collects data about nodes like IP addresses and geolocation.
3. **Data Parsing:** `parse.py` formats raw logs into structured files.
4. **Visualisation:** `plot.py` generates several graphs.

---

## Script Descriptions

### Core Scripts

- **`dcrawl.nim`**  
  Discovers nodes using bootnodes and recursive peer discovery via the Ethereum Discovery protocol. Communicates with peers and gathers peer info.

- **`parse.py`**  
  Processes raw data (e.g., logs from crawling) into structured formats (JSON, CSV) for easier analysis and plotting.

- **`plot.py`**  
  Generates data visualisations.

- **`collect_geodata.py`**  
  Uses third-party APIs to enrich nodes with geolocation info (country, city, organisation).

- **`collect.py`**  
  Used by collect_geodata.py to enrich nodes with geolocation info.
  
- **`helper.py`**  
  Utility functions for logging, time formatting, file handling, etc.


### Automation & Configuration

- **`automation.sh`**  
  Bash script that automates the full pipeline.

- **`config.yaml`**  
  Configuration file defining parameters like the the execution parameters.


---

## Output

The scripts generate:
- Parsed node datasets (CSV, JSON)
- Geolocation-enriched data
- Plots and charts in PNG

---

## Directory Structure

```
ethereum/
│
├── automation.sh
├── run.sh
├── dcrawl.nim
├── collect_geodata.py
├── parse.py
├── plot.py
├── collect.py
├── helper.py
│
├── config.yaml
├── requirements.txt
├── .gitignore
├── LICENSE
├── README.md
├── Makefile
│
└── vendor/
    └── *all submodules*
```
