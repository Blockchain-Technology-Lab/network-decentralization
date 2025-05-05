# Network decentralisation

This component of the project analyses the decentralisation of Bitcoin, Bitcoin Cash, Dogecoin, Litecoin and Zcash networks by exploring them, collecting information about participating nodes and visualising this information through different graphs.

---

## Workflow Overview

1. **Network Crawling:** `crawl.py` tries to discover all reachable nodes participating in the network.
2. **Data Collection:** Scripts collect data about nodes like IP addresses and client versions.
3. **Data Parsing:** `parse.py` formats raw logs into structured files.
4. **Visualisation:** `plot.py` generates several graphs.

---

## Script Descriptions

### Core Scripts

- **`crawl.py`**  
  Discovers nodes using seed nodes and recursive peer discovery via the Bitcoin P2P protocol. Uses low-level sockets to communicate with peers and gathering peer info.

- **`parse.py`**  
  Processes raw data (e.g., logs from crawling) into structured formats (JSON, CSV) for easier analysis and plotting.

- **`analyze.py`** (Not in use)  
  Analyses datasets to extract decentralisation metrics.

- **`distribution.py`**  
  Distributes Tor nodes among others proportionally, by country or organisation.

- **`plot.py`**  
  Generates data visualisations.

- **`collect_geodata.py`**  
  Uses third-party APIs to enrich nodes with geolocation info (country, city, organisation).

- **`collect_osdata.py`** (Not in use)  
  Identifies the operating system running on nodes.

- **`cleanup_dead_nodes.py`**  
  Scans stored node datasets to remove offline or unreachable nodes.

### Automation & Configuration

- **`automation.sh`**  
  Bash script that automates the full pipeline.

- **`config.yaml`**  
  Configuration file defining parameters like the ledgers for which an analysis should be performed and the execution parameters.


### In `network_decentralization`

- **`collect.py`**  
  Internal functions used by `crawl.py` and analysis scripts for handling sockets and networking logic.

- **`constants.py`**  
  Contains constants like magic numbers and protocol identifiers.

- **`helper.py`**  
  Utility functions for logging, time formatting, file handling, etc.

- **`protocol.py`**  
  Implements P2P messaging protocol using raw sockets.


### In `seed_info`

JSON files in the `seed_info/` folder define initial peers or DNS seeds for all ledgers.

---

## Requirements

Install dependencies with:

```bash
python3 -m pip install -r requirements.txt
```

---

## Output

The scripts generate:
- Parsed node datasets (CSV, JSON)
- Geolocation-enriched data
- Plots and charts in PNG

---

## Directory Structure

```
bitcoin/
│
├── analyze.py
├── automation.sh
├── cleanup_dead_nodes.py
├── collect_geodata.py
├── collect_osdata.py
├── crawl.py
├── distribution.py
├── parse.py
├── plot.py
│
├── config.yaml
├── requirements.txt
├── .gitignore
├── .flake8
├── LICENSE
├── README.md
│
├── network_decentralization/
│   ├── collect.py
│   ├── constants.py
│   ├── helper.py
│   └── protocol.py
│
└── seed_info/
    ├── bitcoin.json
    ├── bitcoin_cash.json
    ├── dogecoin.json
    ├── litecoin.json
    └── zcash.json
```
