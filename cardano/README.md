# Cardano Relay Analysis Pipeline

Automated pipeline to analyze the distribution of Cardano relay nodes using Blockfrost, DNS resolution, geolocation APIs, and visual analytics.

## Overview

This pipeline extracts IP addresses from Cardano relay data, queries geolocation APIs, and generates visual analytics showing the distribution of nodes by country, organization, and ASN. It also computes decentralization metrics from parsed country/organization distributions.

## Requirements

It is recommended to use a dedicated Python virtual environment for this project. Create a `.venv` virtual environment in the project directory before installing dependencies:

**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Linux/macOS (bash):**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

This ensures that all dependencies are installed in an isolated environment. The automation script expects the `.venv` environment to be present.

To run the pipeline you need a Blockfrost API key. To get one, follow these steps:
- Go to the Blockfrost website: https://blockfrost.io/
- Sign up for an account.
- Once logged in, in your dashboard, create a new project.
- Your API key for that project will appear in the dashboard.

You must set your Blockfrost API key in the BLOCKFROST_API_KEY environment variable before running the pipeline.

**Windows (PowerShell):**
```powershell
$env:BLOCKFROST_API_KEY = "your_blockfrost_api_key_here"
```
**Linux/macOS (bash):**
```bash
export BLOCKFROST_API_KEY="your_blockfrost_api_key_here"
```

If the environment variable is not set, the pipeline will display an error and exit.

## Quick Start

### Run the Complete Pipeline

The `automation.sh` script runs the full pipeline in a loop, archives generated CSV/PNG outputs into a date-based folder under `output/`, and waits 7 days before repeating.

Run:
```bash
./automation.sh
```

This will execute all 6 steps:
1. Collect relay node data
2. Resolve relay DNS names
3. Collect geolocation data
4. Parse data into CSVs
5. Compute decentralization metrics
6. Generate plots

## Files

### Pipeline Scripts
- **`automation.sh`** - Repeats the full pipeline every 7 days and archives CSV/PNG outputs into `output/YYYY-MM-DD/`
- **`collect.py`** - Collects relay node data using Blockfrost
- **`resolve_dns.py`** - Resolves relay DNS names and writes output/dns_resolved.json
- **`collect_geodata.py`** - Queries geolocation APIs (ip-api.com, ipapi.is) for IP metadata
- **`parse.py`** - Parses geodata and creates CSV files for analysis
- **`compute_metrics.py`** - Computes decentralization metrics from parsed country/organization CSV files
- **`plot.py`** - Generates pie charts showing distribution
- **`run.py`** - Master script that runs all steps in sequence

## Output Files

Apart from `blockfrost_pools_relays.json`, saved in the main directory, all outputs are saved in the `output/` directory:

### JSON Data
- `blockfrost_pools_relays.json` - Raw relay and pool data collected from Blockfrost, used as the initial input for further processing.
- `geodata/cardano.json` - Geolocation metadata for each IP

### CSV Files
- `countries_cardano.csv` - Node distribution by country
- `organizations_cardano.csv` - Node distribution by hosting organization
- `asn_cardano.csv` - Node distribution by Autonomous System Number
- `output_countries_cardano.csv` - Computed metrics for country distributions
- `output_organizations_cardano.csv` - Computed metrics for organization distributions

When running via `automation.sh`, the generated CSV and PNG files are moved into `output/YYYY-MM-DD/` after each run.

### PNG Files
- `countries_cardano.png` - Pie chart of nodes by country
- `organizations_cardano.png` - Pie chart of nodes by organization
- `asn_cardano.png` - Pie chart of nodes by ASN
