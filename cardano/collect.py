"""
Collects Cardano relay node data using the Blockfrost API.
The Blockfrost API key must be set in the BLOCKFROST_API_KEY environment variable.
"""
import os
import requests
import time
import logging

BLOCKFROST_API_KEY = os.environ.get("BLOCKFROST_API_KEY")
if not BLOCKFROST_API_KEY:
    raise RuntimeError("BLOCKFROST_API_KEY environment variable not set. Please set your Blockfrost API key in the system environment.")
BASE_URL = "https://cardano-mainnet.blockfrost.io/api/v0"
HEADERS = {"project_id": BLOCKFROST_API_KEY}

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

def get_all_pools():
    """Fetch all pool IDs from Blockfrost."""
    pools = []
    page = 1
    while True:
        url = f"{BASE_URL}/pools?page={page}&count=100"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            logging.error(f"Failed to fetch pools: {resp.status_code} {resp.text}")
            break
        data = resp.json()
        if not data:
            break
        pools.extend(data)
        logging.info(f"Fetched {len(pools)} pools so far (page {page})")
        page += 1
        time.sleep(0.2)  # Respect Blockfrost rate limits
    return pools

def get_pool_relays(pool_id):
    """Fetch relay nodes for a given pool ID."""
    url = f"{BASE_URL}/pools/{pool_id}/relays"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        logging.warning(f"Failed to fetch relays for pool {pool_id}: {resp.status_code} {resp.text}")
        return []
    return resp.json()

def main():
    pools = get_all_pools()
    logging.info(f"Total pools fetched: {len(pools)}")
    all_relays = {}
    for i, pool_id in enumerate(pools, 1):
        relays = get_pool_relays(pool_id)
        all_relays[pool_id] = relays
        if i % 50 == 0:
            logging.info(f"Processed {i}/{len(pools)} pools...")
        time.sleep(0.2)  # Respect Blockfrost rate limits
    # Save results
    import json
    with open("blockfrost_pools_relays.json", "w") as f:
        json.dump(all_relays, f, indent=2)
    logging.info("Saved all pool relays to blockfrost_pools_relays.json")

if __name__ == "__main__":
    main()
