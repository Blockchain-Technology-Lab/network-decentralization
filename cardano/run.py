"""
Master script to run the complete Cardano analysis pipeline.
Gathers relay node data using Blockfrost, resolves DNS names, collects geodata, parses, and plots.
"""
import subprocess
import sys
import logging
from pathlib import Path

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)

# Get the Python executable from the venv
PYTHON_EXE = Path(__file__).parent.parent / '.venv' / 'Scripts' / 'python.exe'
if not PYTHON_EXE.exists():
    PYTHON_EXE = sys.executable

SCRIPTS_DIR = Path(__file__).parent


def run_script(script_name, description):
    """Run a Python script and handle errors."""
    logging.info(f'\n{"="*60}')
    logging.info(f'{description}')
    logging.info(f'{"="*60}\n')
    
    script_path = SCRIPTS_DIR / script_name
    
    try:
        result = subprocess.run(
            [str(PYTHON_EXE), str(script_path)],
            cwd=str(SCRIPTS_DIR),
            check=True,
            capture_output=False,
            text=True
        )
        logging.info(f'✓ {script_name} completed successfully\n')
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f'✗ {script_name} failed with error code {e.returncode}\n')
        return False
    except Exception as e:
        logging.error(f'✗ Error running {script_name}: {e}\n')
        return False


def main():
    """Run the complete pipeline."""
    logging.info('\n' + '='*60)
    logging.info('CARDANO RELAY ANALYSIS PIPELINE')
    logging.info('='*60)
    
    steps = [
        ('collect.py', 'Step 1: Collect network data from relays'),
        ('resolve_dns.py', 'Step 2: Resolve DNS names and update dns_resolved.json'),
        ('collect_geodata.py', 'Step 3: Collect geolocation data from APIs'),
        ('parse.py', 'Step 4: Parse geodata and create CSV files'),
        ('plot.py', 'Step 5: Generate geographic distribution plots'),
    ]
    
    results = {}
    
    for script, description in steps:
        success = run_script(script, description)
        results[script] = success
        
        if not success:
            logging.error(f'\nPipeline stopped due to error in {script}')
            logging.info('\nYou can run individual steps manually:')
            for s, d in steps:
                status = '✓' if results.get(s, False) else ('✗' if s in results else '○')
                logging.info(f'  {status} python {s}')
            sys.exit(1)
    
    logging.info('\n' + '='*60)
    logging.info('PIPELINE COMPLETE!')
    logging.info('='*60)
    logging.info('\nOutput files created in the output/ directory:')
    logging.info('  - dns_resolved.json (IP addresses)')
    logging.info('  - geodata/cardano.json (geolocation data)')
    logging.info('  - countries_cardano.csv (country distribution)')
    logging.info('  - organizations_cardano.csv (organization distribution)')
    logging.info('  - asn_cardano.csv (ASN distribution)')
    logging.info('  - countries_cardano.png (country pie chart)')
    logging.info('  - organizations_cardano.png (organization pie chart)')
    logging.info('  - asn_cardano.png (ASN pie chart)')
    logging.info('\nAll steps completed successfully! ✓')


if __name__ == '__main__':
    main()
