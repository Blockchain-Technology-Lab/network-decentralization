#!/usr/bin/env python3
"""
Script to compute network decentralization metrics from CSV files in the output directory.
Processes both organization and country CSV files and outputs metrics in CSV format.
"""

import csv
import pathlib
import sys
from network_decentralization.metrics.herfindahl_hirschman_index import compute_hhi
from network_decentralization.metrics.nakamoto_coefficient import compute_nakamoto_coefficient
from network_decentralization.metrics.entropy import compute_entropy
from network_decentralization.metrics.concentration_ratio import compute_concentration_ratio


def read_csv_data(csv_path):
    """
    Read CSV file and extract date and distribution values.
    CSV format: Header row is "EntityType,YYYY-MM-DD" followed by data rows "entity_name,count"
    
    :param csv_path: Path to the CSV file
    :return: Tuple of (date, sorted_distribution_list)
    """
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        # Read header
        header = next(reader)
        date = header[1]  # Extract date from header
        
        # Read data rows and extract counts
        distribution = []
        for row in reader:
            if len(row) >= 2:
                try:
                    count = int(row[1])
                    distribution.append(count)
                except (ValueError, IndexError):
                    continue
        
        # Sort in descending order for metric calculations
        distribution.sort(reverse=True)
        
        return date, distribution


def get_ledger_name(csv_path):
    """
    Extract ledger name from CSV filename.
    Expected format: organizations_<ledger>.csv or countries_<ledger>.csv
    
    :param csv_path: Path to the CSV file
    :return: Ledger name (e.g., 'bitcoin', 'bitcoin_cash')
    """
    filename = csv_path.stem  # Get filename without extension
    parts = filename.split('_')
    # Remove 'organizations' or 'countries' prefix
    return '_'.join(parts[1:])


def compute_metrics(distribution):
    """
    Compute all metrics for a given distribution.
    
    :param distribution: Sorted list of entity counts (descending order)
    :return: Dictionary with all computed metrics
    """
    total = sum(distribution)
    
    if total == 0:
        return {
            'hhi': None,
            'nakamoto': None,
            'entropy': None,
            'max_power_ratio': None
        }
    
    metrics = {
        'hhi': compute_hhi(distribution),
        'nakamoto': compute_nakamoto_coefficient(distribution),
        'entropy': compute_entropy(distribution, alpha=1),  # Shannon entropy
        'max_power_ratio': max(distribution) / total if distribution else 0
    }
    
    return metrics


def process_csv_files(output_dir, file_pattern, is_country=False):
    """
    Process all CSV files matching a pattern and output metrics.
    Appends results to existing files or creates new ones.
    For bitcoin, uses the _without_tor versions if they exist.
    
    :param output_dir: Path to the output directory
    :param file_pattern: Glob pattern for CSV files (e.g., 'organizations_*.csv')
    :param is_country: Boolean to indicate if processing country files
    """
    csv_files = sorted(output_dir.glob(file_pattern))
    
    for csv_path in csv_files:
        # Skip _without_tor files in the glob - we'll handle them explicitly for bitcoin
        if '_without_tor' in csv_path.name:
            continue
            
        try:
            ledger = get_ledger_name(csv_path)
            
            # For bitcoin, check if _without_tor version exists and use that instead
            if ledger == 'bitcoin':
                file_type = 'countries' if is_country else 'organizations'
                without_tor_path = output_dir / f"{file_type}_bitcoin_without_tor.csv"
                if without_tor_path.exists():
                    csv_path = without_tor_path
            
            date, distribution = read_csv_data(csv_path)
            metrics = compute_metrics(distribution)
            
            # Determine output filename
            if is_country:
                output_filename = f"output_countries_{ledger}.csv"
                output_path = output_dir / output_filename
                file_exists = output_path.exists()
                
                # Write header and data (append if exists)
                with open(output_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(['ledger', 'date', 'clustering', 'entropy', 'hhi', 'nakamoto_coefficient', 'max_power_ratio'])
                    writer.writerow([
                        ledger,
                        date,
                        'False',
                        f"{metrics['entropy']:.15g}",
                        f"{metrics['hhi']:.16g}",
                        metrics['nakamoto'],
                        f"{metrics['max_power_ratio']:.16g}"
                    ])
                print(f"Appended to: {output_filename}", file=sys.stderr)
            else:
                output_filename = f"output_organizations_{ledger}.csv"
                output_path = output_dir / output_filename
                file_exists = output_path.exists()
                
                # Write header and data (append if exists)
                with open(output_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(['ledger', 'date', 'clustering', 'hhi', 'nakamoto_coefficient', 'max_power_ratio'])
                    writer.writerow([
                        ledger,
                        date,
                        'False',
                        f"{metrics['hhi']:.16g}",
                        metrics['nakamoto'],
                        f"{metrics['max_power_ratio']:.16g}"
                    ])
                print(f"Appended to: {output_filename}", file=sys.stderr)
            
        except Exception as e:
            print(f"Error processing {csv_path.name}: {e}", file=sys.stderr)
            continue


def main():
    """
    Main entry point for the script.
    Processes organization and country CSV files from the output directory.
    """
    output_dir = pathlib.Path(__file__).parent / 'output'
    
    if not output_dir.exists():
        print(f"Error: Output directory not found at {output_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Process organization files
    process_csv_files(output_dir, 'organizations_*.csv', is_country=False)
    
    # Process country files
    process_csv_files(output_dir, 'countries_*.csv', is_country=True)


if __name__ == '__main__':
    main()
