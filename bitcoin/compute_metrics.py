#!/usr/bin/env python3
"""
Script to compute network decentralization metrics from CSV files in the output directory.
Processes both organization and country CSV files and outputs metrics in CSV format.
"""

import csv
import pathlib
import sys
from network_decentralization.helper import get_metrics_network, get_metrics_geo, get_concentration_ratio_topn
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
    filename = filename.replace('_without_tor', '')  # Normalize bitcoin without_tor variant
    parts = filename.split('_')
    # Remove 'organizations' or 'countries' prefix
    return '_'.join(parts[1:])


def compute_metrics(distribution, metric_names, concentration_ratio_topn):
    """
    Compute specified metrics for a given distribution.
    
    :param distribution: Sorted list of entity counts (descending order)
    :param metric_names: List of metric names to compute (e.g., ['HHI', 'Nakamoto', 'Entropy'])
    :param concentration_ratio_topn: list of top-N parameters for concentration ratio metrics
    :return: Dictionary with computed metric values
    """
    metrics = {}

    # Mapping of metric display names to computation functions
    # Concentration Ratio is handled separately because one metric name expands to multiple outputs (one per configured top-N value).
    metric_map = {
        'HHI': ('hhi', compute_hhi),
        'Nakamoto': ('nakamoto', compute_nakamoto_coefficient),
        'Entropy': ('entropy', lambda d: compute_entropy(d, alpha=1)),
    }
    
    for metric_name in metric_names:
        if metric_name == 'Concentration Ratio':
            for topn in concentration_ratio_topn:
                key = f"concentration_ratio_top_{topn}"
                try:
                    metrics[key] = compute_concentration_ratio(distribution, topn=topn)
                except Exception as e:
                    print(f"Error computing {metric_name} (topn={topn}): {e}", file=sys.stderr)
                    metrics[key] = None
            continue

        if metric_name in metric_map:
            key, func = metric_map[metric_name]
            try:
                metrics[key] = func(distribution)
            except Exception as e:
                print(f"Error computing {metric_name}: {e}", file=sys.stderr)
                metrics[key] = None
    
    return metrics


def process_csv_files(output_dir, file_pattern, is_country, metric_names, concentration_ratio_topn):
    """
    Process all CSV files matching a pattern and output metrics.
    Appends results to existing files or creates new ones.
    For bitcoin, uses the _without_tor versions if they exist.
    
    :param output_dir: Path to the output directory
    :param file_pattern: Glob pattern for CSV files (e.g., 'organizations_*.csv')
    :param is_country: Boolean to indicate if processing country files
    :param metric_names: List of metric names to compute and output
    :param concentration_ratio_topn: list of top-N parameters for concentration ratio metrics
    """
    # For bitcoin, prefer the _without_tor variant if it exists; skip the regular bitcoin file in that case
    file_type = 'countries' if is_country else 'organizations'
    without_tor_path = output_dir / f"{file_type}_bitcoin_without_tor.csv"
    skip_regular_bitcoin = without_tor_path.exists()

    csv_files = sorted(output_dir.glob(file_pattern))
    
    for csv_path in csv_files:
        if csv_path.name == f"{file_type}_bitcoin.csv" and skip_regular_bitcoin:
            continue
            
        try:
            ledger = get_ledger_name(csv_path)
            date, distribution = read_csv_data(csv_path)
            metrics = compute_metrics(distribution, metric_names, concentration_ratio_topn)
            
            # Determine output filename and metric column mapping
            file_type = 'countries' if is_country else 'organizations'
            output_filename = f"output_{file_type}_{ledger}.csv"
            output_path = output_dir / output_filename
            file_exists = output_path.exists()
            
            # Build output metric columns from selected metrics.
            metric_columns = []
            for metric_name in metric_names:
                if metric_name == 'Concentration Ratio':
                    for topn in concentration_ratio_topn:
                        metric_columns.append((f"Concentration Ratio (Top {topn})", f"concentration_ratio_top_{topn}"))
                elif metric_name == 'HHI':
                    metric_columns.append(('HHI', 'hhi'))
                elif metric_name == 'Nakamoto':
                    metric_columns.append(('Nakamoto', 'nakamoto'))
                elif metric_name == 'Entropy':
                    metric_columns.append(('Entropy', 'entropy'))

            header = ['ledger', 'date', 'clustering'] + [column[0] for column in metric_columns]
            
            # Write header and data (append if exists)
            with open(output_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(header)
                
                # Build row with metric values in the same order as header
                row = [ledger, date, 'False']
                for _, metric_key in metric_columns:
                    value = metrics.get(metric_key)
                    if value is None:
                        row.append('')
                    elif isinstance(value, float):
                        row.append(f"{value:.16g}")
                    else:
                        row.append(str(value))
                
                writer.writerow(row)
            
            print(f"Appended to: {output_filename}", file=sys.stderr)
            
        except Exception as e:
            print(f"Error processing {csv_path.name}: {e}", file=sys.stderr)
            continue


def main():
    """
    Main entry point for the script.
    Loads metric names from config and processes organization and country CSV files.
    """
    # Load metric names from config using helper functions
    network_metrics = get_metrics_network()
    geo_metrics = get_metrics_geo()
    concentration_ratio_topn = get_concentration_ratio_topn()
    
    output_dir = pathlib.Path(__file__).parent / 'output'
    
    if not output_dir.exists():
        print(f"Error: Output directory not found at {output_dir}", file=sys.stderr)
        sys.exit(1)
    # Process organization files with network metrics
    process_csv_files(
        output_dir,
        'organizations_*.csv',
        is_country=False,
        metric_names=network_metrics,
        concentration_ratio_topn=concentration_ratio_topn,
    )
    
    # Process country files with geo metrics
    process_csv_files(
        output_dir,
        'countries_*.csv',
        is_country=True,
        metric_names=geo_metrics,
        concentration_ratio_topn=concentration_ratio_topn,
    )


if __name__ == '__main__':
    main()
