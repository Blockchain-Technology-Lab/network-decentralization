#!/usr/bin/env python3
"""
Script to compute network decentralization metrics from CSV files in the output directory.
Processes both organization and country CSV files and outputs metrics in CSV format.
"""

import csv
import pathlib
import sys
from ast import literal_eval

from network_decentralization.helper import get_metrics_network, get_metrics_geo, get_without_tor_ledgers
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


def normalize_metric_name(metric_name):
    """Normalizes metric names from config into registry keys."""
    if metric_name is None:
        return ''
    return str(metric_name).strip().lower().replace('-', '_').replace(' ', '_')


def parse_metric_spec(metric_spec):
    """Parses metric token strings like 'entropy=1' into (token, name, parameter)."""
    token = str(metric_spec).strip()
    if not token:
        return None

    if '=' not in token:
        return token, normalize_metric_name(token), None

    raw_name, raw_parameter = token.split('=', 1)
    normalized_name = normalize_metric_name(raw_name)
    parameter_text = raw_parameter.strip()
    parameter_value = parse_metric_parameter(parameter_text)
    return token, normalized_name, parameter_value


def parse_metric_parameter(parameter_text):
    """Parses metric parameter values from config strings into Python values."""
    if parameter_text is None:
        return None

    text = str(parameter_text).strip()
    if not text:
        return None

    try:
        return literal_eval(text)
    except (ValueError, SyntaxError):
        return text


def build_metric_columns(metric_specs):
    """
    Builds ordered metric specs from configured metric tokens.
    :param metric_specs: list of metric tokens (e.g., ['hhi', 'entropy=1'])
    :returns: list of tuples (metric_token, metric_name, parameter_value)
    """
    columns = []
    for metric_spec in metric_specs:
        parsed = parse_metric_spec(metric_spec)
        if parsed is None:
            continue

        metric_token, metric_name, parameter_value = parsed
        columns.append((metric_token, metric_name, parameter_value))

    return columns


def compute_metrics(distribution, metric_columns):
    """
    Compute specified metrics for a given distribution.
    
    :param distribution: Sorted list of entity counts (descending order)
    :param metric_columns: list of tuples (metric_token, metric_name, parameter_value)
    :return: Dictionary with computed metric values
    """
    metrics = {}

    for metric_token, metric_name, parameter_value in metric_columns:
        function_name = f"compute_{metric_name}"

        try:
            function = eval(function_name)
            if parameter_value is None:
                metrics[metric_token] = function(distribution)
            else:
                metrics[metric_token] = function(distribution, parameter_value)
        except Exception as e:
            print(f"Error computing {metric_token}: {e}", file=sys.stderr)
            metrics[metric_token] = None
    
    return metrics


def process_csv_files(output_dir, file_pattern, is_country, metric_names):
    """
    Process all CSV files matching a pattern and output metrics.
    Appends results to existing files or creates new ones.
    Uses _without_tor versions when configured in parse_parameters.without_tor_ledgers.
    
    :param output_dir: Path to the output directory
    :param file_pattern: Glob pattern for CSV files (e.g., 'organizations_*.csv')
    :param is_country: Boolean to indicate if processing country files
    :param metric_names: List of metric names to compute and output
    """
    metric_columns = build_metric_columns(metric_names)
    without_tor_ledgers = set(get_without_tor_ledgers() or [])

    # Prefer configured _without_tor variants and skip the corresponding regular file when both exist.
    file_type = 'countries' if is_country else 'organizations'

    csv_files = sorted(output_dir.glob(file_pattern))
    
    for csv_path in csv_files:
        try:
            ledger = get_ledger_name(csv_path)

            regular_path = output_dir / f"{file_type}_{ledger}.csv"
            without_tor_path = output_dir / f"{file_type}_{ledger}_without_tor.csv"
            is_regular_file = csv_path.name == regular_path.name
            has_without_tor_variant = without_tor_path.exists()

            if is_regular_file and ledger in without_tor_ledgers and has_without_tor_variant:
                continue

            date, distribution = read_csv_data(csv_path)
            metrics = compute_metrics(distribution, metric_columns)
            
            # Determine output filename and metric column mapping
            file_type = 'countries' if is_country else 'organizations'
            output_filename = f"output_{file_type}_{ledger}.csv"
            output_path = output_dir / output_filename
            file_exists = output_path.exists()
            
            header = ['ledger', 'date', 'clustering'] + [metric_token for metric_token, _, _ in metric_columns]
            
            # Write header and data (append if exists)
            with open(output_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(header)
                
                # Build row with metric values in the same order as header
                row = [ledger, date, 'False']
                for metric_token, _, _ in metric_columns:
                    value = metrics.get(metric_token)
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
    )
    
    # Process country files with geo metrics
    process_csv_files(
        output_dir,
        'countries_*.csv',
        is_country=True,
        metric_names=geo_metrics,
    )


if __name__ == '__main__':
    main()
