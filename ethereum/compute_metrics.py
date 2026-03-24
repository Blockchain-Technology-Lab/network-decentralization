#!/usr/bin/env python3
"""
Compute decentralization metrics from Ethereum CSV distributions in output/.
"""

import csv
import pathlib
import sys
from ast import literal_eval

import helper as hlp
from metrics.herfindahl_hirschman_index import compute_hhi
from metrics.nakamoto_coefficient import compute_nakamoto_coefficient
from metrics.entropy import compute_entropy
from metrics.concentration_ratio import compute_concentration_ratio


def read_csv_data(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if len(header) < 2:
            raise ValueError("CSV header does not contain any date column")

        date = header[-1]
        distribution = []
        for row in reader:
            if len(row) < 2:
                continue
            try:
                value = int(float(row[-1]))
            except ValueError:
                continue
            distribution.append(value)

        distribution.sort(reverse=True)
        return date, distribution


def normalize_metric_name(metric_name):
    """Normalizes metric names from config into compute_* function suffixes."""
    if metric_name is None:
        return ''
    return str(metric_name).strip().lower().replace('-', '_').replace(' ', '_')


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
        columns.append(parsed)

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
        except Exception as exc:
            print(f"Error computing {metric_token}: {exc}", file=sys.stderr)
            metrics[metric_token] = None

    return metrics


def process_csv_files(output_dir, file_pattern, is_country, metric_names):
    metric_columns = build_metric_columns(metric_names)
    csv_files = sorted(output_dir.glob(file_pattern))

    for csv_path in csv_files:
        try:
            layer = csv_path.stem.split("_", 1)[1]
            date, distribution = read_csv_data(csv_path)
            metrics = compute_metrics(distribution, metric_columns)

            file_type = "countries" if is_country else "organizations"
            output_filename = f"output_{file_type}_{layer}.csv"
            output_path = output_dir / output_filename
            file_exists = output_path.exists()

            header = ["layer", "date", "clustering"] + [metric_token for metric_token, _, _ in metric_columns]

            with open(output_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(header)

                row = [layer, date, "False"]
                for metric_token, _, _ in metric_columns:
                    value = metrics.get(metric_token)
                    if value is None:
                        row.append("")
                    elif isinstance(value, float):
                        row.append(f"{value:.16g}")
                    else:
                        row.append(str(value))
                writer.writerow(row)

            print(f"Appended to: {output_filename}", file=sys.stderr)

        except Exception as exc:
            print(f"Error processing {csv_path.name}: {exc}", file=sys.stderr)


def main():
    network_metrics = hlp.get_metrics_network()
    geo_metrics = hlp.get_metrics_geo()

    output_dir = pathlib.Path(__file__).parent / "output"
    if not output_dir.exists():
        print(f"Error: Output directory not found at {output_dir}", file=sys.stderr)
        sys.exit(1)

    process_csv_files(
        output_dir,
        "organizations_*.csv",
        is_country=False,
        metric_names=network_metrics,
    )
    process_csv_files(
        output_dir,
        "countries_*.csv",
        is_country=True,
        metric_names=geo_metrics,
    )


if __name__ == "__main__":
    main()
