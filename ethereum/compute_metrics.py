#!/usr/bin/env python3
"""
Compute decentralization metrics from Ethereum CSV distributions in output/.
"""

import csv
import pathlib
import sys
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


def compute_metrics(distribution, metric_names, concentration_ratio_topn):
    metrics = {}

    for metric_name in metric_names:
        if metric_name == "HHI":
            metrics["hhi"] = compute_hhi(distribution)
        elif metric_name == "Nakamoto":
            metrics["nakamoto"] = compute_nakamoto_coefficient(distribution)
        elif metric_name == "Entropy":
            metrics["entropy"] = compute_entropy(distribution, alpha=1)
        elif metric_name == "Concentration Ratio":
            for topn in concentration_ratio_topn:
                key = f"concentration_ratio_top_{topn}"
                metrics[key] = compute_concentration_ratio(distribution, topn=topn)

    return metrics


def process_csv_files(output_dir, file_pattern, is_country, metric_names, concentration_ratio_topn):
    csv_files = sorted(output_dir.glob(file_pattern))

    for csv_path in csv_files:
        try:
            layer = csv_path.stem.split("_", 1)[1]
            date, distribution = read_csv_data(csv_path)
            metrics = compute_metrics(distribution, metric_names, concentration_ratio_topn)

            file_type = "countries" if is_country else "organizations"
            output_filename = f"output_{file_type}_{layer}.csv"
            output_path = output_dir / output_filename
            file_exists = output_path.exists()

            metric_columns = []
            for metric_name in metric_names:
                if metric_name == "HHI":
                    metric_columns.append(("HHI", "hhi"))
                elif metric_name == "Nakamoto":
                    metric_columns.append(("Nakamoto", "nakamoto"))
                elif metric_name == "Entropy":
                    metric_columns.append(("Entropy", "entropy"))
                elif metric_name == "Concentration Ratio":
                    for topn in concentration_ratio_topn:
                        metric_columns.append((f"Concentration Ratio (Top {topn})", f"concentration_ratio_top_{topn}"))

            header = ["layer", "date", "clustering"] + [column[0] for column in metric_columns]

            with open(output_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(header)

                row = [layer, date, "False"]
                for _, metric_key in metric_columns:
                    value = metrics.get(metric_key)
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
    concentration_ratio_topn = hlp.get_concentration_ratio_topn()

    output_dir = pathlib.Path(__file__).parent / "output"
    if not output_dir.exists():
        print(f"Error: Output directory not found at {output_dir}", file=sys.stderr)
        sys.exit(1)

    process_csv_files(
        output_dir,
        "organizations_*.csv",
        is_country=False,
        metric_names=network_metrics,
        concentration_ratio_topn=concentration_ratio_topn,
    )
    process_csv_files(
        output_dir,
        "countries_*.csv",
        is_country=True,
        metric_names=geo_metrics,
        concentration_ratio_topn=concentration_ratio_topn,
    )


if __name__ == "__main__":
    main()
