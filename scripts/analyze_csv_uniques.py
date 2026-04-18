from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


EXCLUDED_COLUMNS = {
    "id",
    "platform_url",
    "platform_id",
    "title",
    "remarks",
    "orig_data",
    "images",
    "object_description",
    "agency_phone",
    "agency_email",
    "location_address",
    "object_street",
}


@dataclass(slots=True)
class ColumnStats:
    non_empty_count: int = 0
    empty_count: int = 0
    value_counts: Counter[str] | None = None
    sample_values: list[str] | None = None

    def __post_init__(self) -> None:
        if self.value_counts is None:
            self.value_counts = Counter()
        if self.sample_values is None:
            self.sample_values = []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze CSV files with shared schemas and print unique values for columns "
            "that look categorical."
        )
    )
    parser.add_argument(
        "--raw-data-dir",
        type=Path,
        default=Path("raw_data"),
        help="Directory containing the CSV files. Default: raw_data",
    )
    parser.add_argument(
        "--max-unique-values",
        type=int,
        default=40,
        help="Only print full unique-value lists for columns at or below this cardinality.",
    )
    parser.add_argument(
        "--include-columns",
        nargs="*",
        default=None,
        help="Optional explicit list of columns to analyze, even if normally excluded.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write the full analysis as JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    csv_paths = sorted(args.raw_data_dir.glob("*.csv"))
    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found in: {args.raw_data_dir}")

    headers = _read_headers(csv_paths)
    stats = {header: ColumnStats() for header in headers}
    file_row_counts: dict[str, int] = {}

    for csv_path in csv_paths:
        row_count = 0
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                row_count += 1
                for header in headers:
                    raw_value = row.get(header)
                    value = _normalize_value(raw_value)
                    if value is None:
                        stats[header].empty_count += 1
                        continue

                    stats[header].non_empty_count += 1
                    stats[header].value_counts[value] += 1
                    if len(stats[header].sample_values) < 5 and value not in stats[header].sample_values:
                        stats[header].sample_values.append(value)
        file_row_counts[csv_path.name] = row_count

    include_columns = set(args.include_columns or [])
    analysis = build_analysis(
        stats=stats,
        max_unique_values=args.max_unique_values,
        include_columns=include_columns,
        file_row_counts=file_row_counts,
    )

    _print_analysis(analysis)
    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(analysis, indent=2), encoding="utf-8")

    return 0


def _read_headers(csv_paths: list[Path]) -> list[str]:
    headers_by_file: dict[str, list[str]] = {}
    for csv_path in csv_paths:
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader)
            cleaned_header = [item.strip().strip('"') for item in header]
            headers_by_file[csv_path.name] = cleaned_header

    first_name, first_header = next(iter(headers_by_file.items()))
    for name, header in headers_by_file.items():
        if header != first_header:
            raise ValueError(
                f"CSV header mismatch. {name} does not match {first_name}."
            )
    return first_header


def _normalize_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None
    return cleaned


def build_analysis(
    *,
    stats: dict[str, ColumnStats],
    max_unique_values: int,
    include_columns: set[str],
    file_row_counts: dict[str, int],
) -> dict[str, Any]:
    columns: dict[str, Any] = {}
    for column_name, column_stats in stats.items():
        unique_count = len(column_stats.value_counts)
        include_full_values = (
            column_name in include_columns
            or (
                column_name not in EXCLUDED_COLUMNS
                and unique_count <= max_unique_values
            )
        )

        sorted_counts = column_stats.value_counts.most_common()
        column_entry: dict[str, Any] = {
            "non_empty_count": column_stats.non_empty_count,
            "empty_count": column_stats.empty_count,
            "unique_count": unique_count,
            "sample_values": column_stats.sample_values,
            "top_values": [
                {"value": value, "count": count}
                for value, count in sorted_counts[:10]
            ],
            "included_for_unique_value_listing": include_full_values,
        }

        if include_full_values:
            column_entry["unique_values"] = [
                {"value": value, "count": count}
                for value, count in sorted_counts
            ]

        columns[column_name] = column_entry

    return {
        "files": file_row_counts,
        "column_count": len(columns),
        "columns": columns,
    }


def _print_analysis(analysis: dict[str, Any]) -> None:
    print("CSV files analyzed:")
    for file_name, row_count in analysis["files"].items():
        print(f"  - {file_name}: {row_count} rows")

    print()
    print("Columns with full unique-value listings:")
    for column_name, details in analysis["columns"].items():
        if not details["included_for_unique_value_listing"]:
            continue
        print(
            f"  - {column_name} "
            f"(unique={details['unique_count']}, "
            f"non_empty={details['non_empty_count']}, empty={details['empty_count']})"
        )
        for item in details["unique_values"]:
            print(f"      {item['value']}: {item['count']}")

    print()
    print("High-cardinality or skipped columns:")
    for column_name, details in analysis["columns"].items():
        if details["included_for_unique_value_listing"]:
            continue
        print(
            f"  - {column_name} "
            f"(unique={details['unique_count']}, "
            f"sample={details['sample_values']})"
        )


if __name__ == "__main__":
    raise SystemExit(main())
