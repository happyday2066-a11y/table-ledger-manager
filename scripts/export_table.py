#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common import (
    LedgerError,
    connect_db,
    exports_dir,
    fetch_current_records,
    initialize_database,
    normalize_ledger_name,
    parse_requested_columns,
    print_json,
    project_rows_for_export,
)


def build_exact_filters(args: argparse.Namespace) -> dict[str, str]:
    return {
        "entity_name": args.entity_name,
        "location": args.location,
        "ip": args.ip,
        "department": args.department,
        "owner": args.owner,
        "status": args.status,
        "transport": args.transport,
        "source_file": args.source_file,
    }


def build_contains_filters(args: argparse.Namespace) -> dict[str, str]:
    return {
        "entity_name": args.contains_name,
        "location": args.contains_location,
        "remark": args.contains_remark,
        "transport": args.contains_transport,
    }


def default_output_path(output_format: str, ledger_name: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = "csv" if output_format == "csv" else "xlsx"
    return exports_dir() / f"{ledger_name}-export-{timestamp}.{suffix}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Export current active ledger records to CSV or Excel.")
    parser.add_argument("--ledger", default="default")
    parser.add_argument("--format", choices=("csv", "xlsx"), default="csv")
    parser.add_argument("--output", help="Optional output file path.")
    parser.add_argument("--columns", help="Comma-separated output columns, e.g. Name,IP Address")

    parser.add_argument("--entity-name")
    parser.add_argument("--location")
    parser.add_argument("--ip")
    parser.add_argument("--department")
    parser.add_argument("--owner")
    parser.add_argument("--status")
    parser.add_argument("--transport")
    parser.add_argument("--source-file")

    parser.add_argument("--contains-name")
    parser.add_argument("--contains-location")
    parser.add_argument("--contains-remark")
    parser.add_argument("--contains-transport")
    args = parser.parse_args()

    try:
        ledger_name = normalize_ledger_name(args.ledger)
        connection = connect_db()
        initialize_database(connection)
        rows = fetch_current_records(
            connection,
            ledger_name=ledger_name,
            filters=build_exact_filters(args),
            contains_filters=build_contains_filters(args),
            include_deleted=False,
        )
        connection.close()

        requested_columns = parse_requested_columns(args.columns)
        projected_rows, resolved_columns = project_rows_for_export(rows, requested_columns)

        output_path = Path(args.output).expanduser().resolve() if args.output else default_output_path(args.format, ledger_name)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        dataframe = pd.DataFrame(projected_rows, columns=resolved_columns)
        if args.format == "csv":
            dataframe.to_csv(output_path, index=False)
        else:
            dataframe.to_excel(output_path, index=False)
    except LedgerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print_json(
        {
            "ledger": ledger_name,
            "exported_rows": len(projected_rows),
            "columns": resolved_columns,
            "format": args.format,
            "output_file": str(output_path),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
