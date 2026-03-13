#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from common import (
    LedgerError,
    connect_db,
    initialize_database,
    normalize_ledger_name,
    print_json,
    semantic_search_records,
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Semantic search over current ledger records.")
    parser.add_argument("query", help="Natural-language query.")
    parser.add_argument("--ledger", default="default")
    parser.add_argument("--top-k", type=int, default=8)

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

    parser.add_argument("--min-score", type=float, default=None)
    args = parser.parse_args()

    try:
        connection = connect_db()
        initialize_database(connection)
        rows = semantic_search_records(
            connection,
            args.query,
            top_k=args.top_k,
            ledger_name=normalize_ledger_name(args.ledger),
            filters=build_exact_filters(args),
            contains_filters=build_contains_filters(args),
        )
        if args.min_score is not None:
            rows = [row for row in rows if row.get("score", 0.0) >= args.min_score]
        connection.close()
    except LedgerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print_json({
        "ledger": normalize_ledger_name(args.ledger),
        "query": args.query,
        "count": len(rows),
        "records": rows,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
