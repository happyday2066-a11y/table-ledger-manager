#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from common import LedgerError, connect_db, db_path, index_path, initialize_database, print_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize the SQLite database and runtime directories.")
    parser.parse_args()
    try:
        connection = connect_db()
        initialize_database(connection)
        connection.close()
    except LedgerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print_json({"database": str(db_path()), "index": str(index_path()), "status": "initialized"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
