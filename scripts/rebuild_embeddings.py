#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from common import LedgerError, connect_db, index_path, initialize_database, print_json, rebuild_embedding_index


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild the current embedding index from SQLite.")
    parser.parse_args()
    try:
        connection = connect_db()
        initialize_database(connection)
        connection.execute("BEGIN")
        indexed = rebuild_embedding_index(connection, require_provider=True)
        connection.commit()
        connection.close()
    except LedgerError as exc:
        if "connection" in locals():
            connection.rollback()
            connection.close()
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception:
        if "connection" in locals():
            connection.rollback()
            connection.close()
        raise

    print_json({"indexed_records": indexed, "index_file": str(index_path())})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
