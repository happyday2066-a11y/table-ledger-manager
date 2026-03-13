#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from common import (
    LedgerError,
    append_history,
    archive_raw_file,
    canonicalize_value,
    compute_content_hash,
    connect_db,
    count_active_current_records,
    fetch_current_records,
    infer_source_type,
    initialize_database,
    load_current_record,
    load_input_dataframe,
    log_operation,
    maybe_rebuild_embedding_index,
    normalize_ledger_name,
    now_utc,
    print_json,
    source_file_path,
    standardize_dataframe,
    determine_record_identity,
    safe_json_loads,
    upsert_current_record,
)
from ledger_semantics import rebuild_ledger_semantics


def merge_payload(existing: dict[str, str], incoming: dict[str, str]) -> dict[str, str]:
    merged = dict(existing)
    for key, value in incoming.items():
        if canonicalize_value(value):
            merged[key] = value
    return merged


def merge_rows(existing: dict[str, str], incoming: dict[str, str]) -> dict[str, str]:
    merged = dict(existing)
    for field in (
        "entity_name",
        "location",
        "ip",
        "department",
        "owner",
        "status",
        "transport",
        "brand",
        "model",
        "remark",
    ):
        if canonicalize_value(incoming.get(field, "")):
            merged[field] = incoming[field]

    extras = merge_payload(
        safe_json_loads(existing.get("extra_json")),
        safe_json_loads(incoming.get("extra_json")),
    )
    merged["extra_json"] = json.dumps(extras, ensure_ascii=False, sort_keys=True) if extras else "{}"
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description="Import CSV/XLSX/Markdown tables into a versioned ledger.")
    parser.add_argument("input_file", help="Input CSV, XLSX, or Markdown table.")
    parser.add_argument("--ledger", default="default", help="Ledger namespace (default: default).")
    parser.add_argument(
        "--duplicate-strategy",
        choices=("error", "keep-first", "keep-last", "merge"),
        default="keep-last",
        help="How to handle duplicate identities inside one import batch.",
    )
    parser.add_argument(
        "--sync-delete-missing",
        action="store_true",
        help="Soft-delete active rows from same ledger+source file that are absent in this import.",
    )
    parser.add_argument("--sheet", action="append", default=[], help="Include only this Excel sheet name (repeatable).")
    parser.add_argument("--sheet-regex", help="Include Excel sheets matching this regex (case-insensitive).")
    parser.add_argument("--exclude-sheet", action="append", default=[], help="Exclude this Excel sheet name (repeatable).")
    parser.add_argument("--exclude-sheet-regex", help="Exclude Excel sheets matching this regex (case-insensitive).")
    args = parser.parse_args()

    try:
        ledger_name = normalize_ledger_name(args.ledger)
        source_file = str(source_file_path(args.input_file))
        source_type = infer_source_type(args.input_file)
        raw_copy = archive_raw_file(args.input_file)
        dataframe = load_input_dataframe(
            args.input_file,
            include_sheets=args.sheet,
            include_sheet_regex=args.sheet_regex,
            exclude_sheets=args.exclude_sheet,
            exclude_sheet_regex=args.exclude_sheet_regex,
        )
        standardized, mapping, extra_columns = standardize_dataframe(dataframe)

        connection = connect_db()
        initialize_database(connection)

        stats = {
            "processed": len(standardized),
            "imported": 0,
            "updated": 0,
            "unchanged": 0,
            "soft_deleted": 0,
            "duplicates": 0,
        }

        staged_records: dict[str, dict[str, str]] = {}
        for row_number, row in enumerate(standardized.to_dict(orient="records"), start=1):
            record_id, base_id = determine_record_identity(row, ledger_name, source_file, row_number)
            candidate = {
                "record_id": record_id,
                "base_id": base_id,
                "entity_name": row.get("entity_name", ""),
                "location": row.get("location", ""),
                "ip": row.get("ip", ""),
                "department": row.get("department", ""),
                "owner": row.get("owner", ""),
                "status": row.get("status", ""),
                "transport": row.get("transport", ""),
                "brand": row.get("brand", ""),
                "model": row.get("model", ""),
                "remark": row.get("remark", ""),
                "extra_json": row.get("extra_json", "{}") or "{}",
            }

            if record_id not in staged_records:
                staged_records[record_id] = candidate
                continue

            stats["duplicates"] += 1
            if args.duplicate_strategy == "error":
                raise LedgerError(f"Duplicate record detected in batch: {record_id}")
            if args.duplicate_strategy == "keep-first":
                continue
            if args.duplicate_strategy == "keep-last":
                staged_records[record_id] = candidate
                continue
            staged_records[record_id] = merge_rows(staged_records[record_id], candidate)

        connection.execute("BEGIN")

        for staged in staged_records.values():
            record_id = staged["record_id"]
            current = load_current_record(connection, record_id)
            changed_at = now_utc()
            payload = {
                "record_id": record_id,
                "ledger_name": ledger_name,
                "base_id": staged["base_id"],
                "entity_name": staged["entity_name"],
                "location": staged["location"],
                "ip": staged["ip"],
                "department": staged["department"],
                "owner": staged["owner"],
                "status": staged["status"],
                "transport": staged["transport"],
                "brand": staged["brand"],
                "model": staged["model"],
                "remark": staged["remark"],
                "extra_json": staged["extra_json"],
                "source_file": source_file,
                "source_type": source_type,
                "version_no": 1 if current is None else int(current["version_no"]) + 1,
                "updated_at": changed_at,
                "is_deleted": 0,
            }
            payload["content_hash"] = compute_content_hash(payload)

            if current is None:
                upsert_current_record(connection, payload)
                append_history(connection, payload, "import", changed_at)
                log_operation(
                    connection,
                    ledger_name,
                    "import",
                    record_id,
                    None,
                    payload,
                    source_file,
                    f"Imported from {source_file}; archived copy: {raw_copy}",
                    changed_at,
                )
                stats["imported"] += 1
                continue

            if current["content_hash"] == payload["content_hash"] and int(current["is_deleted"]) == 0:
                stats["unchanged"] += 1
                continue

            upsert_current_record(connection, payload)
            append_history(connection, payload, "reimport", changed_at)
            log_operation(
                connection,
                ledger_name,
                "reimport",
                record_id,
                current,
                payload,
                source_file,
                f"Reimported from {source_file}; archived copy: {raw_copy}",
                changed_at,
            )
            stats["updated"] += 1

        if args.sync_delete_missing:
            active_rows = fetch_current_records(
                connection,
                ledger_name=ledger_name,
                filters={"source_file": source_file},
                include_deleted=False,
            )
            current_ids = set(staged_records.keys())
            for current in active_rows:
                if current["record_id"] in current_ids:
                    continue
                changed_at = now_utc()
                deleted_payload = dict(current)
                deleted_payload["version_no"] = int(current["version_no"]) + 1
                deleted_payload["updated_at"] = changed_at
                deleted_payload["is_deleted"] = 1
                deleted_payload["content_hash"] = compute_content_hash(deleted_payload)
                upsert_current_record(connection, deleted_payload)
                append_history(connection, deleted_payload, "delete", changed_at)
                log_operation(
                    connection,
                    ledger_name,
                    "delete",
                    current["record_id"],
                    current,
                    deleted_payload,
                    source_file,
                    f"Soft-deleted because record disappeared from source {source_file}",
                    changed_at,
                )
                stats["soft_deleted"] += 1

        rebuild_ledger_semantics(connection)
        changed = stats["imported"] + stats["updated"] + stats["soft_deleted"] > 0
        if changed:
            vector_index_size, embedding_warning = maybe_rebuild_embedding_index(connection, require_provider=True)
        else:
            vector_index_size = count_active_current_records(connection, ledger_name=None)
            embedding_warning = None

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

    payload = {
        "ledger": ledger_name,
        "source_file": source_file,
        "source_type": source_type,
        "raw_copy": str(raw_copy),
        "sheet_filters": {
            "include": args.sheet,
            "include_regex": args.sheet_regex,
            "exclude": args.exclude_sheet,
            "exclude_regex": args.exclude_sheet_regex,
        },
        "mapped_columns": mapping,
        "extra_columns": extra_columns,
        "duplicate_strategy": args.duplicate_strategy,
        "vector_index_size": vector_index_size,
        **stats,
    }
    if embedding_warning:
        payload["embedding_warning"] = embedding_warning

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
