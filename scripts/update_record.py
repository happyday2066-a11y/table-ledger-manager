#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from common import (
    CANONICAL_FIELDS,
    LedgerError,
    append_history,
    canonicalize_value,
    compute_content_hash,
    connect_db,
    initialize_database,
    log_operation,
    maybe_rebuild_embedding_index,
    normalize_ledger_name,
    now_utc,
    print_json,
    resolve_target_record,
    safe_json_loads,
    upsert_current_record,
)


def parse_assignments(raw_items: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    canonical_updates: dict[str, str] = {}
    extra_updates: dict[str, str] = {}
    for item in raw_items:
        if "=" not in item:
            raise LedgerError(f"Invalid assignment '{item}'. Expected field=value.")
        field, value = item.split("=", 1)
        field = field.strip()
        value = value.strip()
        if field in CANONICAL_FIELDS:
            canonical_updates[field] = value
            continue
        if field.startswith("extra."):
            key = field.split(".", 1)[1].strip()
            if not key:
                raise LedgerError(f"Invalid extra field assignment '{item}'.")
            extra_updates[key] = value
            continue
        raise LedgerError(f"Unsupported update field: {field}")
    return canonical_updates, extra_updates


def main() -> int:
    parser = argparse.ArgumentParser(description="Update, soft-delete, or restore a current ledger record.")
    parser.add_argument("--ledger", default="default")
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--record-id")
    target_group.add_argument("--base-id")
    target_group.add_argument("--ip")
    parser.add_argument("--set", dest="assignments", action="append", default=[], help="Repeatable field=value updates.")
    parser.add_argument("--delete", action="store_true", help="Soft-delete the target record.")
    parser.add_argument("--restore", action="store_true", help="Restore a soft-deleted target record.")
    parser.add_argument("--message", default="", help="Audit message stored in operations_log.")
    parser.add_argument("--source-file", default="manual-update", help="Source label written to the record and logs.")
    args = parser.parse_args()

    if args.delete and args.restore:
        print("ERROR: --delete and --restore cannot be used together.", file=sys.stderr)
        return 1

    try:
        ledger_name = normalize_ledger_name(args.ledger)
        canonical_updates, extra_updates = parse_assignments(args.assignments)
        if not canonical_updates and not extra_updates and not args.delete and not args.restore:
            raise LedgerError("No changes requested. Use --set, --delete, or --restore.")

        connection = connect_db()
        initialize_database(connection)
        current = resolve_target_record(
            connection,
            ledger_name=ledger_name,
            record_id=args.record_id,
            base_id=args.base_id,
            ip=args.ip,
        )

        after = dict(current)
        after.update(canonical_updates)

        extras = safe_json_loads(current.get("extra_json"))
        extras.update(extra_updates)
        after["extra_json"] = json.dumps(extras, ensure_ascii=False, sort_keys=True) if extras else "{}"

        if args.delete:
            after["is_deleted"] = 1
        elif args.restore:
            after["is_deleted"] = 0
        else:
            after["is_deleted"] = int(current["is_deleted"])

        after["ledger_name"] = ledger_name
        after["source_file"] = args.source_file
        after["source_type"] = "manual"
        after["updated_at"] = now_utc()
        after["version_no"] = int(current["version_no"]) + 1
        after["content_hash"] = compute_content_hash(after)

        if after["content_hash"] == current["content_hash"] and int(after["is_deleted"]) == int(current["is_deleted"]):
            raise LedgerError("The requested update does not change the record.")

        if args.delete and int(current["is_deleted"]) == 1:
            raise LedgerError("The target record is already soft-deleted.")
        if args.restore and int(current["is_deleted"]) == 0:
            raise LedgerError("The target record is not soft-deleted.")

        change_type = "delete" if args.delete else "update"
        operation_type = "delete" if args.delete else "restore" if args.restore else "update"

        connection.execute("BEGIN")
        upsert_current_record(connection, after)
        append_history(connection, after, change_type, after["updated_at"])
        log_operation(
            connection,
            ledger_name,
            operation_type,
            current["record_id"],
            current,
            after,
            args.source_file,
            args.message or f"{operation_type} via update_record.py",
            after["updated_at"],
        )
        vector_index_size, embedding_warning = maybe_rebuild_embedding_index(connection, require_provider=True)
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
        "updated_record_id": current["record_id"],
        "operation": operation_type,
        "vector_index_size": vector_index_size,
    }
    if embedding_warning:
        payload["embedding_warning"] = embedding_warning

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
