#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from typing import Any

from common import (
    CANONICAL_FIELDS,
    LedgerError,
    canonicalize_value,
    connect_db,
    count_current_records,
    fetch_current_records,
    fetch_history_records,
    initialize_database,
    normalize_ledger_name,
    print_json,
    semantic_search_records,
)

COUNT_KEYWORDS = (
    "how many",
    "count",
    "number of",
    "total",
    "数量",
    "多少",
    "几个",
    "几条",
    "总数",
)
DETAIL_KEYWORDS = (
    "list",
    "show",
    "which",
    "what",
    "detail",
    "details",
    "明细",
    "详细",
    "有哪些",
    "哪些",
    "列表",
    "清单",
    "列出",
    "查看",
)
TRANSPORT_HINTS = ("4g", "5g", "wifi", "wireless", "fiber", "dedicated", "专线", "光纤", "无线")
AUTO_MATCH_FIELDS = ("location", "department", "owner", "transport", "status", "entity_name")
EXACT_MATCH_FIELDS = {"status"}
STOP_TERMS = {
    "台账",
    "技能",
    "告诉我",
    "查询",
    "记录",
    "信息",
    "详细",
    "明细",
    "列表",
    "清单",
    "桩号",
    "摄像头",
    "设备",
    "点位",
    "的",
    "是",
    "了",
    "吗",
    "呢",
    "啊",
    "吧",
}


def build_exact_filters(args: argparse.Namespace) -> dict[str, str]:
    return {
        "record_id": args.record_id,
        "base_id": args.base_id,
        "ip": args.ip,
        "entity_name": args.entity_name,
        "location": args.location,
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


def normalize_match_text(text: str) -> str:
    return re.sub(r"\s+", "", canonicalize_value(text).lower())


def detect_intent(query: str) -> str:
    lowered = canonicalize_value(query).lower()
    if any(keyword in lowered for keyword in COUNT_KEYWORDS):
        return "count"
    if any(keyword in lowered for keyword in DETAIL_KEYWORDS):
        return "detail"
    if re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", lowered):
        return "detail"
    return "semantic"


def extract_query_filters(connection, ledger: str | None, query: str) -> tuple[dict[str, str], dict[str, str]]:
    exact_filters: dict[str, str] = {}
    contains_filters: dict[str, str] = {}
    normalized_query = normalize_match_text(query)

    ip_match = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", canonicalize_value(query))
    if ip_match:
        exact_filters["ip"] = ip_match.group(0)

    for field in AUTO_MATCH_FIELDS:
        if ledger is None:
            rows = connection.execute(
                f"""
                SELECT DISTINCT {field} AS value
                FROM records_current
                WHERE is_deleted = 0
                  AND {field} IS NOT NULL
                  AND TRIM({field}) <> ''
                ORDER BY LENGTH({field}) DESC
                LIMIT 300
                """
            ).fetchall()
        else:
            rows = connection.execute(
                f"""
                SELECT DISTINCT {field} AS value
                FROM records_current
                WHERE ledger_name = ? AND is_deleted = 0
                  AND {field} IS NOT NULL
                  AND TRIM({field}) <> ''
                ORDER BY LENGTH({field}) DESC
                LIMIT 300
                """,
                (ledger,),
            ).fetchall()

        for row in rows:
            value = canonicalize_value(row["value"])
            normalized_value = normalize_match_text(value)
            if len(normalized_value) < 2:
                continue
            if normalized_value in normalized_query:
                if field in EXACT_MATCH_FIELDS:
                    exact_filters[field] = value
                else:
                    contains_filters[field] = value
                break

    lowered_query = canonicalize_value(query).lower()
    if "transport" not in contains_filters:
        for hint in TRANSPORT_HINTS:
            if hint in lowered_query:
                contains_filters["transport"] = hint
                break

    return exact_filters, contains_filters


def extract_search_terms(query: str) -> list[str]:
    normalized = canonicalize_value(query).lower().strip()
    if not normalized:
        return []

    reduced = normalized
    for term in STOP_TERMS:
        reduced = reduced.replace(term, " ")

    english_terms = [token for token in re.findall(r"[a-z0-9_.:/-]+", reduced) if len(token) >= 2]
    chinese_terms = [ch for ch in re.findall(r"[\u4e00-\u9fff]", reduced) if ch not in {"的", "是", "了", "和"}]
    unique: list[str] = []
    for term in [*english_terms, *chinese_terms]:
        if term not in unique:
            unique.append(term)
    return unique


def lexical_fallback_records(connection, ledger: str | None, query: str, limit: int) -> list[dict[str, Any]]:
    terms = extract_search_terms(query)
    if not terms:
        return []

    rows = fetch_current_records(
        connection,
        ledger_name=ledger,
        include_deleted=False,
        limit=3000,
    )
    if not rows:
        return []

    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        text_parts = [canonicalize_value(row.get(field, "")) for field in CANONICAL_FIELDS]
        text_parts.append(canonicalize_value(row.get("record_id", "")))
        text_parts.append(canonicalize_value(row.get("base_id", "")))
        text_parts.append(canonicalize_value(row.get("source_file", "")))
        searchable_text = " ".join(text_parts).lower()
        score = sum(1 for term in terms if term and term in searchable_text)
        if score <= 0:
            continue
        candidate = dict(row)
        candidate["lexical_score"] = score
        scored_rows.append(candidate)

    scored_rows.sort(
        key=lambda item: (
            int(item.get("lexical_score", 0)),
            canonicalize_value(item.get("updated_at", "")),
        ),
        reverse=True,
    )
    return scored_rows[:limit]


def semantic_rows_are_low_confidence(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return True
    lexical_overlaps = [int(row.get("lexical_overlap", 0)) for row in rows]
    max_overlap = max(lexical_overlaps) if lexical_overlaps else 0
    max_score = max(float(row.get("score", 0.0)) for row in rows)
    return max_overlap <= 0 and max_score < 0.35


def answer_natural_language(connection, ledger: str | None, query: str, limit: int) -> dict[str, Any]:
    intent = detect_intent(query)
    exact_filters, contains_filters = extract_query_filters(connection, ledger, query)

    if intent == "count":
        count = count_current_records(
            connection,
            ledger_name=ledger,
            filters=exact_filters,
            contains_filters=contains_filters,
            include_deleted=False,
        )
        return {
            "type": "count",
            "query": query,
            "ledger": ledger or "all",
            "count": count,
            "filters": exact_filters,
            "contains_filters": contains_filters,
        }

    if intent == "detail" or exact_filters or contains_filters:
        rows = fetch_current_records(
            connection,
            ledger_name=ledger,
            filters=exact_filters,
            contains_filters=contains_filters,
            include_deleted=False,
            limit=limit,
        )
        return {
            "type": "detail",
            "query": query,
            "ledger": ledger or "all",
            "count": len(rows),
            "filters": exact_filters,
            "contains_filters": contains_filters,
            "records": rows,
        }

    try:
        rows = semantic_search_records(
            connection,
            query,
            top_k=min(limit, 20),
            ledger_name=ledger,
            filters=exact_filters,
            contains_filters=contains_filters,
        )
        if semantic_rows_are_low_confidence(rows):
            rows = lexical_fallback_records(connection, ledger, query, limit)
        return {
            "type": "semantic" if rows and "lexical_score" not in rows[0] else "lexical_fallback" if rows else "semantic_empty",
            "query": query,
            "ledger": ledger or "all",
            "count": len(rows),
            "filters": exact_filters,
            "contains_filters": contains_filters,
            "records": rows,
        }
    except LedgerError:
        rows = fetch_current_records(
            connection,
            ledger_name=ledger,
            contains_filters={"remark": canonicalize_value(query)},
            include_deleted=False,
            limit=limit,
        )
        if not rows:
            rows = lexical_fallback_records(connection, ledger, query, limit)
        return {
            "type": "fallback_contains" if rows else "fallback_empty",
            "query": query,
            "ledger": ledger or "all",
            "count": len(rows),
            "records": rows,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Query current/history ledger records and answer natural-language prompts.")
    parser.add_argument("--ledger", default="auto", help="Ledger scope: a ledger name, or auto/all/* for all ledgers.")
    parser.add_argument("--record-id")
    parser.add_argument("--base-id")
    parser.add_argument("--ip")
    parser.add_argument("--entity-name")
    parser.add_argument("--location")
    parser.add_argument("--department")
    parser.add_argument("--owner")
    parser.add_argument("--status")
    parser.add_argument("--transport")
    parser.add_argument("--source-file")

    parser.add_argument("--contains-name")
    parser.add_argument("--contains-location")
    parser.add_argument("--contains-remark")
    parser.add_argument("--contains-transport")

    parser.add_argument("--ask", help="Natural-language query, e.g. how many active records are in HQ")
    parser.add_argument("--count", action="store_true", help="Return count only for structured query.")
    parser.add_argument("--history", action="store_true", help="Query records_history instead of records_current.")
    parser.add_argument("--include-deleted", action="store_true", help="Include soft-deleted rows in current queries.")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    try:
        raw_ledger = canonicalize_value(args.ledger).lower()
        ledger_name: str | None = None if raw_ledger in {"", "auto", "all", "*"} else normalize_ledger_name(args.ledger)
        connection = connect_db()
        initialize_database(connection)

        if args.ask:
            payload = answer_natural_language(connection, ledger_name, args.ask, args.limit)
            if ledger_name == "default" and int(payload.get("count", 0)) == 0:
                default_count = count_current_records(connection, ledger_name="default", include_deleted=False)
                all_count = count_current_records(connection, ledger_name=None, include_deleted=False)
                if all_count > default_count:
                    fallback_payload = answer_natural_language(connection, None, args.ask, args.limit)
                    if int(fallback_payload.get("count", 0)) > 0:
                        fallback_payload["fallback_from_ledger"] = "default"
                        fallback_payload["scope"] = "all_ledgers_fallback"
                        payload = fallback_payload
            connection.close()
            print_json(payload)
            return 0

        exact_filters = build_exact_filters(args)
        contains_filters = build_contains_filters(args)

        if args.history:
            rows = fetch_history_records(
                connection,
                ledger_name=ledger_name,
                filters=exact_filters,
                contains_filters=contains_filters,
                limit=args.limit,
            )
            payload = {
                "scope": "history",
                "ledger": ledger_name or "all",
                "count": len(rows),
                "records": rows,
            }
        else:
            if args.count:
                count = count_current_records(
                    connection,
                    ledger_name=ledger_name,
                    filters=exact_filters,
                    contains_filters=contains_filters,
                    include_deleted=args.include_deleted,
                )
                payload = {
                    "scope": "current",
                    "ledger": ledger_name or "all",
                    "count": count,
                }
            else:
                rows = fetch_current_records(
                    connection,
                    ledger_name=ledger_name,
                    filters=exact_filters,
                    contains_filters=contains_filters,
                    include_deleted=args.include_deleted,
                    limit=args.limit,
                )
                payload = {
                    "scope": "current",
                    "ledger": ledger_name or "all",
                    "count": len(rows),
                    "records": rows,
                }

        connection.close()
    except LedgerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
