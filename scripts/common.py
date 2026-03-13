#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import shutil
import sqlite3
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

SKILL_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LEDGER = "default"
RECORD_ID_SEPARATOR = "::"
CANONICAL_FIELDS = (
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
)
IMPORT_FIELDS = ("record_id", "base_id", *CANONICAL_FIELDS, "extra_json")
QUERYABLE_FIELDS = (
    "record_id",
    "base_id",
    *CANONICAL_FIELDS,
    "ledger_name",
    "source_file",
    "source_type",
)
LIKE_FILTER_FIELDS = ("entity_name", "location", "ip", "department", "owner", "status", "transport", "remark")
SOURCE_TYPE_MAP = {
    ".csv": "csv",
    ".xlsx": "xlsx",
    ".xls": "xlsx",
    ".md": "markdown",
}
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_.:/-]+|[\u4e00-\u9fff]")
MARKDOWN_DELIMITER_PATTERN = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$")
FULL_IPV4_PATTERN = re.compile(r"(?<!\d)(\d{1,3}(?:\.\d{1,3}){3})(?!\d)")
BRACKETED_NOTE_PATTERN = re.compile(r"[（(][^）)]*[）)]")
AUTO_ADDRESS_TOKEN_PATTERN = re.compile(r"(自动获取|无线获取|dhcp|自动分配|自动|获取|auto|wireless)", re.IGNORECASE)
QUANTITY_PATTERN = re.compile(r"(?<!\d)(\d+)(?!\d)")

FIELD_ALIASES = {
    "recordid": "record_id",
    "id": "record_id",
    "record": "record_id",
    "recordno": "record_id",
    "recordnumber": "record_id",
    "序号": "record_id",
    "编号": "record_id",
    "记录编号": "record_id",
    "记录id": "record_id",
    "条目编号": "record_id",
    "条目id": "record_id",
    "baseid": "base_id",
    "base_id": "base_id",
    "businessid": "base_id",
    "业务id": "base_id",
    "业务编号": "base_id",
    "原始id": "base_id",
    "原始编号": "base_id",
    "name": "entity_name",
    "fullname": "entity_name",
    "displayname": "entity_name",
    "entityname": "entity_name",
    "entity": "entity_name",
    "asset": "entity_name",
    "assetname": "entity_name",
    "device": "entity_name",
    "devicename": "entity_name",
    "hostname": "entity_name",
    "contact": "entity_name",
    "person": "entity_name",
    "unit": "entity_name",
    "title": "entity_name",
    "名称": "entity_name",
    "姓名": "entity_name",
    "名字": "entity_name",
    "设备名称": "entity_name",
    "资产名称": "entity_name",
    "条目名称": "entity_name",
    "联系人": "entity_name",
    "人员": "entity_name",
    "主机名": "entity_name",
    "location": "location",
    "site": "location",
    "loc": "location",
    "position": "location",
    "region": "location",
    "zone": "location",
    "office": "location",
    "地点": "location",
    "位置": "location",
    "地址": "location",
    "存放地点": "location",
    "区域": "location",
    "站点": "location",
    "工区": "location",
    "办公点": "location",
    "现场": "location",
    "场所": "location",
    "库位": "location",
    "机房": "location",
    "机柜": "location",
    "楼层": "location",
    "房间": "location",
    "坝号": "location",
    "桩号": "location",
    "路段": "location",
    "门店": "location",
    "仓库": "location",
    "仓位": "location",
    "园区": "location",
    "片区": "location",
    "分区": "location",
    "坐标": "location",
    "经纬度": "location",
    "地理位置": "location",
    "地理坐标": "location",
    "机位": "location",
    "点位": "location",
    "address": "location",
    "ip": "ip",
    "ipv4": "ip",
    "ipv6": "ip",
    "管理ip": "ip",
    "ip地址": "ip",
    "网络地址": "ip",
    "ipaddress": "ip",
    "ipaddr": "ip",
    "department": "department",
    "dept": "department",
    "team": "department",
    "group": "department",
    "org": "department",
    "organization": "department",
    "组织": "department",
    "部门": "department",
    "科室": "department",
    "单位": "department",
    "owner": "owner",
    "responsible": "owner",
    "assignee": "owner",
    "manager": "owner",
    "maintainer": "owner",
    "负责人": "owner",
    "责任人": "owner",
    "归属人": "owner",
    "所有人": "owner",
    "联系人姓名": "owner",
    "使用人": "owner",
    "持有人": "owner",
    "领用人": "owner",
    "status": "status",
    "state": "status",
    "enabled": "status",
    "active": "status",
    "运行状态": "status",
    "状态": "status",
    "是否启用": "status",
    "使用状况": "status",
    "transport": "transport",
    "network": "transport",
    "link": "transport",
    "channel": "transport",
    "carrier": "transport",
    "transmission": "transport",
    "传输": "transport",
    "传输方式": "transport",
    "网络": "transport",
    "链路": "transport",
    "通道": "transport",
    "brand": "brand",
    "vendor": "brand",
    "maker": "brand",
    "品牌": "brand",
    "厂商": "brand",
    "model": "model",
    "type": "model",
    "型号": "model",
    "机型": "model",
    "规格": "model",
    "remark": "remark",
    "comment": "remark",
    "note": "remark",
    "memo": "remark",
    "description": "remark",
    "备注": "remark",
    "说明": "remark",
    "描述": "remark",
    "注释": "remark",
}

EXPORT_LABEL_ALIASES = {
    "record_id": "record_id",
    "base_id": "base_id",
    "name": "entity_name",
    "fullname": "entity_name",
    "entity_name": "entity_name",
    "entity": "entity_name",
    "asset": "entity_name",
    "device": "entity_name",
    "contact": "entity_name",
    "名称": "entity_name",
    "姓名": "entity_name",
    "设备名称": "entity_name",
    "资产名称": "entity_name",
    "联系人": "entity_name",
    "location": "location",
    "site": "location",
    "address": "location",
    "地点": "location",
    "位置": "location",
    "地址": "location",
    "存放地点": "location",
    "ip": "ip",
    "ipaddress": "ip",
    "ip地址": "ip",
    "管理ip": "ip",
    "department": "department",
    "dept": "department",
    "部门": "department",
    "组织": "department",
    "owner": "owner",
    "负责人": "owner",
    "责任人": "owner",
    "使用人": "owner",
    "status": "status",
    "state": "status",
    "状态": "status",
    "使用状况": "status",
    "transport": "transport",
    "network": "transport",
    "传输": "transport",
    "传输方式": "transport",
    "brand": "brand",
    "vendor": "brand",
    "品牌": "brand",
    "model": "model",
    "type": "model",
    "型号": "model",
    "remark": "remark",
    "note": "remark",
    "备注": "remark",
}


class LedgerError(RuntimeError):
    pass


@dataclass
class EmbeddingSettings:
    provider: str
    base_url: str | None
    api_key: str | None
    model: str | None


def default_data_root() -> Path:
    # Keep runtime state outside the skill folder so reinstalling/upgrading the skill does not erase data.
    return (Path.home() / ".table-ledger-manager" / "data").resolve()


def legacy_data_root() -> Path:
    return (SKILL_ROOT / "data").resolve()


def data_root() -> Path:
    override = os.getenv("LEDGER_DATA_DIR")
    return Path(override).expanduser().resolve() if override else default_data_root()


def raw_dir() -> Path:
    return data_root() / "raw"


def logs_dir() -> Path:
    return data_root() / "logs"


def exports_dir() -> Path:
    return data_root() / "exports"


def db_path() -> Path:
    override = os.getenv("LEDGER_DB_PATH")
    return Path(override).expanduser().resolve() if override else (data_root() / "db" / "ledger.db").resolve()


def index_path() -> Path:
    override = os.getenv("LEDGER_INDEX_PATH")
    return Path(override).expanduser().resolve() if override else (data_root() / "index" / "current_embeddings.npz").resolve()


def maybe_migrate_legacy_data() -> None:
    if os.getenv("LEDGER_DATA_DIR"):
        return

    destination = data_root()
    legacy = legacy_data_root()
    if destination == legacy or destination.exists() or not legacy.exists():
        return

    for relative in (Path("db"), Path("index"), Path("raw"), Path("logs"), Path("exports")):
        source_path = legacy / relative
        if source_path.exists():
            shutil.copytree(source_path, destination / relative, dirs_exist_ok=True)


def ensure_runtime_paths() -> None:
    maybe_migrate_legacy_data()
    for path in (data_root(), raw_dir(), logs_dir(), exports_dir(), db_path().parent, index_path().parent):
        path.mkdir(parents=True, exist_ok=True)


def connect_db() -> sqlite3.Connection:
    ensure_runtime_paths()
    connection = sqlite3.connect(db_path())
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    if column_name not in table_columns(connection, table_name):
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS records_current (
            record_id TEXT PRIMARY KEY,
            ledger_name TEXT NOT NULL DEFAULT 'default',
            base_id TEXT NOT NULL DEFAULT '',
            entity_name TEXT,
            location TEXT,
            ip TEXT,
            department TEXT,
            owner TEXT,
            status TEXT,
            transport TEXT,
            brand TEXT,
            model TEXT,
            remark TEXT,
            extra_json TEXT NOT NULL DEFAULT '{}',
            source_file TEXT,
            source_type TEXT,
            version_no INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS records_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id TEXT NOT NULL,
            ledger_name TEXT NOT NULL DEFAULT 'default',
            base_id TEXT NOT NULL DEFAULT '',
            entity_name TEXT,
            location TEXT,
            ip TEXT,
            department TEXT,
            owner TEXT,
            status TEXT,
            transport TEXT,
            brand TEXT,
            model TEXT,
            remark TEXT,
            extra_json TEXT NOT NULL DEFAULT '{}',
            source_file TEXT,
            source_type TEXT,
            version_no INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            change_type TEXT NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS operations_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ledger_name TEXT NOT NULL DEFAULT 'default',
            operation_type TEXT NOT NULL,
            target_record_id TEXT,
            before_json TEXT,
            after_json TEXT,
            source_file TEXT,
            message TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS embeddings_meta (
            record_id TEXT PRIMARY KEY,
            ledger_name TEXT NOT NULL DEFAULT 'default',
            version_no INTEGER NOT NULL,
            is_current INTEGER NOT NULL,
            embedding_text TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ledger_semantics (
            ledger_name TEXT PRIMARY KEY,
            family_name TEXT NOT NULL,
            dominant_type TEXT NOT NULL DEFAULT 'generic',
            row_count INTEGER NOT NULL DEFAULT 0,
            profile_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ledger_family_links (
            ledger_name TEXT NOT NULL,
            related_ledger_name TEXT NOT NULL,
            relation_type TEXT NOT NULL,
            shared_base_count INTEGER NOT NULL DEFAULT 0,
            shared_hash_count INTEGER NOT NULL DEFAULT 0,
            overlap_base REAL NOT NULL DEFAULT 0,
            overlap_hash REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (ledger_name, related_ledger_name)
        );
        """
    )
    ensure_column(connection, "records_current", "ledger_name", "TEXT NOT NULL DEFAULT 'default'")
    ensure_column(connection, "records_current", "base_id", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "records_current", "entity_name", "TEXT")
    ensure_column(connection, "records_current", "location", "TEXT")
    ensure_column(connection, "records_current", "transport", "TEXT")
    ensure_column(connection, "records_current", "brand", "TEXT")
    ensure_column(connection, "records_current", "model", "TEXT")
    ensure_column(connection, "records_current", "extra_json", "TEXT NOT NULL DEFAULT '{}'")

    ensure_column(connection, "records_history", "ledger_name", "TEXT NOT NULL DEFAULT 'default'")
    ensure_column(connection, "records_history", "base_id", "TEXT NOT NULL DEFAULT ''")
    ensure_column(connection, "records_history", "entity_name", "TEXT")
    ensure_column(connection, "records_history", "location", "TEXT")
    ensure_column(connection, "records_history", "transport", "TEXT")
    ensure_column(connection, "records_history", "brand", "TEXT")
    ensure_column(connection, "records_history", "model", "TEXT")
    ensure_column(connection, "records_history", "extra_json", "TEXT NOT NULL DEFAULT '{}'")

    ensure_column(connection, "operations_log", "ledger_name", "TEXT NOT NULL DEFAULT 'default'")
    ensure_column(connection, "embeddings_meta", "ledger_name", "TEXT NOT NULL DEFAULT 'default'")
    ensure_column(connection, "ledger_semantics", "family_name", "TEXT NOT NULL DEFAULT 'default'")
    ensure_column(connection, "ledger_semantics", "dominant_type", "TEXT NOT NULL DEFAULT 'generic'")
    ensure_column(connection, "ledger_semantics", "row_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(connection, "ledger_semantics", "profile_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_column(connection, "ledger_semantics", "updated_at", "TEXT NOT NULL DEFAULT ''")

    connection.executescript(
        """
        UPDATE records_current SET ledger_name = 'default' WHERE ledger_name IS NULL OR TRIM(ledger_name) = '';
        UPDATE records_current SET base_id = record_id WHERE base_id IS NULL OR TRIM(base_id) = '';
        UPDATE records_current SET extra_json = '{}' WHERE extra_json IS NULL OR TRIM(extra_json) = '';

        UPDATE records_history SET ledger_name = 'default' WHERE ledger_name IS NULL OR TRIM(ledger_name) = '';
        UPDATE records_history SET base_id = record_id WHERE base_id IS NULL OR TRIM(base_id) = '';
        UPDATE records_history SET extra_json = '{}' WHERE extra_json IS NULL OR TRIM(extra_json) = '';

        UPDATE operations_log SET ledger_name = 'default' WHERE ledger_name IS NULL OR TRIM(ledger_name) = '';
        UPDATE embeddings_meta SET ledger_name = 'default' WHERE ledger_name IS NULL OR TRIM(ledger_name) = '';
        UPDATE ledger_semantics SET family_name = ledger_name WHERE family_name IS NULL OR TRIM(family_name) = '';
        UPDATE ledger_semantics SET dominant_type = 'generic' WHERE dominant_type IS NULL OR TRIM(dominant_type) = '';
        UPDATE ledger_semantics SET profile_json = '{}' WHERE profile_json IS NULL OR TRIM(profile_json) = '';
        """
    )

    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_records_current_ledger ON records_current (ledger_name);
        CREATE INDEX IF NOT EXISTS idx_records_current_ledger_ip ON records_current (ledger_name, ip);
        CREATE INDEX IF NOT EXISTS idx_records_current_ledger_location ON records_current (ledger_name, location);
        CREATE INDEX IF NOT EXISTS idx_records_current_ledger_name ON records_current (ledger_name, entity_name);
        CREATE INDEX IF NOT EXISTS idx_records_current_ledger_owner ON records_current (ledger_name, owner);
        CREATE INDEX IF NOT EXISTS idx_records_current_ledger_status ON records_current (ledger_name, status);
        CREATE INDEX IF NOT EXISTS idx_records_history_ledger_record ON records_history (ledger_name, record_id);
        CREATE INDEX IF NOT EXISTS idx_operations_log_ledger_target ON operations_log (ledger_name, target_record_id);
        CREATE INDEX IF NOT EXISTS idx_ledger_semantics_family ON ledger_semantics (family_name);
        CREATE INDEX IF NOT EXISTS idx_ledger_family_links_related ON ledger_family_links (related_ledger_name);
        """
    )

    connection.commit()


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonicalize_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _is_valid_ipv4(candidate: str) -> bool:
    try:
        ipaddress.ip_address(candidate)
    except ValueError:
        return False
    return True


def extract_ipv4_values(value: Any, dominant_prefix: str | None = None) -> list[str]:
    text = canonicalize_value(value)
    if not text:
        return []

    results: list[str] = []
    for candidate in FULL_IPV4_PATTERN.findall(text):
        if _is_valid_ipv4(candidate) and candidate not in results:
            results.append(candidate)
    if results:
        return results

    if not dominant_prefix:
        return []

    cleaned = BRACKETED_NOTE_PATTERN.sub("", text)
    cleaned = AUTO_ADDRESS_TOKEN_PATTERN.sub("", cleaned)
    cleaned = cleaned.replace("：", ":")
    segments = re.split(r"[、，,;/；]+", cleaned)
    for segment in segments:
        compact = canonicalize_value(segment).strip(": ")
        if not re.fullmatch(r"\d{1,3}", compact):
            continue
        suffix = int(compact)
        if suffix > 255:
            continue
        candidate = f"{dominant_prefix}.{suffix}"
        if _is_valid_ipv4(candidate) and candidate not in results:
            results.append(candidate)
    return results


def infer_dominant_ipv4_prefix(values: Iterable[Any]) -> str:
    counter: Counter[str] = Counter()
    for value in values:
        for candidate in extract_ipv4_values(value):
            parts = candidate.split(".")
            counter[".".join(parts[:3])] += 1
    if not counter:
        return ""
    return counter.most_common(1)[0][0]


def extract_quantity_values(value: Any) -> list[int]:
    text = canonicalize_value(value)
    if not text:
        return []
    values: list[int] = []
    for token in QUANTITY_PATTERN.findall(text):
        try:
            number = int(token)
        except ValueError:
            continue
        values.append(number)
    return values


def normalize_header(header: Any) -> str:
    text = canonicalize_value(header).lstrip("\ufeff").lower()
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"[\s_\-:/()\[\]{}]+", "", text)
    return text


def normalize_ledger_name(ledger_name: str | None) -> str:
    raw = canonicalize_value(ledger_name) or DEFAULT_LEDGER
    raw = raw.replace("/", "-").replace("\\", "-")
    raw = re.sub(r"\s+", "-", raw)
    raw = re.sub(r"-+", "-", raw).strip("-")
    return raw.lower() or DEFAULT_LEDGER


def sanitize_base_id(base_id: str | None) -> str:
    raw = canonicalize_value(base_id)
    raw = raw.replace(RECORD_ID_SEPARATOR, ":")
    raw = re.sub(r"\s+", "-", raw)
    return raw.strip("-")


def compose_record_id(ledger_name: str, base_id: str) -> str:
    return f"{normalize_ledger_name(ledger_name)}{RECORD_ID_SEPARATOR}{sanitize_base_id(base_id)}"


def split_record_id(record_id: str) -> tuple[str, str]:
    value = canonicalize_value(record_id)
    if RECORD_ID_SEPARATOR not in value:
        return DEFAULT_LEDGER, value
    ledger_name, base_id = value.split(RECORD_ID_SEPARATOR, 1)
    return normalize_ledger_name(ledger_name), base_id


def resolve_record_id_input(record_id: str, ledger_name: str) -> str:
    value = canonicalize_value(record_id)
    if not value:
        raise LedgerError("record_id cannot be empty.")
    if RECORD_ID_SEPARATOR in value:
        return value
    return compose_record_id(ledger_name, value)


def infer_source_type(file_path: str | Path) -> str:
    suffix = Path(file_path).suffix.lower()
    if suffix not in SOURCE_TYPE_MAP:
        raise LedgerError(f"Unsupported file type: {suffix or '<none>'}")
    return SOURCE_TYPE_MAP[suffix]


def source_file_path(file_path: str | Path) -> Path:
    return Path(file_path).expanduser().resolve()


def archive_raw_file(file_path: str | Path) -> Path:
    source = source_file_path(file_path)
    if not source.is_file():
        raise LedgerError(f"Input file does not exist: {source}")
    ensure_runtime_paths()
    stamped_name = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{source.name}"
    destination = raw_dir() / stamped_name
    shutil.copy2(source, destination)
    return destination


def deduplicate_headers(headers: Sequence[Any]) -> list[str]:
    counts: dict[str, int] = {}
    output: list[str] = []
    for index, header in enumerate(headers, start=1):
        label = canonicalize_value(header) or f"column_{index}"
        seen = counts.get(label, 0) + 1
        counts[label] = seen
        output.append(label if seen == 1 else f"{label}__{seen}")
    return output


def header_score(headers: Sequence[Any]) -> int:
    normalized = [normalize_header(cell) for cell in headers if canonicalize_value(cell)]
    if not normalized:
        return 0
    return sum(1 for cell in normalized if cell in FIELD_ALIASES)


def detect_header_row(raw_dataframe: pd.DataFrame, scan_rows: int = 40) -> tuple[int, int]:
    best_index = 0
    best_score = -1
    max_rows = min(scan_rows, len(raw_dataframe.index))
    for row_index in range(max_rows):
        score = header_score(raw_dataframe.iloc[row_index].tolist())
        if score > best_score:
            best_index = row_index
            best_score = score
    return best_index, max(best_score, 0)


def parse_excel_sheet(raw_dataframe: pd.DataFrame, sheet_name: str) -> pd.DataFrame | None:
    if raw_dataframe.empty:
        return None
    header_index, score = detect_header_row(raw_dataframe)
    if score < 2:
        return None
    headers = deduplicate_headers(raw_dataframe.iloc[header_index].tolist())
    table = raw_dataframe.iloc[header_index + 1 :].copy()
    table.columns = headers
    table = table.fillna("")
    table = table.loc[table.apply(lambda row: any(canonicalize_value(value) for value in row), axis=1)]
    if table.empty:
        return None
    table["__sheet_name__"] = sheet_name
    return table.reset_index(drop=True)


def should_include_sheet(
    sheet_name: str,
    include_sheets: set[str] | None = None,
    include_sheet_regex: str | None = None,
    exclude_sheets: set[str] | None = None,
    exclude_sheet_regex: str | None = None,
) -> bool:
    normalized_name = canonicalize_value(sheet_name).strip()
    normalized_lower = normalized_name.lower()

    if include_sheets is not None and normalized_lower not in include_sheets:
        return False
    if include_sheet_regex and not re.search(include_sheet_regex, normalized_name, flags=re.IGNORECASE):
        return False
    if exclude_sheets is not None and normalized_lower in exclude_sheets:
        return False
    if exclude_sheet_regex and re.search(exclude_sheet_regex, normalized_name, flags=re.IGNORECASE):
        return False
    return True


def normalize_sheet_selection(sheet_names: Sequence[str] | None) -> set[str] | None:
    if not sheet_names:
        return None
    normalized = {canonicalize_value(name).strip().lower() for name in sheet_names if canonicalize_value(name)}
    return normalized or None


def load_excel_table(
    file_path: Path,
    include_sheets: Sequence[str] | None = None,
    include_sheet_regex: str | None = None,
    exclude_sheets: Sequence[str] | None = None,
    exclude_sheet_regex: str | None = None,
) -> pd.DataFrame:
    workbook = pd.ExcelFile(file_path)
    include_set = normalize_sheet_selection(include_sheets)
    exclude_set = normalize_sheet_selection(exclude_sheets)
    candidate_sheets = [
        name
        for name in workbook.sheet_names
        if should_include_sheet(
            name,
            include_sheets=include_set,
            include_sheet_regex=include_sheet_regex,
            exclude_sheets=exclude_set,
            exclude_sheet_regex=exclude_sheet_regex,
        )
    ]

    if not candidate_sheets:
        raise LedgerError("No Excel sheets matched the provided sheet filters.")

    tables: list[pd.DataFrame] = []
    for sheet_name in candidate_sheets:
        raw_sheet = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str).fillna("")
        parsed = parse_excel_sheet(raw_sheet, sheet_name)
        if parsed is not None and not parsed.empty:
            tables.append(parsed)
    if tables:
        return pd.concat(tables, ignore_index=True, sort=False).fillna("")

    fallback_sheet = candidate_sheets[0]
    fallback = pd.read_excel(file_path, sheet_name=fallback_sheet, dtype=str).fillna("")
    if fallback.empty:
        return fallback
    fallback["__sheet_name__"] = fallback_sheet
    return fallback


def load_input_dataframe(
    file_path: str | Path,
    include_sheets: Sequence[str] | None = None,
    include_sheet_regex: str | None = None,
    exclude_sheets: Sequence[str] | None = None,
    exclude_sheet_regex: str | None = None,
) -> pd.DataFrame:
    source = source_file_path(file_path)
    if not source.is_file():
        raise LedgerError(f"Input file does not exist: {source}")
    source_type = infer_source_type(source)
    if source_type == "csv":
        if include_sheets or include_sheet_regex or exclude_sheets or exclude_sheet_regex:
            raise LedgerError("Sheet filters are only supported for Excel (.xlsx/.xls) inputs.")
        dataframe = pd.read_csv(source, dtype=str, encoding="utf-8-sig").fillna("")
    elif source_type == "xlsx":
        dataframe = load_excel_table(
            source,
            include_sheets=include_sheets,
            include_sheet_regex=include_sheet_regex,
            exclude_sheets=exclude_sheets,
            exclude_sheet_regex=exclude_sheet_regex,
        )
    else:
        if include_sheets or include_sheet_regex or exclude_sheets or exclude_sheet_regex:
            raise LedgerError("Sheet filters are only supported for Excel (.xlsx/.xls) inputs.")
        from parse_markdown_table import parse_markdown_table_file

        dataframe = parse_markdown_table_file(source)
    if dataframe.empty:
        raise LedgerError("Input table contains no rows.")
    return dataframe


def map_columns(columns: Iterable[Any]) -> tuple[dict[str, str], list[str]]:
    mapping: dict[str, str] = {}
    mapped_targets: dict[str, str] = {}
    extra_columns: list[str] = []
    for column in columns:
        column_name = str(column)
        if column_name.startswith("__"):
            extra_columns.append(column_name)
            continue
        normalized = normalize_header(column)
        target = FIELD_ALIASES.get(normalized)
        if target is None:
            extra_columns.append(column_name)
            continue
        if target in mapped_targets:
            extra_columns.append(column_name)
            continue
        mapping[column_name] = target
        mapped_targets[target] = column_name

    recognized = set(mapping.values()) - {"record_id", "base_id"}
    if not recognized.intersection({"entity_name", "ip", "location", "owner", "department"}):
        raise LedgerError(
            "Field mapping failed: at least one of entity_name/ip/location/owner/department must be recognized."
        )
    return mapping, extra_columns


def safe_json_loads(payload: str | None) -> dict[str, Any]:
    raw = canonicalize_value(payload)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def is_summary_row(record: dict[str, Any]) -> bool:
    marker = normalize_header(record.get("entity_name"))
    if marker not in {"total", "subtotal", "summary", "合计", "总计", "小计", "汇总"}:
        return False
    return not any(canonicalize_value(record.get(field)) for field in ("ip", "location", "owner", "department"))


def standardize_dataframe(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str], list[str]]:
    mapping, extra_columns = map_columns(dataframe.columns)
    standardized_rows: list[dict[str, Any]] = []
    raw_rows = dataframe.to_dict(orient="records")
    for row in raw_rows:
        standardized = {field: "" for field in IMPORT_FIELDS}
        for source_column, target_column in mapping.items():
            standardized[target_column] = canonicalize_value(row.get(source_column, ""))

        extras: dict[str, str] = {}
        for column_name in extra_columns:
            value = canonicalize_value(row.get(column_name, ""))
            if value:
                label = canonicalize_value(column_name) or column_name
                extras[label] = value
                promoted_field = canonical_field_from_label(label)
                if promoted_field in CANONICAL_FIELDS and not standardized.get(promoted_field):
                    standardized[promoted_field] = value
        standardized["extra_json"] = json.dumps(extras, ensure_ascii=False, sort_keys=True) if extras else "{}"

        if not any(canonicalize_value(standardized[field]) for field in CANONICAL_FIELDS) and standardized["extra_json"] == "{}":
            continue
        if is_summary_row(standardized):
            continue

        standardized_rows.append(standardized)

    if not standardized_rows:
        raise LedgerError("Input table contains no usable rows after standardization.")

    standardized_frame = pd.DataFrame(standardized_rows)
    for field in IMPORT_FIELDS:
        if field not in standardized_frame:
            standardized_frame[field] = ""
    standardized_frame = standardized_frame.loc[:, IMPORT_FIELDS].reset_index(drop=True)
    return standardized_frame, mapping, extra_columns


def make_auto_base_id(source_file: str | Path, row_number: int) -> str:
    stem = re.sub(r"[^a-z0-9]+", "-", source_file_path(source_file).stem.lower()).strip("-") or "ledger"
    return f"auto-{stem}-{row_number}"


def determine_base_id(record: dict[str, Any], source_file: str | Path, row_number: int) -> str:
    explicit = sanitize_base_id(record.get("base_id") or record.get("record_id"))
    if explicit:
        return explicit

    ip_value = sanitize_base_id(record.get("ip"))
    if ip_value:
        return ip_value

    entity = sanitize_base_id(record.get("entity_name"))
    location = sanitize_base_id(record.get("location"))
    owner = sanitize_base_id(record.get("owner"))

    if entity and location:
        return f"{entity}-{location}"
    if entity:
        return entity
    if owner and location:
        return f"{owner}-{location}"

    return make_auto_base_id(source_file, row_number)


def determine_record_identity(
    record: dict[str, Any],
    ledger_name: str,
    source_file: str | Path,
    row_number: int,
) -> tuple[str, str]:
    explicit_record_id = canonicalize_value(record.get("record_id"))
    if explicit_record_id:
        if RECORD_ID_SEPARATOR in explicit_record_id:
            _, explicit_base = split_record_id(explicit_record_id)
            return explicit_record_id, sanitize_base_id(explicit_base) or make_auto_base_id(source_file, row_number)
        base_id = sanitize_base_id(explicit_record_id)
    else:
        base_id = determine_base_id(record, source_file, row_number)

    if not base_id:
        base_id = make_auto_base_id(source_file, row_number)
    return compose_record_id(ledger_name, base_id), base_id


def compute_content_hash(record: dict[str, Any]) -> str:
    payload = {field: canonicalize_value(record.get(field, "")) for field in CANONICAL_FIELDS}
    payload["base_id"] = canonicalize_value(record.get("base_id", ""))
    payload["extra_json"] = canonicalize_value(record.get("extra_json", "{}"))
    payload["is_deleted"] = int(record.get("is_deleted", 0))
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_embedding_text(record: dict[str, Any]) -> str:
    extras = safe_json_loads(record.get("extra_json"))
    extra_preview = " | ".join(f"{key}:{value}" for key, value in list(extras.items())[:6]) or "none"

    def render(field: str) -> str:
        value = canonicalize_value(record.get(field, ""))
        return value or "none"

    return "\n".join(
        [
            f"ledger: {render('ledger_name')}",
            f"record_id: {render('record_id')}",
            f"base_id: {render('base_id')}",
            f"name: {render('entity_name')}",
            f"location: {render('location')}",
            f"ip: {render('ip')}",
            f"department: {render('department')}",
            f"owner: {render('owner')}",
            f"status: {render('status')}",
            f"transport: {render('transport')}",
            f"brand: {render('brand')}",
            f"model: {render('model')}",
            f"remark: {render('remark')}",
            f"extra: {extra_preview}",
            f"updated_at: {render('updated_at')}",
        ]
    )


def row_to_dict(row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def upsert_current_record(connection: sqlite3.Connection, record: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO records_current (
            record_id, ledger_name, base_id,
            entity_name, location, ip, department, owner, status, transport, brand, model, remark,
            extra_json, source_file, source_type, version_no, content_hash, updated_at, is_deleted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(record_id) DO UPDATE SET
            ledger_name = excluded.ledger_name,
            base_id = excluded.base_id,
            entity_name = excluded.entity_name,
            location = excluded.location,
            ip = excluded.ip,
            department = excluded.department,
            owner = excluded.owner,
            status = excluded.status,
            transport = excluded.transport,
            brand = excluded.brand,
            model = excluded.model,
            remark = excluded.remark,
            extra_json = excluded.extra_json,
            source_file = excluded.source_file,
            source_type = excluded.source_type,
            version_no = excluded.version_no,
            content_hash = excluded.content_hash,
            updated_at = excluded.updated_at,
            is_deleted = excluded.is_deleted
        """,
        (
            record["record_id"],
            normalize_ledger_name(record["ledger_name"]),
            record["base_id"],
            record["entity_name"],
            record["location"],
            record["ip"],
            record["department"],
            record["owner"],
            record["status"],
            record["transport"],
            record["brand"],
            record["model"],
            record["remark"],
            record["extra_json"],
            record["source_file"],
            record["source_type"],
            int(record["version_no"]),
            record["content_hash"],
            record["updated_at"],
            int(record["is_deleted"]),
        ),
    )
def append_history(connection: sqlite3.Connection, record: dict[str, Any], change_type: str, changed_at: str) -> None:
    connection.execute(
        """
        INSERT INTO records_history (
            record_id, ledger_name, base_id,
            entity_name, location, ip, department, owner, status, transport, brand, model, remark,
            extra_json, source_file, source_type, version_no, content_hash,
            changed_at, change_type, is_deleted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["record_id"],
            normalize_ledger_name(record["ledger_name"]),
            record["base_id"],
            record["entity_name"],
            record["location"],
            record["ip"],
            record["department"],
            record["owner"],
            record["status"],
            record["transport"],
            record["brand"],
            record["model"],
            record["remark"],
            record["extra_json"],
            record["source_file"],
            record["source_type"],
            int(record["version_no"]),
            record["content_hash"],
            changed_at,
            change_type,
            int(record["is_deleted"]),
        ),
    )


def log_operation(
    connection: sqlite3.Connection,
    ledger_name: str,
    operation_type: str,
    target_record_id: str | None,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    source_file: str | None,
    message: str,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO operations_log (
            ledger_name, operation_type, target_record_id, before_json, after_json,
            source_file, message, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalize_ledger_name(ledger_name),
            operation_type,
            target_record_id,
            json.dumps(before, ensure_ascii=False, sort_keys=True) if before else None,
            json.dumps(after, ensure_ascii=False, sort_keys=True) if after else None,
            source_file,
            message,
            created_at,
        ),
    )


def load_current_record(connection: sqlite3.Connection, record_id: str) -> dict[str, Any] | None:
    row = connection.execute("SELECT * FROM records_current WHERE record_id = ?", (record_id,)).fetchone()
    return row_to_dict(row)


def resolve_target_record(
    connection: sqlite3.Connection,
    ledger_name: str,
    record_id: str | None = None,
    base_id: str | None = None,
    ip: str | None = None,
) -> dict[str, Any]:
    ledger = normalize_ledger_name(ledger_name)
    if record_id:
        normalized_record_id = resolve_record_id_input(record_id, ledger)
        record = load_current_record(connection, normalized_record_id)
        if record is None:
            raise LedgerError(f"Target record does not exist: {record_id}")
        return record
    if base_id:
        normalized = sanitize_base_id(base_id)
        row = connection.execute(
            "SELECT * FROM records_current WHERE ledger_name = ? AND base_id = ?",
            (ledger, normalized),
        ).fetchone()
        if row is None:
            raise LedgerError(f"Target record does not exist for base_id: {base_id}")
        return row_to_dict(row) or {}
    if ip:
        rows = connection.execute(
            "SELECT * FROM records_current WHERE ledger_name = ? AND ip = ?",
            (ledger, ip),
        ).fetchall()
        if not rows:
            raise LedgerError(f"Target record does not exist for ip: {ip}")
        if len(rows) > 1:
            raise LedgerError(f"IP is not unique in ledger '{ledger}': {ip}")
        return row_to_dict(rows[0]) or {}
    raise LedgerError("Either record_id, base_id, or ip must be provided.")


def build_where_clause(
    ledger_name: str | None = DEFAULT_LEDGER,
    filters: dict[str, Any] | None = None,
    contains_filters: dict[str, Any] | None = None,
    record_ids: Sequence[str] | None = None,
    include_deleted: bool = False,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    parameters: list[Any] = []

    if ledger_name is not None:
        clauses.append("ledger_name = ?")
        parameters.append(normalize_ledger_name(ledger_name))

    if not include_deleted:
        clauses.append("is_deleted = 0")

    for field, value in (filters or {}).items():
        if value is None or value == "":
            continue
        if field not in QUERYABLE_FIELDS:
            raise LedgerError(f"Unsupported filter field: {field}")
        clauses.append(f"{field} = ?")
        parameters.append(value)

    for field, value in (contains_filters or {}).items():
        if value is None or value == "":
            continue
        if field not in LIKE_FILTER_FIELDS:
            raise LedgerError(f"Unsupported contains filter field: {field}")
        clauses.append(f"{field} LIKE ?")
        parameters.append(f"%{value}%")

    if record_ids is not None:
        if not record_ids:
            clauses.append("1 = 0")
        else:
            placeholders = ", ".join("?" for _ in record_ids)
            clauses.append(f"record_id IN ({placeholders})")
            parameters.extend(record_ids)

    sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    return sql, parameters


def fetch_current_records(
    connection: sqlite3.Connection,
    ledger_name: str | None = DEFAULT_LEDGER,
    filters: dict[str, Any] | None = None,
    contains_filters: dict[str, Any] | None = None,
    record_ids: Sequence[str] | None = None,
    include_deleted: bool = False,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    where_clause, parameters = build_where_clause(
        ledger_name=ledger_name,
        filters=filters,
        contains_filters=contains_filters,
        record_ids=record_ids,
        include_deleted=include_deleted,
    )
    sql = "SELECT * FROM records_current" + where_clause + " ORDER BY updated_at DESC, record_id ASC"
    if limit is not None:
        sql += " LIMIT ?"
        parameters.append(limit)
    rows = connection.execute(sql, parameters).fetchall()
    return [row_to_dict(row) or {} for row in rows]


def fetch_history_records(
    connection: sqlite3.Connection,
    ledger_name: str | None = DEFAULT_LEDGER,
    filters: dict[str, Any] | None = None,
    contains_filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    parameters: list[Any] = []

    if ledger_name is not None:
        clauses.append("ledger_name = ?")
        parameters.append(normalize_ledger_name(ledger_name))

    for field, value in (filters or {}).items():
        if value is None or value == "":
            continue
        if field not in QUERYABLE_FIELDS:
            raise LedgerError(f"Unsupported history filter field: {field}")
        clauses.append(f"{field} = ?")
        parameters.append(value)

    for field, value in (contains_filters or {}).items():
        if value is None or value == "":
            continue
        if field not in LIKE_FILTER_FIELDS:
            raise LedgerError(f"Unsupported history contains filter field: {field}")
        clauses.append(f"{field} LIKE ?")
        parameters.append(f"%{value}%")

    sql = "SELECT * FROM records_history"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY changed_at DESC, history_id DESC"
    if limit is not None:
        sql += " LIMIT ?"
        parameters.append(limit)
    rows = connection.execute(sql, parameters).fetchall()
    return [row_to_dict(row) or {} for row in rows]


def count_current_records(
    connection: sqlite3.Connection,
    ledger_name: str | None = DEFAULT_LEDGER,
    filters: dict[str, Any] | None = None,
    contains_filters: dict[str, Any] | None = None,
    include_deleted: bool = False,
) -> int:
    where_clause, parameters = build_where_clause(
        ledger_name=ledger_name,
        filters=filters,
        contains_filters=contains_filters,
        include_deleted=include_deleted,
    )
    row = connection.execute("SELECT COUNT(*) AS total FROM records_current" + where_clause, parameters).fetchone()
    return int(row["total"]) if row else 0


def count_active_current_records(connection: sqlite3.Connection, ledger_name: str | None = DEFAULT_LEDGER) -> int:
    return count_current_records(connection, ledger_name=ledger_name, include_deleted=False)


def load_embedding_settings(require_provider: bool = True) -> EmbeddingSettings:
    provider = canonicalize_value(os.getenv("LEDGER_EMBEDDING_PROVIDER"))
    if not provider:
        if require_provider:
            raise LedgerError(
                "Embedding provider is not configured. Set LEDGER_EMBEDDING_PROVIDER "
                "or use LEDGER_EMBEDDING_PROVIDER=mock for local testing."
            )
        return EmbeddingSettings("", None, None, None)

    base_url = canonicalize_value(os.getenv("LEDGER_EMBEDDING_BASE_URL")) or None
    api_key = canonicalize_value(os.getenv("LEDGER_EMBEDDING_API_KEY")) or None
    model = canonicalize_value(os.getenv("LEDGER_EMBEDDING_MODEL")) or None

    if provider != "mock":
        missing = [
            name
            for name, value in (
                ("LEDGER_EMBEDDING_BASE_URL", base_url),
                ("LEDGER_EMBEDDING_API_KEY", api_key),
                ("LEDGER_EMBEDDING_MODEL", model),
            )
            if not value
        ]
        if missing:
            raise LedgerError(f"Embedding provider is missing configuration: {', '.join(missing)}")

    return EmbeddingSettings(provider, base_url, api_key, model)


def resolve_embeddings_endpoint(base_url: str) -> str:
    clean_url = base_url.rstrip("/")
    return clean_url if clean_url.endswith("/embeddings") else f"{clean_url}/embeddings"


def normalize_embeddings(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return matrix.astype(np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return (matrix / norms).astype(np.float32)


def tokenize_text(text: str) -> list[str]:
    return [token for token in TOKEN_PATTERN.findall((text or "").lower()) if token]


def mock_embed_texts(texts: Sequence[str], dimensions: int = 128) -> np.ndarray:
    matrix = np.zeros((len(texts), dimensions), dtype=np.float32)
    for row_index, text in enumerate(texts):
        tokens = tokenize_text(text) or ["<empty>"]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            for column_index in range(dimensions):
                byte = digest[column_index % len(digest)]
                sign = 1.0 if digest[(column_index + 7) % len(digest)] % 2 == 0 else -1.0
                matrix[row_index, column_index] += sign * (byte / 255.0)
    return normalize_embeddings(matrix)


def remote_embed_texts(texts: Sequence[str], settings: EmbeddingSettings) -> np.ndarray:
    if not settings.base_url or not settings.api_key or not settings.model:
        raise LedgerError("Embedding provider configuration is incomplete.")

    endpoint = resolve_embeddings_endpoint(settings.base_url)
    payload = json.dumps({"input": list(texts), "model": settings.model}).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise LedgerError(f"Embedding request failed with HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise LedgerError(f"Embedding request failed: {exc.reason}") from exc

    data = response_payload.get("data")
    if not isinstance(data, list) or len(data) != len(texts):
        raise LedgerError("Embedding provider returned an unexpected payload.")
    ordered = sorted(data, key=lambda item: item.get("index", 0))
    matrix = np.asarray([item["embedding"] for item in ordered], dtype=np.float32)
    return normalize_embeddings(matrix)


def embed_texts(texts: Sequence[str], require_provider: bool = True) -> np.ndarray:
    settings = load_embedding_settings(require_provider=require_provider)
    if not settings.provider:
        return np.empty((0, 0), dtype=np.float32)
    if settings.provider == "mock":
        return mock_embed_texts(texts)
    return remote_embed_texts(texts, settings)


def save_index(
    ledger_names: Sequence[str],
    record_ids: Sequence[str],
    version_nos: Sequence[int],
    embeddings: np.ndarray,
) -> Path:
    ensure_runtime_paths()
    destination = index_path()
    temporary = destination.with_suffix(".tmp.npz")
    np.savez_compressed(
        temporary,
        ledger_names=np.asarray(ledger_names, dtype=str),
        record_ids=np.asarray(record_ids, dtype=str),
        version_nos=np.asarray(version_nos, dtype=np.int64),
        embeddings=np.asarray(embeddings, dtype=np.float32),
    )
    temporary.replace(destination)
    return destination


def load_index() -> tuple[list[str], list[str], list[int], np.ndarray]:
    destination = index_path()
    if not destination.is_file():
        raise LedgerError(f"Vector index is missing at {destination}. Run scripts/rebuild_embeddings.py.")

    with np.load(destination, allow_pickle=False) as payload:
        record_ids = payload["record_ids"].astype(str).tolist()
        version_nos = payload["version_nos"].astype(np.int64).tolist()
        embeddings = payload["embeddings"].astype(np.float32)
        if "ledger_names" in payload.files:
            ledger_names = payload["ledger_names"].astype(str).tolist()
        else:
            ledger_names = [split_record_id(record_id)[0] for record_id in record_ids]

    return ledger_names, record_ids, version_nos, embeddings


def rebuild_embedding_index(connection: sqlite3.Connection, require_provider: bool = True) -> int:
    rows = fetch_current_records(connection, ledger_name=None, include_deleted=False)
    connection.execute("DELETE FROM embeddings_meta")

    if not rows:
        save_index([], [], [], np.empty((0, 0), dtype=np.float32))
        return 0

    texts = [build_embedding_text(row) for row in rows]
    embeddings = embed_texts(texts, require_provider=require_provider)
    if embeddings.shape[0] != len(rows):
        raise LedgerError("Embedding provider returned an unexpected number of vectors.")

    for row, text in zip(rows, texts):
        connection.execute(
            """
            INSERT INTO embeddings_meta (record_id, ledger_name, version_no, is_current, embedding_text, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            """,
            (
                row["record_id"],
                normalize_ledger_name(row["ledger_name"]),
                int(row["version_no"]),
                text,
                row["updated_at"],
            ),
        )

    save_index(
        [normalize_ledger_name(row["ledger_name"]) for row in rows],
        [row["record_id"] for row in rows],
        [int(row["version_no"]) for row in rows],
        embeddings,
    )
    return len(rows)


def maybe_rebuild_embedding_index(connection: sqlite3.Connection, require_provider: bool = True) -> tuple[int, str | None]:
    try:
        return rebuild_embedding_index(connection, require_provider=require_provider), None
    except LedgerError as exc:
        message = str(exc)
        if message.startswith("Embedding provider"):
            return count_active_current_records(connection, ledger_name=None), message
        raise


def validate_index_sync(connection: sqlite3.Connection) -> None:
    current_triplets = sorted(
        (normalize_ledger_name(row["ledger_name"]), row["record_id"], int(row["version_no"]))
        for row in fetch_current_records(connection, ledger_name=None, include_deleted=False)
    )
    meta_triplets = sorted(
        (normalize_ledger_name(row["ledger_name"]), row["record_id"], int(row["version_no"]))
        for row in connection.execute(
            "SELECT ledger_name, record_id, version_no FROM embeddings_meta WHERE is_current = 1"
        ).fetchall()
    )

    if not current_triplets and not meta_triplets:
        if index_path().exists():
            _, _, _, embeddings = load_index()
            if embeddings.size != 0:
                raise LedgerError("Vector index is out of sync with current records. Run scripts/rebuild_embeddings.py.")
        return

    if not index_path().exists():
        raise LedgerError("Vector index is out of sync with current records. Run scripts/rebuild_embeddings.py.")

    ledger_names, record_ids, version_nos, _ = load_index()
    index_triplets = sorted(
        (normalize_ledger_name(ledger_name), record_id, int(version_no))
        for ledger_name, record_id, version_no in zip(ledger_names, record_ids, version_nos)
    )

    if current_triplets != meta_triplets or current_triplets != index_triplets:
        raise LedgerError("Vector index is out of sync with current records. Run scripts/rebuild_embeddings.py.")


def semantic_search_records(
    connection: sqlite3.Connection,
    query: str,
    top_k: int = 5,
    ledger_name: str | None = DEFAULT_LEDGER,
    filters: dict[str, Any] | None = None,
    contains_filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    validate_index_sync(connection)
    ledger_names, record_ids, _, embeddings = load_index()
    if not record_ids:
        return []

    normalized_ledger = normalize_ledger_name(ledger_name) if ledger_name is not None else None
    candidate_indices = list(range(len(record_ids)))
    if normalized_ledger is not None:
        candidate_indices = [index for index, name in enumerate(ledger_names) if normalize_ledger_name(name) == normalized_ledger]
        if not candidate_indices:
            return []

    query_embedding = embed_texts([query], require_provider=True)
    if query_embedding.shape[0] != 1:
        raise LedgerError("Failed to create query embedding.")

    selected_embeddings = embeddings[candidate_indices]
    scores = selected_embeddings @ query_embedding[0]
    candidate_count = min(len(candidate_indices), max(top_k * 8, top_k))
    ranked_local_indices = np.argsort(scores)[::-1][:candidate_count]
    ranked_indices = [candidate_indices[index] for index in ranked_local_indices]

    selected_record_ids = [record_ids[index] for index in ranked_indices]
    score_map = {record_ids[index]: float(scores[local_index]) for local_index, index in enumerate(ranked_indices)}
    query_tokens = set(tokenize_text(query))

    rows = fetch_current_records(
        connection,
        ledger_name=normalized_ledger,
        filters=filters,
        contains_filters=contains_filters,
        record_ids=selected_record_ids,
        include_deleted=False,
    )

    for row in rows:
        extras = safe_json_loads(row.get("extra_json"))
        lexical_source = " ".join(canonicalize_value(row.get(field, "")) for field in CANONICAL_FIELDS)
        lexical_source += " " + " ".join(f"{key} {value}" for key, value in extras.items())
        lexical_overlap = len(query_tokens & set(tokenize_text(lexical_source)))
        vector_score = score_map.get(row["record_id"], 0.0)
        row["lexical_overlap"] = lexical_overlap
        row["score"] = round(vector_score + (0.1 * lexical_overlap), 6)

    rows.sort(key=lambda item: (item.get("lexical_overlap", 0), item.get("score", 0.0)), reverse=True)
    return rows[:top_k]


def canonical_field_from_label(label: str) -> str | None:
    normalized = normalize_header(label)
    normalized = re.sub(r"__\d+$", "", normalized)
    if normalized in FIELD_ALIASES:
        return FIELD_ALIASES[normalized]
    if normalized in EXPORT_LABEL_ALIASES:
        return EXPORT_LABEL_ALIASES[normalized]
    return None


def parse_requested_columns(raw_columns: str | None) -> list[str]:
    if not raw_columns:
        return ["record_id", "entity_name", "location", "ip", "owner", "status", "transport", "remark", "updated_at"]
    parts = [part.strip() for part in raw_columns.split(",") if part.strip()]
    if not parts:
        raise LedgerError("Requested columns are empty.")
    return parts


def flatten_record_for_export(row: dict[str, Any]) -> dict[str, Any]:
    flattened = dict(row)
    extras = safe_json_loads(row.get("extra_json"))
    for key, value in extras.items():
        if key not in flattened:
            flattened[key] = value
    return flattened


def project_rows_for_export(rows: list[dict[str, Any]], requested_columns: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    projected_rows: list[dict[str, Any]] = []
    resolved_labels: list[str] = list(requested_columns)

    for row in rows:
        flattened = flatten_record_for_export(row)
        output_row: dict[str, Any] = {}
        for label in requested_columns:
            canonical_field = canonical_field_from_label(label)
            lookup_key = canonical_field or label
            output_row[label] = canonicalize_value(flattened.get(lookup_key, ""))
        projected_rows.append(output_row)

    return projected_rows, resolved_labels


def print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
