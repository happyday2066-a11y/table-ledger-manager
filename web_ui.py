from __future__ import annotations

import csv
import io
import ipaddress
import json
import math
import re
import sys
import tempfile
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from subprocess import CompletedProcess, run
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pypinyin import lazy_pinyin

ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from common import (  # noqa: E402
    CANONICAL_FIELDS,
    LedgerError,
    canonical_field_from_label,
    canonicalize_value,
    connect_db,
    count_active_current_records,
    extract_ipv4_values,
    extract_quantity_values,
    exports_dir,
    fetch_current_records,
    flatten_record_for_export,
    infer_dominant_ipv4_prefix,
    initialize_database,
    normalize_ledger_name,
    project_rows_for_export,
    safe_json_loads,
)
from ledger_semantics import ensure_ledger_semantics, load_ledger_semantics, rebuild_ledger_semantics  # noqa: E402

app = FastAPI(title="Dynamic Ledger Engine")
templates = Jinja2Templates(directory=str(ROOT / "templates"))
app.mount("/static", StaticFiles(directory=str(ROOT / "static")), name="static")

PAGE_SIZE = 12
IGNORED_AUTO_LEDGERS = {"sample", "sample-2"}
MIXED_LEDGERS = {"default"}
INTERNAL_LEDGERS = IGNORED_AUTO_LEDGERS | MIXED_LEDGERS

COUNT_TERMS = ("多少", "几个", "几条", "多少个", "一共", "总共", "总数", "统计", "统计下", "数量", "个数", "count", "how many", "number of")
LIST_TERMS = ("列出", "哪些", "清单", "列表", "查看", "展示", "show", "list", "which")
GROUP_TERMS = ("分别", "各", "每个", "按", "group by")
GLOBAL_TERMS = ("一共", "总共", "所有", "全部", "全体", "全库", "跨账本")
EXISTS_TERMS = ("有", "具有", "带有", "存在", "非空")
QUANTITY_SUM_TERMS = ("数量", "个数", "数目", "台数", "套数", "部数", "件数", "合计", "汇总", "总量", "总计")
STOP_TERMS = {
    "台账",
    "工具",
    "告诉我",
    "帮我",
    "请",
    "一下",
    "查看",
    "查询",
    "记录",
    "信息",
    "详细",
    "明细",
    "列表",
    "清单",
    "列出",
    "是否",
    "多少",
    "几个",
    "几条",
    "一共",
    "总共",
    "总数",
    "分别",
    "各",
    "每个",
    "的",
    "是",
    "了",
    "吗",
    "呢",
    "吧",
    "请问",
    "条",
    "个",
    "个数",
    "数量",
    "所有",
    "全部",
    "全体",
    "统计",
    "统计下",
    "有",
    "带有",
    "具有",
    "存在",
    "非空",
}
GENERIC_SUBJECT_TERMS = {
    "相关",
    "台账",
    "账本",
    "数据",
    "记录",
    "信息",
    "资料",
    "内容",
    "情况",
    "统计下",
    "统计",
    "共",
    "共有",
    "所有",
    "全部",
    "全体",
    "什么",
    "有",
    "下",
}
EXTRA_SUFFIX_PATTERN = re.compile(r"__(\d+)$")
MOBILE_PATTERN = re.compile(r"(?<!\d)(1[3-9]\d{9})(?!\d)")
EXTENSION_PATTERN = re.compile(r"(?<!\d)(\d{3,5})(?!\d)")
QUERY_TOKEN_PATTERN = re.compile(r"[a-z0-9_.:/-]*[\u4e00-\u9fff]+[a-z0-9_.:/-]*|[a-z0-9_.:/-]+|[\u4e00-\u9fff]{2,}")
DISPLAY_PLACEHOLDER_PATTERN = re.compile(r"^sheet\d*$", re.IGNORECASE)

FIELD_HINTS: dict[str, tuple[str, ...]] = {
    "ip": ("ip", "ip地址", "地址", "网络地址"),
    "phone": ("电话", "电话号码", "手机号", "手机", "手机号码", "联系电话", "联系方式", "办电", "座机", "办公电话", "移动电话", "phone", "mobile", "tel"),
    "department": ("部门", "科室", "管理段", "单位", "组织"),
    "owner": ("负责人", "责任人", "联系人", "所有人", "归属人", "使用人", "持有人", "领用人"),
    "location": ("位置", "地点", "点位", "地址", "桩号", "站点", "坝", "辅道", "铁塔", "存放地点", "房间", "机房", "库位"),
    "transport": ("传输", "传输方式", "4g", "5g", "光纤", "专线", "无线", "wifi"),
    "brand": ("品牌", "厂商", "大华", "海康", "vendor"),
    "model": ("型号", "机型", "规格"),
    "status": ("状态", "启用", "停用", "active", "使用状况"),
    "quantity": ("数量", "资产数量", "台数", "个数", "数目"),
    "entity_name": ("名称", "名字", "名称是什么", "叫什么", "name"),
}

ANSWER_FIELD_HINTS: dict[str, tuple[str, ...]] = {
    "ip": ("ip", "ip地址", "网络地址"),
    "phone": ("电话", "电话号码", "手机号", "手机号码", "联系电话", "联系方式", "办电", "座机", "办公电话", "移动电话", "phone", "mobile", "tel"),
    "department": ("部门", "科室", "管理段", "单位", "组织"),
    "owner": ("负责人", "责任人", "联系人", "所有人", "归属人", "使用人", "持有人", "领用人"),
    "location": ("位置", "地点", "桩号", "存放地点", "房间", "机房", "库位", "在哪里", "在哪"),
    "transport": ("传输", "传输方式"),
    "brand": ("品牌", "厂商"),
    "model": ("型号", "机型", "规格"),
    "status": ("状态", "启用", "停用", "使用状况"),
    "quantity": ("数量", "资产数量", "台数", "个数", "数目"),
    "entity_name": ("名称", "名字", "名称是什么", "叫什么", "name"),
}

DETAIL_FIELD_PRIORITY = ("ip", "owner", "department", "brand", "model", "transport", "location")
ENTITY_OBJECT_TERMS = ("摄像头", "监控", "点位", "设备", "资产", "联系人", "电话", "主机", "终端")
CATEGORY_TARGET_HINTS: dict[str, tuple[str, ...]] = {
    "camera": ("摄像头", "监控", "点位", "设备", "桩号", "铁塔", "辅道", "号坝", "4g摄像头", "监控点位"),
    "contact": ("电话", "电话号码", "手机号", "手机号码", "联系电话", "联系方式", "通讯录", "联系人"),
    "network": ("ip", "ip地址", "网络", "路由器", "交换机", "网关", "mac"),
    "asset": ("资产", "盘点", "在用", "闲置", "存放", "机房", "库位", "防火墙", "交换机", "设备类型"),
}
GROUP_FIELD_HINTS = {
    "department": ("管理段", "部门", "科室", "单位"),
    "brand": ("品牌", "厂商"),
    "model": ("型号", "机型"),
    "transport": ("传输", "传输方式", "4g", "5g", "光纤", "专线", "无线"),
    "status": ("状态",),
}

ANSWER_FIELD_LABELS = {
    "ip": "IP",
    "phone": "电话",
    "department": "部门",
    "owner": "负责人",
    "brand": "品牌",
    "model": "型号",
    "transport": "传输方式",
    "location": "位置",
    "quantity": "数量",
}

RECORD_TITLE_FIELDS = ("entity_name", "location", "owner", "department", "ip")
DETAIL_SUBJECT_FIELDS = ("owner", "entity_name", "department", "location")

ACTIVE_ROWS_CACHE: dict[str, list[dict[str, Any]]] | None = None
QUERY_INDEX_CACHE: dict[str, dict[str, Any]] = {}
LEDGER_PROFILE_CACHE: dict[str, dict[str, Any]] = {}
LEDGER_SEMANTICS_CACHE: dict[str, dict[str, Any]] | None = None
EXPORT_CACHE: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True)
class QueryPlan:
    question: str
    intent: str
    answer_field: str | None
    group_field: str | None
    dedupe_key: str | None
    global_scope: bool
    existence_fields: tuple[str, ...]
    subject: str
    filter_fields: tuple[str, ...]
    free_terms: tuple[str, ...]


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def invalidate_runtime_cache(ledger_names: list[str] | None = None) -> None:
    global ACTIVE_ROWS_CACHE, LEDGER_SEMANTICS_CACHE
    ACTIVE_ROWS_CACHE = None
    if ledger_names is None:
        QUERY_INDEX_CACHE.clear()
        LEDGER_PROFILE_CACHE.clear()
        LEDGER_SEMANTICS_CACHE = None
        return
    normalized = {normalize_ledger_name(name) for name in ledger_names}
    for ledger_name in list(QUERY_INDEX_CACHE):
        if ledger_name in normalized:
            QUERY_INDEX_CACHE.pop(ledger_name, None)
    for ledger_name in list(LEDGER_PROFILE_CACHE):
        if ledger_name in normalized:
            LEDGER_PROFILE_CACHE.pop(ledger_name, None)
    if LEDGER_SEMANTICS_CACHE:
        for ledger_name in list(LEDGER_SEMANTICS_CACHE):
            if ledger_name in normalized:
                LEDGER_SEMANTICS_CACHE.pop(ledger_name, None)


@lru_cache(maxsize=4096)
def normalize_text(value: str) -> str:
    text = canonicalize_value(value).lower()
    text = text.replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", " ", text).strip()


@lru_cache(maxsize=4096)
def normalize_compact(value: str) -> str:
    text = normalize_text(value)
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", text)


@lru_cache(maxsize=4096)
def to_pinyin_text(value: str) -> str:
    text = re.sub(r"\s+", "", canonicalize_value(value))
    if not text:
        return ""
    return "".join(lazy_pinyin(text)).lower()


@lru_cache(maxsize=4096)
def pinyin_initials(value: str) -> str:
    text = re.sub(r"\s+", "", canonicalize_value(value))
    if not text:
        return ""
    return "".join(item[0] for item in lazy_pinyin(text) if item).lower()


def extra_key_base(label: str) -> str:
    return EXTRA_SUFFIX_PATTERN.sub("", canonicalize_value(label))


def extra_key_group(label: str) -> str:
    match = EXTRA_SUFFIX_PATTERN.search(canonicalize_value(label))
    return match.group(1) if match else ""


def looks_like_ip(value: str) -> bool:
    text = canonicalize_value(value)
    if not text:
        return False
    try:
        ipaddress.ip_address(text)
        return True
    except ValueError:
        return False


def extract_phone_numbers(value: str) -> list[str]:
    text = canonicalize_value(value)
    if not text:
        return []
    results: list[str] = []
    for number in MOBILE_PATTERN.findall(text):
        if number not in results:
            results.append(number)
    for number in EXTENSION_PATTERN.findall(text):
        if number not in results:
            results.append(number)
    return results


def parse_brand_model_value(value: str) -> tuple[str, str]:
    text = canonicalize_value(value)
    if not text:
        return "", ""
    compact = text.replace("／", "/").replace(" / ", "/")
    if "/" in compact:
        brand, model = compact.split("/", 1)
        return canonicalize_value(brand), canonicalize_value(model)
    return canonicalize_value(text), canonicalize_value(text)


def ledger_dominant_ip_prefix(ledger_name: str) -> str:
    normalized = normalize_ledger_name(ledger_name)
    semantic_profile = load_ledger_semantic_profiles().get(normalized, {})
    prefix = canonicalize_value(semantic_profile.get("dominant_ip_prefix", ""))
    if prefix:
        return prefix
    source_values: list[str] = []
    for row in load_all_active_rows():
        if normalize_ledger_name(row.get("ledger_name", "")) != normalized:
            continue
        direct = canonicalize_value(row.get("ip", ""))
        if direct:
            source_values.append(direct)
        extras = safe_json_loads(row.get("extra_json"))
        for key, value in extras.items():
            if infer_extra_field(key) == "ip":
                source_values.append(value)
    return infer_dominant_ipv4_prefix(source_values)


def row_quantity_values(row: dict[str, Any]) -> list[int]:
    values: list[int] = []

    def add(number: int) -> None:
        if number > 0:
            values.append(number)

    direct = canonicalize_value(row.get("quantity", ""))
    for number in extract_quantity_values(direct):
        add(number)

    for key, value in row_extra_items(row):
        if infer_extra_field(key) != "quantity":
            continue
        for number in extract_quantity_values(value):
            add(number)
    return values


def normalize_subject_phrase(subject: str, answer_field: str | None = None) -> str:
    text = normalize_text(subject)
    for token in sorted(GENERIC_SUBJECT_TERMS, key=len, reverse=True):
        text = text.replace(token, " ")
    if answer_field:
        for hint in sorted(ANSWER_FIELD_HINTS.get(answer_field, ()), key=len, reverse=True):
            text = text.replace(hint, " ")
    for token in ENTITY_OBJECT_TERMS:
        if text == token:
            return ""
        if text.endswith(token) and len(text) > len(token):
            text = text[: -len(token)]
            break
    text = re.sub(r"\s+", " ", text).strip()
    return canonicalize_value(text)


def normalize_display_text(value: Any) -> str:
    text = re.sub(r"\s+", " ", canonicalize_value(value)).strip()
    return re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)


def trim_query_token(token: str) -> str:
    value = canonicalize_value(token)
    if not value:
        return ""

    edge_terms = tuple(sorted(STOP_TERMS | set(EXISTS_TERMS), key=len, reverse=True))
    changed = True
    while value and changed:
        changed = False
        for term in edge_terms:
            if value.startswith(term) and len(value) > len(term):
                value = canonicalize_value(value[len(term) :])
                changed = True
            if value.endswith(term) and len(value) > len(term):
                value = canonicalize_value(value[: -len(term)])
                changed = True
    return value


@lru_cache(maxsize=4096)
def split_query_terms(question: str) -> tuple[str, ...]:
    raw = normalize_text(question)
    for term in sorted(STOP_TERMS, key=len, reverse=True):
        raw = raw.replace(term, " ")
    ordered: list[str] = []
    for token in QUERY_TOKEN_PATTERN.findall(raw):
        token = trim_query_token(token)
        if re.fullmatch(r"[a-z0-9_.:/-]+", token) and len(token) < 2:
            continue
        if re.fullmatch(r"[\u4e00-\u9fff]+", token) and len(token) < 2:
            continue
        if token and token not in ordered:
            ordered.append(token)
    return tuple(ordered)


@lru_cache(maxsize=8192)
def alias_forms(value: str) -> tuple[tuple[str, str], ...]:
    raw = canonicalize_value(value)
    if not raw:
        return tuple()
    forms: list[tuple[str, str]] = []

    def add(kind: str, form: str) -> None:
        if form and (kind, form) not in forms:
            forms.append((kind, form))

    add("raw", raw)
    add("text", normalize_text(raw))
    add("compact", normalize_compact(raw))
    add("pinyin", to_pinyin_text(raw))
    initials = pinyin_initials(raw)
    if len(initials) >= 2:
        add("initials", initials)
    return tuple(forms)


def ledger_base_name(ledger_name: str) -> str:
    return re.sub(r"-\d+$", "", normalize_ledger_name(ledger_name))


def ledger_family_name(ledger_name: str, known_ledgers: list[str] | tuple[str, ...]) -> str:
    normalized = normalize_ledger_name(ledger_name)
    semantic_profiles = load_ledger_semantic_profiles()
    semantic_profile = semantic_profiles.get(normalized)
    if semantic_profile:
        family_name = normalize_ledger_name(semantic_profile.get("family_name", normalized))
        if family_name:
            known_normalized = {normalize_ledger_name(name) for name in known_ledgers}
            if family_name in known_normalized:
                return family_name
    match = re.match(r"^(.*)-(\d+)$", normalized)
    if match:
        candidate = match.group(1)
        known_normalized = {normalize_ledger_name(name) for name in known_ledgers}
        if candidate in known_normalized:
            return candidate
    return normalized


def question_mentions_ledger(question: str, ledger_name: str) -> bool:
    normalized_question = normalize_text(question)
    compact_question = normalize_compact(question)
    pinyin_question = to_pinyin_text(question)
    for candidate in {normalize_ledger_name(ledger_name), ledger_base_name(ledger_name)}:
        if not candidate:
            continue
        normalized_candidate = normalize_text(candidate)
        compact_candidate = normalize_compact(candidate)
        pinyin_candidate = to_pinyin_text(candidate)
        if normalized_candidate and normalized_candidate in normalized_question:
            return True
        if compact_candidate and compact_candidate in compact_question:
            return True
        if pinyin_candidate and pinyin_candidate in pinyin_question:
            return True
    return False


def load_all_active_rows(force_refresh: bool = False) -> list[dict[str, Any]]:
    global ACTIVE_ROWS_CACHE
    if ACTIVE_ROWS_CACHE is not None and not force_refresh:
        return ACTIVE_ROWS_CACHE
    connection = connect_db()
    initialize_database(connection)
    rows = fetch_current_records(connection, ledger_name=None, include_deleted=False)
    connection.close()
    ACTIVE_ROWS_CACHE = rows
    return rows


def load_ledger_semantic_profiles(force_refresh: bool = False) -> dict[str, dict[str, Any]]:
    global LEDGER_SEMANTICS_CACHE
    if LEDGER_SEMANTICS_CACHE is not None and not force_refresh:
        return LEDGER_SEMANTICS_CACHE
    connection = connect_db()
    initialize_database(connection)
    ensure_ledger_semantics(connection)
    # Persist rebuilt profiles for existing ledgers imported before this feature landed.
    connection.commit()
    LEDGER_SEMANTICS_CACHE = load_ledger_semantics(connection)
    connection.close()
    return LEDGER_SEMANTICS_CACHE


def list_ledger_summaries() -> list[dict[str, Any]]:
    rows = load_all_active_rows()
    counts: Counter[str] = Counter()
    for row in rows:
        counts[normalize_ledger_name(row.get("ledger_name", ""))] += 1
    return [
        {"ledger_name": ledger_name, "count": counts[ledger_name]}
        for ledger_name in sorted(counts)
    ]


def visible_ledger_summaries() -> list[dict[str, Any]]:
    return [item for item in list_ledger_summaries() if item["ledger_name"] not in INTERNAL_LEDGERS]


def row_blob(row: dict[str, Any]) -> str:
    parts = [canonicalize_value(row.get(field, "")) for field in CANONICAL_FIELDS]
    parts.extend(
        canonicalize_value(row.get(field, ""))
        for field in ("record_id", "base_id", "ledger_name", "source_file", "source_type")
    )
    extras = safe_json_loads(row.get("extra_json"))
    parts.extend(f"{key} {value}" for key, value in extras.items())
    return " ".join(part for part in parts if part)


def flattened_row(row: dict[str, Any]) -> dict[str, Any]:
    flat = flatten_record_for_export(row)
    extras = safe_json_loads(row.get("extra_json"))
    for key, value in extras.items():
        inferred = infer_extra_field(key)
        text = canonicalize_value(value)
        if not inferred or not text:
            continue
        if inferred == "brand_model":
            brand_value, model_value = parse_brand_model_value(text)
            if brand_value and not canonicalize_value(flat.get("brand", "")):
                flat["brand"] = brand_value
            if model_value and not canonicalize_value(flat.get("model", "")):
                flat["model"] = model_value
            continue
        if inferred == "ip":
            if not canonicalize_value(flat.get("ip", "")):
                prefix = ledger_dominant_ip_prefix(flat.get("ledger_name", ""))
                ip_values = extract_ipv4_values(text, prefix)
                if ip_values:
                    flat["ip"] = ip_values[0]
            continue
        if inferred == "quantity":
            if not canonicalize_value(flat.get("quantity", "")):
                numbers = row_quantity_values({**row, "quantity": text})
                if numbers:
                    flat["quantity"] = str(numbers[0])
            continue
        if not canonicalize_value(flat.get(inferred, "")):
            flat[inferred] = text
    flat["_search_blob"] = row_blob(row)
    flat["_search_compact"] = normalize_compact(flat["_search_blob"])
    flat["_search_pinyin"] = to_pinyin_text(flat["_search_blob"])
    return flat


def load_rows_by_ledger() -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in load_all_active_rows():
        grouped[normalize_ledger_name(row.get("ledger_name", ""))].append(flattened_row(row))
    return grouped


def build_ledger_index(ledger_name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    cached = QUERY_INDEX_CACHE.get(ledger_name)
    fingerprint = (len(rows), sum(len(row.get("record_id", "")) for row in rows))
    if cached and cached.get("fingerprint") == fingerprint:
        return cached

    values_by_field: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        for field in (*CANONICAL_FIELDS, "phone", "quantity"):
            for _, value in row_field_entries(row, field):
                values_by_field[field][value] += 1
        extras = safe_json_loads(row.get("extra_json"))
        for key, value in extras.items():
            text = canonicalize_value(value)
            if not text:
                continue
            extra_field = infer_extra_field(key)
            if extra_field in {None, "brand_model"}:
                values_by_field["generic"][text] += 1

    entries: list[dict[str, Any]] = []
    for field, counter in values_by_field.items():
        for value, count in counter.items():
            entries.append(
                {
                    "field": field,
                    "value": value,
                    "count": count,
                    "aliases": alias_forms(value),
                }
            )

    index = {"fingerprint": fingerprint, "entries": entries}
    QUERY_INDEX_CACHE[ledger_name] = index
    return index


def build_ledger_profile(ledger_name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    cached = LEDGER_PROFILE_CACHE.get(ledger_name)
    fingerprint = (len(rows), sum(len(row.get("record_id", "")) for row in rows))
    if cached and cached.get("fingerprint") == fingerprint:
        return cached

    total = max(1, len(rows))
    fields = ("entity_name", "location", "ip", "phone", "department", "owner", "brand", "model", "transport")
    ratios = {
        field: sum(1 for row in rows if row_has_field_value(row, field)) / total
        for field in fields
    }
    semantic_profile = load_ledger_semantic_profiles().get(ledger_name, {})
    merged_ratios = dict(ratios)
    merged_ratios.update(semantic_profile.get("field_ratios", {}))
    profile = {
        "fingerprint": fingerprint,
        "ratios": merged_ratios,
        "dominant_type": semantic_profile.get("dominant_type", "generic"),
        "semantic_terms": semantic_profile.get("semantic_terms", []),
        "value_examples": semantic_profile.get("value_examples", {}),
        "field_labels": semantic_profile.get("field_labels", {}),
        "family_name": semantic_profile.get("family_name", ledger_name),
        "semantic_type_scores": semantic_profile.get("semantic_type_scores", {}),
    }
    LEDGER_PROFILE_CACHE[ledger_name] = profile
    return profile


def infer_answer_field(question: str) -> str | None:
    normalized = normalize_text(question)
    if any(token in normalized for token in ("台账", "账本")) and any(term in normalized for term in COUNT_TERMS):
        return None
    if "ip地址" in normalized or re.search(r"\bip\b", normalized):
        return "ip"
    if "电话号码" in normalized or "电话" in normalized or "手机号" in normalized or "手机号码" in normalized:
        return "phone"
    if (
        ("数量" in normalized or "个数" in normalized)
        and not any(token in normalized for token in ("台账", "账本"))
    ):
        return "quantity"
    best_match: tuple[int, int, str] | None = None
    for field, hints in ANSWER_FIELD_HINTS.items():
        for hint in hints:
            position = normalized.rfind(hint)
            if position < 0:
                continue
            candidate = (position, len(hint), field)
            if best_match is None or candidate > best_match:
                best_match = candidate
    return best_match[2] if best_match else None


def infer_extra_field(label: str) -> str | None:
    base_label = extra_key_base(label)
    if base_label == "__sheet_name__" or canonicalize_value(label) == "__sheet_name__":
        return None
    canonical = canonical_field_from_label(base_label) or canonical_field_from_label(label)
    if canonical:
        return canonical
    normalized = normalize_text(base_label)
    if "品牌" in normalized and ("型号" in normalized or "规格" in normalized):
        return "brand_model"
    for field, hints in FIELD_HINTS.items():
        if any(hint in normalized for hint in hints):
            return field
    return None


def row_extra_items(row: dict[str, Any]) -> list[tuple[str, str]]:
    extras = safe_json_loads(row.get("extra_json"))
    return [(canonicalize_value(key), canonicalize_value(value)) for key, value in extras.items() if canonicalize_value(value)]


def row_subject_group(row: dict[str, Any], subject: str) -> str | None:
    if not subject:
        return None
    normalized_subject = normalize_subject_phrase(subject)
    subject_compact = normalize_compact(normalized_subject or subject)
    subject_pinyin = to_pinyin_text(normalized_subject or subject)

    def matches(value: str) -> bool:
        compact = normalize_compact(value)
        pinyin = to_pinyin_text(value)
        if subject_compact and compact == subject_compact:
            return True
        if subject_pinyin and pinyin == subject_pinyin:
            return True
        return False

    for field in ("entity_name", "owner"):
        value = canonicalize_value(row.get(field, ""))
        if value and matches(value):
            return ""
    for key, value in row_extra_items(row):
        if infer_extra_field(key) not in {"entity_name", "owner"}:
            continue
        if value and matches(value):
            return extra_key_group(key)
    return None


def row_field_entries(row: dict[str, Any], field: str, subject: str | None = None) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    seen_values: dict[str, int] = {}
    subject_group = row_subject_group(row, subject or "") if field == "phone" else None
    dominant_ip_prefix = ledger_dominant_ip_prefix(row.get("ledger_name", "")) if field == "ip" else ""

    def label_priority(label: str) -> int:
        base_label = extra_key_base(label) or canonicalize_value(label)
        if field == "phone":
            if base_label in {"办电", "座机", "办公电话", "手机号码", "手机号", "移动电话", "联系电话"}:
                return 3
            if base_label in {"电话", "phone", "tel"}:
                return 1
        canonical_label = ANSWER_FIELD_LABELS.get(field, field)
        if base_label == canonical_label:
            return 3
        if base_label:
            return 2
        return 1

    def add(label: str, value: str) -> None:
        text = canonicalize_value(value)
        if not text:
            return
        if field == "ip" and not looks_like_ip(text):
            return
        normalized_label = canonicalize_value(label)
        existing_index = seen_values.get(text)
        if existing_index is None:
            seen_values[text] = len(entries)
            entries.append((normalized_label, text))
            return
        current_label, _ = entries[existing_index]
        if label_priority(normalized_label) > label_priority(current_label):
            entries[existing_index] = (normalized_label, text)

    direct = canonicalize_value(row.get(field, ""))
    if direct:
        if field == "phone":
            for number in extract_phone_numbers(direct):
                add("电话", number)
        elif field == "ip":
            for ip_value in extract_ipv4_values(direct, dominant_ip_prefix):
                add("IP", ip_value)
        elif field == "quantity":
            for number in extract_quantity_values(direct):
                add("数量", str(number))
        else:
            add(ANSWER_FIELD_LABELS.get(field, field), direct)

    for key, value in row_extra_items(row):
        inferred = infer_extra_field(key)
        base_key = extra_key_base(key)
        group = extra_key_group(key)
        if field == "phone":
            if inferred != "phone":
                continue
            if subject_group is not None and group != subject_group:
                continue
            for number in extract_phone_numbers(value):
                add(base_key or "电话", number)
            continue
        if field == "ip":
            if inferred != "ip":
                continue
            for ip_value in extract_ipv4_values(value, dominant_ip_prefix):
                add(base_key or "IP", ip_value)
            continue
        if field == "quantity":
            if inferred != "quantity":
                continue
            for number in extract_quantity_values(value):
                add(base_key or "数量", str(number))
            continue
        if field in {"brand", "model"} and inferred == "brand_model":
            brand_value, model_value = parse_brand_model_value(value)
            add(base_key or field, brand_value if field == "brand" else model_value)
            continue
        if inferred == field:
            add(base_key or field, value)
    return entries


def classify_phone_entry(label: str, value: str) -> str:
    base_label = normalize_text(extra_key_base(label) or label)
    text = canonicalize_value(value)
    digits = re.sub(r"\D+", "", text)

    if MOBILE_PATTERN.fullmatch(digits):
        return "mobile"
    if any(token in base_label for token in ("手机", "移动电话", "手机号", "手机号码", "mobile")):
        return "mobile"
    if any(token in base_label for token in ("办电", "座机", "办公电话", "内线", "分机", "tel", "电话")):
        return "landline"
    if 3 <= len(digits) <= 8:
        return "landline"
    return "unknown"


def phone_entries(row: dict[str, Any], subject: str | None = None) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for label, value in row_field_entries(row, "phone", subject):
        entries.append(
            {
                "label": extra_key_base(label) or "电话",
                "value": canonicalize_value(value),
                "type": classify_phone_entry(label, value),
            }
        )
    return entries


def distinct_phone_summary(rows: list[dict[str, Any]], subject: str | None = None) -> dict[str, int]:
    type_by_number: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        for entry in phone_entries(row, subject):
            number = entry["value"]
            if not number:
                continue
            type_by_number[number].add(entry["type"])

    summary = {"mobile": 0, "landline": 0, "unknown": 0, "total": len(type_by_number)}
    for kinds in type_by_number.values():
        normalized_kinds = {kind for kind in kinds if kind != "unknown"}
        if normalized_kinds == {"mobile"}:
            summary["mobile"] += 1
        elif normalized_kinds == {"landline"}:
            summary["landline"] += 1
        else:
            summary["unknown"] += 1
    return summary


def row_answer_value(row: dict[str, Any], field: str | None, *, subject: str | None = None) -> str:
    if not field:
        return ""
    if field == "phone":
        entries = phone_entries(row, subject)
        if not entries:
            return ""
        formatted: list[str] = []
        for entry in entries:
            label = normalize_display_text(entry["label"])
            value = normalize_display_text(entry["value"])
            if label in {"电话", "phone", "tel"}:
                formatted.append(value)
            else:
                formatted.append(f"{label} {value}")
        return "；".join(formatted)
    entries = row_field_entries(row, field, subject)
    if not entries:
        return ""
    return normalize_display_text(entries[0][1])


def row_has_field_value(row: dict[str, Any], field: str) -> bool:
    return bool(row_field_entries(row, field))


def infer_group_field(question: str) -> str | None:
    normalized = normalize_text(question)
    if not any(term in normalized for term in GROUP_TERMS):
        return None
    for field, hints in GROUP_FIELD_HINTS.items():
        if any(hint in normalized for hint in hints):
            return field
    return None


def extract_subject(question: str, answer_field: str | None) -> str:
    normalized = normalize_text(question)
    if not answer_field:
        return ""
    for hint in ANSWER_FIELD_HINTS.get(answer_field, FIELD_HINTS.get(answer_field, ())):
        pattern = re.compile(rf"(.+?)(?:的)?{re.escape(hint)}")
        match = pattern.search(normalized)
        if match:
            candidate = canonicalize_value(match.group(1))
            candidate = re.sub(r"^(请|帮我|告诉我|查询|查看|列出)", "", candidate).strip()
            for splitter in ("里的", "中的", "内的", "里", "中", "内"):
                if splitter in candidate:
                    tail = candidate.split(splitter)[-1].strip()
                    if len(normalize_compact(tail)) >= 2:
                        candidate = tail
            candidate = normalize_subject_phrase(candidate, answer_field)
            if candidate and candidate not in {"有", "具有", "带有", "存在", "非空"}:
                return candidate
    return ""


def extract_primary_subject(question: str) -> str:
    normalized = normalize_text(question)
    if re.search(r"(?<!\d)(\d{2,4})(?!\d)", normalized):
        number = re.search(r"(?<!\d)(\d{2,4})(?!\d)", normalized).group(1)
        if any(token in normalized for token in ("资产", "存放", "房间", "机房", "室", "库位", "位置", "地点")):
            return number
    normalized = re.sub(r"^(请问|请|帮我|帮忙|告诉我|告诉|查询|查看|列出|统计|展示|给我)+", "", normalized).strip()
    normalized = re.sub(r"^(带有|具有|含有|有着|存在的|存在)+", "", normalized).strip()
    normalized = re.sub(r"(一共有多少|总共有多少|一共有|总共有|总共|一共)", " ", normalized)
    normalized = re.sub(r"(所有|全部|全体|统计下|统计)", " ", normalized)
    normalized = re.sub(r"(有多少个|有多少条|有多少人|有多少|多少个|多少条|多少人|多少|几个人|几条|几个)", " ", normalized)
    normalized = re.sub(r"(分别有哪些|分别是什么|分别是|有哪些|是什么|是多少|哪些)", " ", normalized)
    for tail in (
        "点位",
        "监控",
        "摄像头",
        "记录",
        "明细",
        "清单",
        "列表",
        "详情",
        "ip地址",
        "ip",
        "地址",
        "型号",
        "品牌",
        "传输方式",
        "传输",
        "位置",
        "地点",
        "桩号",
        "个数",
        "数量",
        "人员",
        "人",
        "什么",
    ):
        normalized = normalized.replace(tail, " ")
    normalized = normalized.replace("的", " ")
    phrases = [canonicalize_value(token).strip() for token in QUERY_TOKEN_PATTERN.findall(normalized) if canonicalize_value(token).strip()]
    if not phrases:
        return ""
    cleaned: list[str] = []
    for token in phrases:
        token = re.sub(r"^(个|条|位|人)+", "", token)
        token = re.sub(r"(有)+$", "", token)
        for term in ENTITY_OBJECT_TERMS:
            if token.endswith(term) and len(token) > len(term):
                token = token[: -len(term)]
                break
        if token in {"统计", "统计下", "所有", "全部", "全体", "什么", "存放", "相关", "信息"}:
            continue
        token = token.strip()
        if token:
            cleaned.append(token)
    if cleaned:
        phrases = cleaned
    phrases.sort(key=lambda item: (len(normalize_compact(item)), len(item)), reverse=True)
    return normalize_subject_phrase(phrases[0])


def infer_dedupe_key(question: str, answer_field: str | None) -> str | None:
    normalized = normalize_text(question)
    if any(token in normalized for token in ("ip", "ip地址")):
        if any(token in normalized for token in ENTITY_OBJECT_TERMS):
            return None
        return "ip"
    if answer_field == "phone" and any(token in normalized for token in COUNT_TERMS):
        return "phone"
    if "多少人" in normalized or "几个人" in normalized or "人数" in normalized:
        return "person"
    if answer_field == "owner" and any(token in normalized for token in COUNT_TERMS):
        return "person"
    return None


def infer_existence_fields(question: str, answer_field: str | None) -> tuple[str, ...]:
    normalized = normalize_text(question)
    fields: list[str] = []
    if answer_field in {"ip", "phone"} and any(term in normalized for term in COUNT_TERMS):
        fields.append(answer_field)
    if any(term in normalized for term in EXISTS_TERMS):
        if answer_field in {"ip", "phone", "owner", "department", "transport", "brand", "model", "location"}:
            fields.append(answer_field)
    if "独立的ip" in normalized or "有ip" in normalized or "具有ip" in normalized:
        fields.append("ip")
    if any(token in normalized for token in ("有电话", "有手机号", "有手机", "有联系方式", "具有电话", "具有手机号")):
        fields.append("phone")
    deduped: list[str] = []
    for field in fields:
        if field not in deduped:
            deduped.append(field)
    return tuple(deduped)


def is_detail_style(question: str, answer_field: str | None, detail_subject: str) -> bool:
    normalized = normalize_text(question)
    if not answer_field or not detail_subject:
        return False
    if detail_subject in {"有", "具有", "带有", "存在", "非空"}:
        return False
    if answer_field == "entity_name" and any(token in normalized for token in ENTITY_OBJECT_TERMS):
        return False
    if any(term in normalized for term in ("有几个", "多少个", "多少条", "有多少", "个数", "数量")):
        return normalized.endswith("是多少") or normalized.endswith("是什么")
    if any(term in normalized for term in LIST_TERMS):
        return False
    return "的" in normalized or normalized.endswith("是多少") or normalized.endswith("是什么")


def infer_filter_fields(question: str, answer_field: str | None, subject: str, detail_style: bool) -> tuple[str, ...]:
    normalized = normalize_text(question)
    ordered: list[str] = []

    def add(field: str) -> None:
        if field not in ordered:
            ordered.append(field)

    if detail_style and answer_field:
        for field in DETAIL_SUBJECT_FIELDS:
            add(field)
        return tuple(ordered)

    if subject and re.fullmatch(r"\d{2,4}", normalize_compact(subject)):
        add("location")

    if answer_field == "ip" and not subject:
        return tuple()

    if any(hint in normalized for hint in FIELD_HINTS["department"]):
        add("department")
    if any(hint in normalized for hint in FIELD_HINTS["location"]):
        add("location")
    if any(hint in normalized for hint in FIELD_HINTS["brand"]):
        add("brand")
    if any(hint in normalized for hint in FIELD_HINTS["model"]):
        add("model")
    if any(hint in normalized for hint in FIELD_HINTS["transport"]):
        add("transport")
    if any(hint in normalized for hint in FIELD_HINTS["status"]):
        add("status")
    if any(hint in normalized for hint in FIELD_HINTS["owner"]):
        add("owner")
    if subject and any(hint in normalized for hint in FIELD_HINTS["entity_name"]):
        add("entity_name")

    if not ordered and subject:
        for field in ("department", "location", "status", "owner", "entity_name"):
            add(field)
    return tuple(ordered)


def detect_intent(question: str, answer_field: str | None, group_field: str | None, detail_style: bool, subject: str) -> str:
    normalized = normalize_text(question)
    if group_field:
        return "group"
    if any(term in normalized for term in LIST_TERMS):
        return "list"
    if "什么" in normalized and not answer_field:
        return "list"
    if detail_style:
        return "detail"
    if (
        any(term in normalized for term in COUNT_TERMS)
        or "统计" in normalized
        or "数量" in normalized
        or "个数" in normalized
        or answer_field == "quantity"
    ):
        return "count"
    if answer_field == "entity_name" and subject and any(token in normalized for token in ENTITY_OBJECT_TERMS):
        return "list"
    if answer_field:
        return "detail"
    return "list"


def make_query_plan(question: str) -> QueryPlan:
    answer_field = infer_answer_field(question)
    group_field = infer_group_field(question)
    detail_subject = extract_subject(question, answer_field)
    detail_style = is_detail_style(question, answer_field, detail_subject)
    subject = detail_subject if detail_style else extract_primary_subject(question)
    subject = normalize_subject_phrase(subject, answer_field)
    if subject in ENTITY_OBJECT_TERMS or subject in {"所", "统计所", "所有", "全部", "全体", "什么"}:
        subject = ""
    if subject and not re.search(r"\d", subject) and len(normalize_compact(subject)) < 2:
        subject = ""
    dedupe_key = infer_dedupe_key(question, answer_field)
    normalized_question = normalize_text(question)
    object_query = any(token in normalized_question for token in ENTITY_OBJECT_TERMS)
    existence_fields = infer_existence_fields(question, answer_field)
    # In count queries like "有IP地址的摄像头有几个", IP/电话 is a filter condition,
    # not the requested answer field. Keep the existence constraint, but let the
    # object term drive ledger selection and counting.
    if (
        answer_field in {"ip", "phone"}
        and any(term in normalized_question for term in COUNT_TERMS)
        and object_query
        and answer_field in existence_fields
    ):
        answer_field = "entity_name"
    free_terms = list(split_query_terms(subject or question))
    if existence_fields and answer_field not in existence_fields:
        existence_hint_compact = {
            normalize_compact(hint)
            for field in existence_fields
            for hint in FIELD_HINTS.get(field, ())
        }
        free_terms = [
            term
            for term in free_terms
            if normalize_compact(term) not in existence_hint_compact
        ]
    if answer_field == "ip":
        free_terms = [term for term in free_terms if term != "地址"]
    explicit_global_scope = any(term in normalized_question for term in GLOBAL_TERMS)
    return QueryPlan(
        question=question,
        intent=detect_intent(question, answer_field, group_field, detail_style, subject),
        answer_field=answer_field,
        group_field=group_field,
        dedupe_key=dedupe_key,
        global_scope=(
            explicit_global_scope
            or (dedupe_key == "ip" and not subject)
        ),
        existence_fields=existence_fields,
        subject=subject,
        filter_fields=infer_filter_fields(question, answer_field, subject, detail_style),
        free_terms=tuple(free_terms),
    )


def has_explicit_global_scope(question: str) -> bool:
    normalized = normalize_text(question)
    return any(term in normalized for term in GLOBAL_TERMS)


def infer_target_types(question: str, plan: QueryPlan) -> tuple[str, ...]:
    normalized = normalize_text(question)
    targets: list[str] = []

    def add(target: str) -> None:
        if target not in targets:
            targets.append(target)

    object_query = any(token in normalized for token in ENTITY_OBJECT_TERMS)

    if any(token in normalized for token in CATEGORY_TARGET_HINTS["camera"]):
        add("camera")
    if any(token in normalized for token in CATEGORY_TARGET_HINTS["contact"]) or plan.answer_field == "phone" or plan.dedupe_key == "phone":
        add("contact")
    if any(token in normalized for token in CATEGORY_TARGET_HINTS["asset"]):
        add("asset")
    if (
        any(token in normalized for token in CATEGORY_TARGET_HINTS["network"])
        or plan.answer_field == "ip"
        or plan.dedupe_key == "ip"
    ) and not ("camera" in targets and object_query):
        add("network")
    return tuple(targets)


def is_ledger_inventory_query(plan: QueryPlan, requested_ledger: str) -> bool:
    if requested_ledger not in {"", "auto"}:
        return False
    if plan.subject or plan.group_field or plan.answer_field == "quantity":
        return False
    if infer_target_types(plan.question, plan):
        return False
    normalized = normalize_text(plan.question)
    if not any(term in normalized for term in COUNT_TERMS):
        return False
    return any(token in normalized for token in ("台账", "账本"))


def is_bare_field_query(plan: QueryPlan) -> bool:
    if not plan.answer_field or plan.subject or plan.group_field:
        return False
    normalized = normalize_text(plan.question)
    if any(term in normalized for term in COUNT_TERMS):
        return False
    if any(term in normalized for term in LIST_TERMS):
        return False
    if any(term in normalized for term in GROUP_TERMS):
        return False
    if has_explicit_global_scope(plan.question):
        return False
    compact_question = normalize_compact(plan.question)
    if not compact_question:
        return False
    compact_hints = {normalize_compact(hint) for hint in ANSWER_FIELD_HINTS.get(plan.answer_field, FIELD_HINTS.get(plan.answer_field, ()))}
    return compact_question in compact_hints


def should_sum_quantity(question: str, plan: QueryPlan) -> bool:
    normalized = normalize_text(question)
    if plan.answer_field == "quantity":
        return True
    if any(term in normalized for term in QUANTITY_SUM_TERMS):
        return True
    return bool(re.search(r"多少(台|项|件|部|套)", normalized))


def subject_query_variants(subject: str) -> tuple[tuple[str, str], ...]:
    variants: list[tuple[str, str]] = []

    def add(kind: str, value: str) -> None:
        if value and (kind, value) not in variants:
            variants.append((kind, value))

    add("compact", normalize_compact(subject))
    add("pinyin", to_pinyin_text(subject))
    initials = pinyin_initials(subject)
    if len(initials) >= 2:
        add("initials", initials)
    return tuple(variants)


def score_alias_match(kind: str, query_value: str, alias: str) -> float:
    if not query_value or not alias:
        return 0.0
    if query_value == alias:
        return {"compact": 1.0, "pinyin": 0.96, "initials": 0.72}.get(kind, 0.92)
    shortest = min(len(query_value), len(alias))
    if kind == "compact":
        if shortest >= 3 and (query_value in alias or alias in query_value):
            return 0.88
        ratio = SequenceMatcher(None, query_value, alias).ratio()
        if shortest >= 2 and ratio >= 0.82:
            return 0.72
        return 0.0
    if kind == "pinyin":
        if shortest >= 4 and (query_value in alias or alias in query_value):
            return 0.84
        ratio = SequenceMatcher(None, query_value, alias).ratio()
        if shortest >= 4 and ratio >= 0.9:
            return 0.74
        return 0.0
    if kind == "initials":
        if shortest >= 3 and (query_value.startswith(alias) or alias.startswith(query_value)):
            return 0.58
        return 0.0
    return 0.0


def score_ledger_name_match(ledger_name: str, question: str, terms: tuple[str, ...]) -> float:
    compact_name = normalize_compact(ledger_name)
    pinyin_name = to_pinyin_text(ledger_name)
    score = 0.0
    for term in terms:
        compact_term = normalize_compact(term)
        pinyin_term = to_pinyin_text(term)
        if compact_term and compact_term in compact_name:
            score += 1.4 if len(compact_term) >= 2 else 0.4
        elif pinyin_term and pinyin_term in pinyin_name:
            score += 1.2 if len(pinyin_term) >= 4 else 0.3
    normalized = normalize_text(question)
    for token in ENTITY_OBJECT_TERMS:
        if token in normalized and token in ledger_name:
            score += 1.8
    if "摄像头" in normalized or "监控" in normalized or "点位" in normalized or "桩号" in normalized:
        if any(token in ledger_name for token in ("摄像头", "监控", "点位", "设备")):
            score += 0.8
    if "ip" in normalized or "ip地址" in normalized:
        if "ip" in ledger_name.lower():
            score += 1.6
    return score


def score_ledger_profile(plan: QueryPlan, ledger_name: str, rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    profile = build_ledger_profile(ledger_name, rows)
    ratios = profile["ratios"]
    semantic_type_scores = profile.get("semantic_type_scores", {})
    semantic_terms = {normalize_compact(term) for term in profile.get("semantic_terms", []) if term}
    example_values = profile.get("value_examples", {})
    normalized = normalize_text(plan.question)
    object_query = any(token in normalized for token in ENTITY_OBJECT_TERMS)
    target_types = set(infer_target_types(plan.question, plan))
    person_query = (
        any(hint in normalized for hint in FIELD_HINTS["owner"])
        or any(token in normalized for token in ("多少人", "几个人", "人数", "联系人", "电话"))
    )
    location_query = any(hint in normalized for hint in FIELD_HINTS["location"])

    score = 0.0
    if object_query and not person_query:
        score += ratios["location"] * 1.8
        score += ratios["brand"] * 1.0
        score += ratios["model"] * 0.9
        score += ratios["transport"] * 0.9
        if plan.answer_field == "ip" or "ip" in normalized:
            score += ratios["ip"] * 0.6
    if location_query:
        score += ratios["location"] * 1.2
    if plan.answer_field == "ip" and not object_query:
        score += ratios["owner"] * 1.0
        score += ratios["department"] * 0.5
        score += ratios["ip"] * 0.5
    if plan.answer_field == "phone":
        score += ratios["phone"] * 2.2
        score += ratios["entity_name"] * 1.0
        score += ratios["owner"] * 0.9
    if plan.answer_field == "owner" or person_query or plan.dedupe_key == "person":
        score += ratios["owner"] * 1.5
        score += ratios["department"] * 0.8
    if question_mentions_ledger(plan.question, ledger_name):
        score += 3.0

    bounded_type_scores = {
        kind: math.log1p(max(0.0, float(value)))
        for kind, value in semantic_type_scores.items()
    }
    if object_query:
        score += bounded_type_scores.get("camera", 0.0) * 0.35
    if plan.answer_field == "phone" or "电话" in normalized or "手机号" in normalized:
        score += bounded_type_scores.get("contact", 0.0) * 0.45
    if "资产" in normalized or "盘点" in normalized or "品牌" in normalized:
        score += bounded_type_scores.get("asset", 0.0) * 0.35
    if "ip" in normalized or "ip地址" in normalized:
        score += bounded_type_scores.get("network", 0.0) * 0.25

    dominant_type = canonicalize_value(profile.get("dominant_type", ""))
    if target_types:
        if dominant_type in target_types:
            score += 2.2
        elif target_types == {"camera"} and dominant_type in {"network", "contact", "asset"}:
            score -= 2.4
        elif target_types == {"contact"} and dominant_type in {"camera", "asset"}:
            score -= 1.8
        elif target_types == {"asset"} and dominant_type in {"camera", "contact"}:
            score -= 1.6
        elif target_types == {"network"} and dominant_type in {"camera", "contact"}:
            score -= 1.3

    for token in plan.free_terms or (() if not plan.subject else (plan.subject,)):
        compact_token = normalize_compact(token)
        if not compact_token:
            continue
        if compact_token in semantic_terms:
            score += 0.55
            continue
        for values in example_values.values():
            if any(compact_token in normalize_compact(value) or normalize_compact(value) in compact_token for value in values[:40]):
                score += 0.35
                break
    return score


def match_value_candidates(
    question: str,
    subject: str,
    ledgers: list[str],
    rows_by_ledger: dict[str, list[dict[str, Any]]],
    filter_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    if not subject:
        return []
    query_variants = subject_query_variants(subject)
    preferred_fields = list(filter_fields)
    normalized_question = normalize_text(question)

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for ledger_name in ledgers:
        index = build_ledger_index(ledger_name, rows_by_ledger.get(ledger_name, []))
        for entry in index["entries"]:
            if preferred_fields and entry["field"] not in preferred_fields:
                continue
            best = 0.0
            for alias_kind, alias in entry["aliases"]:
                for query_kind, query_value in query_variants:
                    if alias_kind != query_kind:
                        continue
                    best = max(best, score_alias_match(query_kind, query_value, alias))
            if best <= 0.0:
                continue
            if entry["field"] in preferred_fields:
                best += 0.08
                for hint in FIELD_HINTS.get(entry["field"], ()):
                    if hint in normalized_question and hint in normalize_text(entry["value"]):
                        best += 0.18
                        break
            if entry["field"] == "generic":
                best -= 0.06
            normalized_key = (ledger_name, entry["field"], entry["value"])
            if normalized_key in seen:
                continue
            seen.add(normalized_key)
            candidates.append(
                {
                    "ledger_name": ledger_name,
                    "field": entry["field"],
                    "value": entry["value"],
                    "count": entry["count"],
                    "score": round(best, 6),
                }
            )
    candidates.sort(key=lambda item: (item["score"], -item["count"], item["field"]), reverse=True)
    return candidates


def select_ledgers(plan: QueryPlan, requested_ledger: str, rows_by_ledger: dict[str, list[dict[str, Any]]]) -> tuple[list[str], list[str], str]:
    available = [ledger_name for ledger_name in sorted(rows_by_ledger) if rows_by_ledger[ledger_name]]
    if requested_ledger not in {"", "auto"}:
        normalized = normalize_ledger_name(requested_ledger)
        return ([normalized] if normalized in rows_by_ledger else []), [], "user_selected"

    candidates = [ledger_name for ledger_name in available if ledger_name not in IGNORED_AUTO_LEDGERS]
    ignored = [ledger_name for ledger_name in available if ledger_name in IGNORED_AUTO_LEDGERS]
    if not candidates:
        return available, ignored, "fallback_all"
    ledger_terms = plan.free_terms or ((plan.subject,) if plan.subject else tuple())
    object_query = any(token in normalize_text(plan.question) for token in ENTITY_OBJECT_TERMS)
    object_tokens_in_question = [token for token in ENTITY_OBJECT_TERMS if token in normalize_text(plan.question)]
    normalized_question = normalize_text(plan.question)
    target_types = set(infer_target_types(plan.question, plan))

    def filter_target_ledgers(selected_ledgers: list[str]) -> list[str]:
        if requested_ledger not in {"", "auto"} or not target_types or plan.global_scope:
            return selected_ledgers
        typed_ledgers = [
            ledger_name
            for ledger_name in selected_ledgers
            if canonicalize_value(build_ledger_profile(ledger_name, rows_by_ledger.get(ledger_name, [])).get("dominant_type", "")) in target_types
        ]
        if typed_ledgers:
            return typed_ledgers
        fallback_typed_ledgers = [
            ledger_name
            for ledger_name in candidates
            if canonicalize_value(build_ledger_profile(ledger_name, rows_by_ledger.get(ledger_name, [])).get("dominant_type", "")) in target_types
        ]
        return fallback_typed_ledgers or selected_ledgers

    def collapse_duplicate_families(selected_ledgers: list[str], mode: str) -> tuple[list[str], list[str], str]:
        if requested_ledger not in {"", "auto"} or len(selected_ledgers) < 2:
            return selected_ledgers, [], mode
        by_family: dict[str, list[str]] = defaultdict(list)
        for ledger_name in selected_ledgers:
            by_family[ledger_family_name(ledger_name, available)].append(ledger_name)
        kept: list[str] = []
        suppressed: list[str] = []
        for family_name, members in by_family.items():
            members.sort(
                key=lambda ledger_name: (
                    normalize_ledger_name(ledger_name) != family_name,
                    -len(rows_by_ledger.get(ledger_name, [])),
                    ledger_name,
                )
            )
            kept.append(members[0])
            suppressed.extend(members[1:])
        if suppressed:
            mode = f"{mode}_family"
        return kept, suppressed, mode

    if plan.dedupe_key == "phone" and not plan.subject and not any(term in normalized_question for term in GLOBAL_TERMS):
        contact_ledgers: list[tuple[float, str]] = []
        fallback_ledgers: list[tuple[float, str]] = []
        for ledger_name in candidates:
            profile = build_ledger_profile(ledger_name, rows_by_ledger.get(ledger_name, []))
            semantic_scores = profile.get("semantic_type_scores", {})
            ratio = float(profile["ratios"].get("phone", 0.0))
            score = ratio * 2.5 + math.log1p(float(semantic_scores.get("contact", 0.0)))
            if profile.get("dominant_type") == "contact":
                score += 2.0
                contact_ledgers.append((score, ledger_name))
            elif score > 1.2:
                # Only use non-contact ledgers as fallback when no通讯录/电话本型账本 exists.
                fallback_ledgers.append((score, ledger_name))
        preferred_ledgers = contact_ledgers or fallback_ledgers
        if preferred_ledgers:
            preferred_ledgers.sort(reverse=True)
            selected = [ledger_name for _, ledger_name in preferred_ledgers if ledger_name not in MIXED_LEDGERS] or [ledger_name for _, ledger_name in preferred_ledgers]
            selected = filter_target_ledgers(selected)
            selected, suppressed, mode = collapse_duplicate_families(selected[:4], "auto_phone_preferred")
            excluded = [ledger_name for ledger_name in available if ledger_name not in selected] + suppressed
            return selected[:4], excluded, mode

    if plan.global_scope and plan.dedupe_key == "ip":
        selected = [ledger_name for ledger_name in candidates if ledger_name not in MIXED_LEDGERS]
        if not selected:
            selected = candidates
        selected, suppressed, mode = collapse_duplicate_families(selected, "auto_global")
        excluded = ignored + [name for name in available if name in MIXED_LEDGERS and name not in selected] + suppressed
        return selected, excluded, mode

    if plan.global_scope or plan.intent == "group":
        selected = [ledger_name for ledger_name in candidates if ledger_name not in MIXED_LEDGERS]
        if not selected:
            selected = candidates
        selected, suppressed, mode = collapse_duplicate_families(selected, "auto_global")
        excluded = ignored + [name for name in available if name in MIXED_LEDGERS and name not in selected] + suppressed
        return selected, excluded, mode

    scored: list[tuple[float, str]] = []
    field_candidates = match_value_candidates(plan.question, plan.subject, candidates, rows_by_ledger, plan.filter_fields)
    scores_by_ledger: Counter[str] = Counter()
    for ledger_name in candidates:
        scores_by_ledger[ledger_name] += score_ledger_name_match(ledger_name, plan.question, ledger_terms)
        scores_by_ledger[ledger_name] += score_ledger_profile(plan, ledger_name, rows_by_ledger.get(ledger_name, []))
    for candidate in field_candidates[:20]:
        scores_by_ledger[candidate["ledger_name"]] += candidate["score"] + min(candidate["count"], 20) / 50.0
        if candidate["field"] in plan.filter_fields:
            scores_by_ledger[candidate["ledger_name"]] += 0.25
        if plan.answer_field == "phone" and candidate["field"] in {"owner", "entity_name"}:
            scores_by_ledger[candidate["ledger_name"]] += 0.35

    if plan.free_terms:
        for ledger_name in candidates:
            blob = " ".join(row["_search_blob"] for row in rows_by_ledger[ledger_name])
            compact_blob = normalize_compact(blob)
            pinyin_blob = to_pinyin_text(blob)
            score = 0.0
            for term in plan.free_terms:
                compact_term = normalize_compact(term)
                pinyin_term = to_pinyin_text(term)
                if compact_term and compact_term in compact_blob:
                    score += 0.5
                elif pinyin_term and pinyin_term in pinyin_blob:
                    score += 0.45
            scores_by_ledger[ledger_name] += score

    for ledger_name, score in scores_by_ledger.items():
        if score > 0:
            if ledger_name in MIXED_LEDGERS:
                score -= 0.35
            else:
                score += 0.15
            scored.append((score, ledger_name))

    if not scored:
        selected = [ledger_name for ledger_name in candidates if ledger_name not in MIXED_LEDGERS] or candidates
        return selected[:1], ignored + [name for name in available if name in MIXED_LEDGERS and name not in selected[:1]], "auto_fallback"

    scored.sort(reverse=True)
    best_score, best_ledger = scored[0]
    if plan.answer_field == "phone" or plan.intent == "detail" or plan.subject:
        def filter_object_ledgers(selected_ledgers: list[str]) -> list[str]:
            if not object_query:
                return selected_ledgers
            named_object_ledgers = [
                ledger_name
                for ledger_name in selected_ledgers
                if any(token in ledger_name for token in object_tokens_in_question)
            ]
            if named_object_ledgers:
                return named_object_ledgers
            object_ledgers = [
                ledger_name
                for ledger_name in selected_ledgers
                if score_ledger_name_match(ledger_name, plan.question, ledger_terms) > 0
            ]
            return object_ledgers or selected_ledgers

        exact_ledgers: list[str] = []
        for candidate in field_candidates:
            if candidate["score"] < 0.95:
                continue
            if plan.answer_field == "phone":
                phone_ratio = build_ledger_profile(candidate["ledger_name"], rows_by_ledger.get(candidate["ledger_name"], []))["ratios"]["phone"]
                if phone_ratio <= 0:
                    continue
            if candidate["ledger_name"] not in exact_ledgers and candidate["ledger_name"] not in MIXED_LEDGERS:
                exact_ledgers.append(candidate["ledger_name"])
        if exact_ledgers:
            exact_ledgers.sort(
                key=lambda ledger_name: (
                    not question_mentions_ledger(plan.question, ledger_name),
                    -scores_by_ledger[ledger_name],
                    ledger_name,
                )
            )
            selected = filter_target_ledgers(filter_object_ledgers(exact_ledgers))[:4]
            selected, suppressed, mode = collapse_duplicate_families(selected, "auto_subject_exact")
            excluded = [ledger_name for ledger_name in available if ledger_name not in selected] + suppressed
            return selected, excluded, mode
        threshold = max(best_score * 0.72, best_score - 0.55)
        selected = [ledger_name for score, ledger_name in scored if score >= threshold]
        selected = [ledger_name for ledger_name in selected if ledger_name not in MIXED_LEDGERS] or selected
        selected = filter_object_ledgers(selected)
        selected = filter_target_ledgers(selected)
        selected = selected[:4] or [best_ledger]
        selected, suppressed, mode = collapse_duplicate_families(selected, "auto_multi")
        excluded = [ledger_name for ledger_name in available if ledger_name not in selected] + suppressed
        return selected, excluded, mode

    if plan.free_terms or plan.existence_fields:
        threshold = max(best_score * 0.78, best_score - 0.6)
        selected = [ledger_name for score, ledger_name in scored if score >= threshold]
        selected = [ledger_name for ledger_name in selected if ledger_name not in MIXED_LEDGERS] or selected
        if object_query:
            named_object_ledgers = [
                ledger_name
                for ledger_name in selected
                if any(token in ledger_name for token in object_tokens_in_question)
            ]
            if named_object_ledgers:
                selected = named_object_ledgers
            object_ledgers = [
                ledger_name
                for ledger_name in selected
                if score_ledger_name_match(ledger_name, plan.question, ledger_terms) > 0
            ]
        if object_ledgers:
            selected = object_ledgers
        selected = filter_target_ledgers(selected)
        selected = selected[:4] or [best_ledger]
        selected, suppressed, mode = collapse_duplicate_families(selected, "auto_multi")
        excluded = [ledger_name for ledger_name in available if ledger_name not in selected] + suppressed
        return selected, excluded, mode

    selected = filter_target_ledgers([best_ledger])
    selected, suppressed, mode = collapse_duplicate_families(selected, "auto_best")
    excluded = [ledger_name for ledger_name in available if ledger_name not in selected] + suppressed
    return selected, excluded, mode


def row_matches_terms(row: dict[str, Any], terms: tuple[str, ...]) -> bool:
    if not terms:
        return True
    compact_blob = row["_search_compact"]
    pinyin_blob = row["_search_pinyin"]
    for term in terms:
        compact_term = normalize_compact(term)
        pinyin_term = to_pinyin_text(term)
        if compact_term and compact_term in compact_blob:
            continue
        if pinyin_term and pinyin_term in pinyin_blob:
            continue
        return False
    return True


def select_filter_candidates(plan: QueryPlan, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not candidates:
        return []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate["field"]].append(candidate)

    if plan.intent == "detail" and plan.filter_fields:
        best_score = candidates[0]["score"]
        detail_candidates: list[dict[str, Any]] = []
        for field in plan.filter_fields:
            field_candidates = grouped.get(field, [])
            if not field_candidates:
                continue
            field_candidates.sort(key=lambda item: (item["score"], len(item["value"]), item["count"]), reverse=True)
            detail_candidates.extend(
                item for item in field_candidates if item["score"] >= best_score - 0.06
            )
        if detail_candidates:
            unique: list[dict[str, Any]] = []
            seen: set[tuple[str, str, str]] = set()
            for item in detail_candidates:
                key = (item["ledger_name"], item["field"], item["value"])
                if key not in seen:
                    seen.add(key)
                    unique.append(item)
            return unique

    for field in plan.filter_fields:
        field_candidates = grouped.get(field, [])
        if not field_candidates:
            continue
        field_candidates.sort(key=lambda item: (item["score"], len(item["value"]), item["count"]), reverse=True)
        best_score = field_candidates[0]["score"]
        return [item for item in field_candidates if item["score"] >= best_score - 0.06]

    best_score = candidates[0]["score"]
    return [item for item in candidates if item["score"] >= best_score - 0.04]


def apply_candidate_filters(rows: list[dict[str, Any]], candidates: list[dict[str, Any]], *, combine_mode: str = "and") -> list[dict[str, Any]]:
    if not candidates:
        return rows

    chosen: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        chosen[candidate["field"]].append(candidate)

    if combine_mode == "or":
        matched_rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for row in rows:
            for field, field_candidates in chosen.items():
                field_value = canonicalize_value(row.get(field, ""))
                field_compact = normalize_compact(field_value)
                field_pinyin = to_pinyin_text(field_value)
                for candidate in field_candidates:
                    value_compact = normalize_compact(candidate["value"])
                    value_pinyin = to_pinyin_text(candidate["value"])
                    compact_match = value_compact and (
                        value_compact == field_compact
                        or (
                            field == "location"
                            and field_compact
                            and (value_compact in field_compact or field_compact in value_compact)
                        )
                        or (field != "location" and len(value_compact) >= 4 and value_compact in field_compact)
                    )
                    pinyin_match = value_pinyin and (
                        value_pinyin == field_pinyin
                        or (
                            field == "location"
                            and field_pinyin
                            and (value_pinyin in field_pinyin or field_pinyin in value_pinyin)
                        )
                        or (field != "location" and len(value_pinyin) >= 6 and value_pinyin in field_pinyin)
                    )
                    if compact_match or pinyin_match:
                        record_id = canonicalize_value(row.get("record_id", ""))
                        if record_id not in seen_ids:
                            seen_ids.add(record_id)
                            matched_rows.append(row)
                        break
                else:
                    continue
                break
        return matched_rows

    filtered = rows
    for field, field_candidates in chosen.items():
        next_rows: list[dict[str, Any]] = []
        for row in filtered:
            field_value = canonicalize_value(row.get(field, ""))
            field_compact = normalize_compact(field_value)
            field_pinyin = to_pinyin_text(field_value)
            for candidate in field_candidates:
                value_compact = normalize_compact(candidate["value"])
                value_pinyin = to_pinyin_text(candidate["value"])
                if field == "location":
                    compact_match = value_compact and field_compact and (value_compact in field_compact or field_compact in value_compact)
                    pinyin_match = value_pinyin and field_pinyin and (value_pinyin in field_pinyin or field_pinyin in value_pinyin)
                else:
                    compact_match = value_compact and (
                        value_compact == field_compact or (len(value_compact) >= 4 and value_compact in field_compact)
                    )
                    pinyin_match = value_pinyin and (
                        value_pinyin == field_pinyin or (len(value_pinyin) >= 6 and value_pinyin in field_pinyin)
                    )
                if compact_match:
                    next_rows.append(row)
                    break
                if pinyin_match:
                    next_rows.append(row)
                    break
        filtered = next_rows
        if not filtered:
            break
    return filtered


def apply_existence_filters(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    if not fields:
        return rows
    output = rows
    for field in fields:
        output = [row for row in output if row_has_field_value(row, field)]
    return output


def row_richness_score(row: dict[str, Any]) -> int:
    flat = flatten_record_for_export(row)
    ignored = {"record_id", "base_id", "ledger_name", "source_file", "source_type", "content_hash", "updated_at", "version_no", "is_deleted", "extra_json"}
    return sum(1 for key, value in flat.items() if key not in ignored and canonicalize_value(value))


def collapse_duplicate_family_rows(rows: list[dict[str, Any]], selected_ledgers: list[str], requested_ledger: str) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if requested_ledger not in {"", "auto"} or len(selected_ledgers) < 2:
        return rows, None

    normalized_ledgers = [normalize_ledger_name(name) for name in selected_ledgers]
    family_counts = Counter(ledger_family_name(name, normalized_ledgers) for name in normalized_ledgers)
    duplicate_families = {family for family, count in family_counts.items() if count > 1}
    if not duplicate_families:
        return rows, None

    kept: dict[tuple[str, str], dict[str, Any]] = {}
    family_stats: dict[str, dict[str, Any]] = {}
    untouched: list[dict[str, Any]] = []

    for row in rows:
        ledger_name = normalize_ledger_name(row.get("ledger_name", ""))
        family = ledger_family_name(ledger_name, normalized_ledgers)
        if family not in duplicate_families:
            untouched.append(row)
            continue
        base_id = canonicalize_value(row.get("base_id", "")) or canonicalize_value(row.get("record_id", ""))
        key = (family, base_id)
        stats = family_stats.setdefault(family, {"family": family, "raw_count": 0, "unique_count": 0})
        stats["raw_count"] += 1
        current = kept.get(key)
        if current is None:
            kept[key] = row
            continue
        current_ledger = normalize_ledger_name(current.get("ledger_name", ""))
        current_score = (
            int(current_ledger == family),
            row_richness_score(current),
            canonicalize_value(current.get("updated_at", "")),
            current_ledger,
        )
        candidate_score = (
            int(ledger_name == family),
            row_richness_score(row),
            canonicalize_value(row.get("updated_at", "")),
            ledger_name,
        )
        if candidate_score > current_score:
            kept[key] = row

    collapsed = untouched + list(kept.values())
    for family, stats in family_stats.items():
        stats["unique_count"] = sum(1 for fam, _ in kept if fam == family)
        stats["duplicate_count"] = max(0, stats["raw_count"] - stats["unique_count"])
    summary = {"families": sorted(family_stats.values(), key=lambda item: item["family"])} if family_stats else None
    return collapsed, summary


def distinct_field_values(rows: list[dict[str, Any]], field: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for _, value in row_field_entries(row, field):
            if value and value not in seen:
                seen.add(value)
                values.append(value)
    return values


def project_distinct_field_rows(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    projected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        entries = row_field_entries(row, field)
        for index, (label, value) in enumerate(entries, start=1):
            if not value or value in seen:
                continue
            seen.add(value)
            copy_row = dict(row)
            copy_row[field] = value
            copy_row["_projected_field_label"] = extra_key_base(label) or ANSWER_FIELD_LABELS.get(field, field)
            if len(entries) > 1:
                base_record_id = canonicalize_value(row.get("record_id", "")) or canonicalize_value(row.get("base_id", ""))
                copy_row["record_id"] = f"{base_record_id}::{field}::{index}"
            projected.append(copy_row)
    return projected


def dedupe_rows(rows: list[dict[str, Any]], dedupe_key: str | None) -> list[dict[str, Any]]:
    if dedupe_key is None:
        return rows
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        if dedupe_key == "ip":
            keys = [value for _, value in row_field_entries(row, "ip")]
        elif dedupe_key == "phone":
            keys = [value for _, value in row_field_entries(row, "phone")]
        else:
            key = canonicalize_value(row.get("owner", "")) or canonicalize_value(row.get("entity_name", ""))
            keys = [key] if key else []
        if not keys:
            continue
        fresh = False
        for key in keys:
            if key and key not in seen:
                seen.add(key)
                fresh = True
        if fresh:
            output.append(row)
    return output


def is_placeholder_title_value(row: dict[str, Any], field: str, value: str) -> bool:
    text = normalize_display_text(value)
    if not text:
        return True

    extras = safe_json_loads(row.get("extra_json"))
    sheet_name = normalize_display_text(extras.get("__sheet_name__", ""))
    lowered = normalize_text(text)
    if field == "entity_name":
        if sheet_name and lowered == normalize_text(sheet_name):
            return True
        if DISPLAY_PLACEHOLDER_PATTERN.fullmatch(text):
            return True
        if lowered in {"汇总", "summary", "总表"}:
            return True
    if field == "department" and lowered in {"汇总", "summary", "总表"}:
        return True
    return False


def display_value_for_field(row: dict[str, Any], field: str) -> str:
    value = normalize_display_text(row.get(field, ""))
    if not value:
        return ""
    if is_placeholder_title_value(row, field, value):
        return ""
    return value


def title_for_row(row: dict[str, Any]) -> str:
    for field in RECORD_TITLE_FIELDS:
        value = display_value_for_field(row, field)
        if value:
            return value
    return normalize_display_text(row.get("record_id", "")) or "Untitled Record"


def title_fields_for_plan(plan: QueryPlan) -> tuple[str, ...]:
    normalized = normalize_text(plan.question)
    if plan.answer_field in {"ip", "phone"} and plan.subject:
        return ("owner", "entity_name", "department", "location", "ip")
    if any(hint in normalized for hint in FIELD_HINTS["location"]):
        return ("location", "entity_name", "owner", "department", "ip")
    if plan.answer_field == "owner" or plan.dedupe_key == "person":
        return ("owner", "entity_name", "department", "location", "ip")
    return RECORD_TITLE_FIELDS


def title_for_row_with_fields(row: dict[str, Any], fields: tuple[str, ...]) -> str:
    for field in fields:
        value = display_value_for_field(row, field)
        if value:
            return value
    return title_for_row(row)


def subtitle_for_row(row: dict[str, Any], title: str) -> str:
    for field in RECORD_TITLE_FIELDS:
        value = display_value_for_field(row, field)
        if value and value != title:
            return value
    return normalize_display_text(row.get("ledger_name", ""))


def visible_pairs(row: dict[str, Any]) -> list[tuple[str, str]]:
    flattened = flatten_record_for_export(row)
    ordered_keys = [
        "record_id",
        "base_id",
        "entity_name",
        "location",
        "ip",
        "phone",
        "quantity",
        "department",
        "owner",
        "status",
        "transport",
        "brand",
        "model",
        "remark",
    ]
    pairs: list[tuple[str, str]] = []
    for key in ordered_keys:
        value = canonicalize_value(flattened.get(key, ""))
        if value:
            pairs.append((key, value))
    for key, value in flattened.items():
        if key.startswith("_") or key in ordered_keys or key in {"extra_json", "ledger_name", "source_file", "source_type", "version_no", "content_hash", "updated_at", "is_deleted"}:
            continue
        text = canonicalize_value(value)
        if text:
            pairs.append((key, text))
    return pairs[:14]


def prettify_key(key: str) -> str:
    labels = {
        "record_id": "记录ID",
        "base_id": "基础ID",
        "entity_name": "名称",
        "location": "位置",
        "ip": "IP",
        "phone": "电话",
        "quantity": "数量",
        "department": "部门",
        "owner": "负责人",
        "status": "状态",
        "transport": "传输",
        "brand": "品牌",
        "model": "型号",
        "remark": "备注",
    }
    return labels.get(key, key)


def infer_group_label(field: str) -> str:
    return {
        "department": "部门",
        "brand": "品牌",
        "model": "型号",
        "transport": "传输方式",
        "status": "状态",
    }.get(field, field)


def summarize_cross_ledger_dedupe(rows: list[dict[str, Any]], dedupe_key: str) -> dict[str, Any] | None:
    if dedupe_key is None:
        return None

    def dedupe_values(row: dict[str, Any]) -> list[str]:
        if dedupe_key == "ip":
            return [value for _, value in row_field_entries(row, "ip")]
        if dedupe_key == "phone":
            return [value for _, value in row_field_entries(row, "phone")]
        key = canonicalize_value(row.get("owner", "")) or canonicalize_value(row.get("entity_name", ""))
        return [key] if key else []

    ledger_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        ledger_map[normalize_ledger_name(row.get("ledger_name", ""))].append(row)

    per_ledger: list[dict[str, Any]] = []
    global_occurrences: dict[str, set[str]] = defaultdict(set)
    for ledger_name, ledger_rows in sorted(ledger_map.items()):
        values: list[str] = []
        for row in ledger_rows:
            values.extend(value for value in dedupe_values(row) if value)
        unique_values = sorted(set(values))
        for value in unique_values:
            global_occurrences[value].add(ledger_name)
        per_ledger.append(
            {
                "ledger_name": ledger_name,
                "raw_count": len(ledger_rows),
                "value_count": len(values),
                "unique_count": len(unique_values),
                "duplicate_count": max(0, len(values) - len(unique_values)),
            }
        )

    overlaps = [
        {"value": value, "ledgers": sorted(ledgers)}
        for value, ledgers in global_occurrences.items()
        if len(ledgers) > 1
    ]
    overlaps.sort(key=lambda item: (len(item["ledgers"]), item["value"]), reverse=True)
    payload = {
        "dedupe_label": "IP" if dedupe_key == "ip" else ("电话号码" if dedupe_key == "phone" else "人员"),
        "ledger_summaries": per_ledger,
        "total_unique": len(global_occurrences),
        "overlap_count": len(overlaps),
        "overlap_items": overlaps[:12],
    }
    if dedupe_key == "phone":
        payload["type_breakdown"] = distinct_phone_summary(rows)
    return payload


EXPORT_LABELS = {
    "ledger_name": "账本",
    "record_id": "记录ID",
    "entity_name": "名称",
    "location": "位置",
    "ip": "IP",
    "phone": "电话",
    "phone_landline": "办公/座机号码",
    "phone_mobile": "手机号码",
    "phone_other": "其他号码",
    "quantity": "数量",
    "department": "部门",
    "owner": "负责人",
    "status": "状态",
    "transport": "传输方式",
    "brand": "品牌",
    "model": "型号",
    "remark": "备注",
    "group_label": "分组字段",
    "group_value": "分组值",
    "record_count": "记录数",
}


def unique_export_values(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = canonicalize_value(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def join_export_values(values: list[str]) -> str:
    return "；".join(unique_export_values(values))


def export_cell_value(key: str, value: Any) -> str:
    text = canonicalize_value(value)
    if not text:
        return ""
    digits = re.sub(r"\D+", "", text)
    if key in {"phone", "phone_landline", "phone_mobile", "phone_other", "办电", "手机号码"} and digits:
        return f"'{text}"
    return text


def export_label(key: str) -> str:
    return EXPORT_LABELS.get(key, prettify_key(key))


def grouped_extra_buckets(row: dict[str, Any]) -> dict[str, list[tuple[str, str, str | None]]]:
    buckets: dict[str, list[tuple[str, str, str | None]]] = defaultdict(list)
    for key, value in row_extra_items(row):
        base_key = extra_key_base(key) or canonicalize_value(key)
        buckets[extra_key_group(key)].append((base_key, canonicalize_value(value), infer_extra_field(key)))
    return buckets


def canonical_export_values(row: dict[str, Any], field: str, group: str = "") -> list[str]:
    values: list[str] = []
    if group == "" or field in {"department", "location", "status", "transport", "brand", "model", "remark", "quantity", "ip"}:
        if field == "ip":
            values.extend(value for _, value in row_field_entries(row, "ip"))
        elif field == "quantity":
            values.extend(str(number) for number in row_quantity_values(row))
        elif field in {"brand", "model"}:
            values.extend(value for _, value in row_field_entries(row, field))
        elif field == "phone":
            values.extend(entry["value"] for entry in phone_entries(row))
        else:
            direct = canonicalize_value(row.get(field, ""))
            if direct:
                values.append(direct)

    for base_key, value, inferred in grouped_extra_buckets(row).get(group, []):
        if not value:
            continue
        if field in {"brand", "model"} and inferred == "brand_model":
            brand_value, model_value = parse_brand_model_value(value)
            values.append(brand_value if field == "brand" else model_value)
            continue
        if inferred == field:
            if field == "ip":
                values.extend(extract_ipv4_values(value, ledger_dominant_ip_prefix(row.get("ledger_name", ""))))
            elif field == "quantity":
                values.extend(str(number) for number in extract_quantity_values(value))
            elif field == "phone":
                values.extend(extract_phone_numbers(value))
            else:
                values.append(value)
    return unique_export_values(values)


def phone_subject_groups(row: dict[str, Any], subject: str | None = None) -> list[str]:
    if subject:
        matched_group = row_subject_group(row, subject)
        return [matched_group if matched_group is not None else ""]

    groups: list[str] = []
    if canonical_export_values(row, "phone", "") or canonical_export_values(row, "entity_name", "") or canonical_export_values(row, "owner", ""):
        groups.append("")
    for group, items in grouped_extra_buckets(row).items():
        if group == "":
            continue
        if any(inferred in {"phone", "entity_name", "owner"} for _, value, inferred in items if value):
            groups.append(group)
    return groups or [""]


def phone_group_numbers(row: dict[str, Any], group: str) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(label: str, raw_value: str) -> None:
        for number in extract_phone_numbers(raw_value):
            if not number or number in seen:
                continue
            seen.add(number)
            entries.append(
                {
                    "label": canonicalize_value(label) or "电话",
                    "value": canonicalize_value(number),
                    "type": classify_phone_entry(label, number),
                }
            )

    if group == "":
        direct = canonicalize_value(row.get("phone", ""))
        if direct:
            add("电话", direct)

    for base_key, value, inferred in grouped_extra_buckets(row).get(group, []):
        if inferred == "phone":
            add(base_key, value)

    return entries


def build_phone_export_rows(rows: list[dict[str, Any]], subject: str | None = None) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        base_record_id = canonicalize_value(row.get("record_id", "")) or canonicalize_value(row.get("base_id", ""))
        for group in phone_subject_groups(row, subject):
            names = canonical_export_values(row, "entity_name", group) or canonical_export_values(row, "owner", group)
            phone_group_entries = phone_group_numbers(row, group)
            numbers = unique_export_values([entry["value"] for entry in phone_group_entries])
            if not names and not numbers:
                continue

            landlines: list[str] = []
            mobiles: list[str] = []
            others: list[str] = []
            for entry in phone_group_entries:
                kind = entry["type"]
                if kind == "landline":
                    landlines.append(entry["value"])
                elif kind == "mobile":
                    mobiles.append(entry["value"])
                else:
                    others.append(entry["value"])

            export_row: dict[str, Any] = {
                "ledger_name": normalize_ledger_name(row.get("ledger_name", "")),
                "record_id": base_record_id if not group else f"{base_record_id}#{group}",
                "entity_name": join_export_values(names),
                "department": join_export_values(canonical_export_values(row, "department", group)),
                "location": join_export_values(canonical_export_values(row, "location", group)),
                "phone": join_export_values(numbers),
                "phone_landline": join_export_values(landlines),
                "phone_mobile": join_export_values(mobiles),
                "phone_other": join_export_values(others),
                "remark": join_export_values(canonical_export_values(row, "remark", group)),
            }

            extra_fields: dict[str, list[str]] = defaultdict(list)
            for base_key, value, inferred in grouped_extra_buckets(row).get(group, []):
                if inferred in set(CANONICAL_FIELDS) | {"brand_model", "phone"}:
                    continue
                if not base_key or base_key == "__sheet_name__" or re.fullmatch(r"column_\d+", base_key):
                    continue
                extra_fields[base_key].append(value)
            for base_key, values in extra_fields.items():
                export_row[base_key] = join_export_values(values)

            dedupe_key = (
                export_row.get("ledger_name", ""),
                export_row.get("entity_name", ""),
                export_row.get("phone", ""),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            output.append(export_row)
    return output


def detail_answer_values(rows: list[dict[str, Any]], plan: QueryPlan) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for row in rows:
        value = row_answer_value(row, plan.answer_field, subject=plan.subject)
        normalized = canonicalize_value(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalize_display_text(value))
    return values


def build_answer_field_export_rows(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    if not plan.answer_field:
        return [build_generic_export_row(row) for row in rows]

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    order: list[str] = []
    for row in rows:
        value = row_answer_value(row, plan.answer_field, subject=plan.subject)
        normalized = canonicalize_value(value)
        if not normalized:
            continue
        if normalized not in grouped:
            order.append(normalized)
        grouped[normalized].append(row)

    if not grouped:
        return [build_generic_export_row(row) for row in rows]

    export_rows: list[dict[str, Any]] = []
    for key in order:
        grouped_rows = grouped[key]
        first = grouped_rows[0]
        export_row: dict[str, Any] = {
            "ledger_name": join_export_values(
                [normalize_ledger_name(row.get("ledger_name", "")) for row in grouped_rows]
            ),
            "record_id": "",
            "entity_name": join_export_values(
                canonical_export_values(first, "entity_name")
                or canonical_export_values(first, "owner")
                or canonical_export_values(first, "location")
            ),
            "location": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "location")
                )
            ),
            "ip": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "ip")
                )
            ),
            "department": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "department")
                )
            ),
            "owner": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "owner")
                )
            ),
            "status": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "status")
                )
            ),
            "transport": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "transport")
                )
            ),
            "brand": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "brand")
                )
            ),
            "model": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "model")
                )
            ),
            "quantity": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "quantity")
                )
            ),
            "remark": join_export_values(
                unique_export_values(
                    value
                    for row in grouped_rows
                    for value in canonical_export_values(row, "remark")
                )
            ),
        }
        export_row[plan.answer_field] = normalize_display_text(key)
        export_rows.append(export_row)
    return export_rows


def build_count_export_rows(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    if plan.dedupe_key == "phone":
        return build_phone_export_rows(rows, plan.subject)
    if plan.dedupe_key == "ip":
        detail_plan = QueryPlan(
            question=plan.question,
            intent="detail",
            answer_field="ip",
            group_field=None,
            dedupe_key=None,
            global_scope=plan.global_scope,
            existence_fields=plan.existence_fields,
            subject=plan.subject,
            filter_fields=plan.filter_fields,
            free_terms=plan.free_terms,
        )
        return build_answer_field_export_rows(rows, detail_plan)
    if plan.dedupe_key == "person":
        return [build_generic_export_row(row) for row in dedupe_rows(rows, "person")]
    return [build_generic_export_row(row) for row in rows]


def build_group_export_rows(grouped_rows: list[tuple[str, int]], group_field: str | None) -> list[dict[str, Any]]:
    label = infer_group_label(group_field or "group")
    return [
        {
            "group_label": label,
            "group_value": normalize_display_text(value),
            "record_count": str(count),
        }
        for value, count in grouped_rows
    ]


def build_detail_export_rows(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    if plan.answer_field == "phone" or plan.dedupe_key == "phone":
        return build_phone_export_rows(rows, plan.subject)
    if plan.answer_field:
        return build_answer_field_export_rows(rows, plan)
    return [build_generic_export_row(row) for row in rows]


def build_generic_export_row(row: dict[str, Any]) -> dict[str, Any]:
    export_row: dict[str, Any] = {
        "ledger_name": normalize_ledger_name(row.get("ledger_name", "")),
        "record_id": canonicalize_value(row.get("record_id", "")) or canonicalize_value(row.get("base_id", "")),
        "entity_name": join_export_values(canonical_export_values(row, "entity_name")),
        "location": join_export_values(canonical_export_values(row, "location")),
        "ip": join_export_values(canonical_export_values(row, "ip")),
        "department": join_export_values(canonical_export_values(row, "department")),
        "owner": join_export_values(canonical_export_values(row, "owner")),
        "status": join_export_values(canonical_export_values(row, "status")),
        "transport": join_export_values(canonical_export_values(row, "transport")),
        "brand": join_export_values(canonical_export_values(row, "brand")),
        "model": join_export_values(canonical_export_values(row, "model")),
        "quantity": join_export_values(canonical_export_values(row, "quantity")),
        "remark": join_export_values(canonical_export_values(row, "remark")),
    }

    flat = flatten_record_for_export(row)
    extra_fields: dict[str, list[str]] = defaultdict(list)
    for key, value in flat.items():
        text = canonicalize_value(value)
        if not text:
            continue
        if key.startswith("_") or key in {"extra_json", "content_hash", "__sheet_name__", "ledger_name", "record_id", "base_id", "source_file", "source_type", "version_no", "updated_at", "is_deleted"}:
            continue
        if re.fullmatch(r"column_\d+", key) or EXTRA_SUFFIX_PATTERN.search(key):
            continue
        inferred = infer_extra_field(key)
        if inferred in set(CANONICAL_FIELDS) | {"brand_model", "phone"}:
            continue
        extra_fields[key].append(text)
    for key, values in extra_fields.items():
        export_row[key] = join_export_values(values)
    return export_row


def build_export_rows(rows: list[dict[str, Any]], plan: QueryPlan, grouped_rows: list[tuple[str, int]] | None = None) -> list[dict[str, Any]]:
    if not rows:
        return build_group_export_rows(grouped_rows or [], plan.group_field) if plan.intent == "group" else []
    if plan.intent == "group":
        return build_group_export_rows(grouped_rows or [], plan.group_field)
    if plan.intent == "detail":
        return build_detail_export_rows(rows, plan)
    if plan.intent == "count":
        return build_count_export_rows(rows, plan)
    return [build_generic_export_row(row) for row in rows]


def template_present_columns(
    rows: list[dict[str, Any]],
    candidates: list[str],
    always_include: set[str] | None = None,
) -> list[str]:
    always_include = always_include or set()
    columns: list[str] = []
    for key in candidates:
        if key in always_include or any(canonicalize_value(row.get(key, "")) for row in rows):
            columns.append(key)
    return columns


def filtered_extra_export_columns(rows: list[dict[str, Any]], min_ratio: float = 0.15) -> list[str]:
    preferred = [
        "ledger_name",
        "record_id",
        "entity_name",
        "location",
        "ip",
        "phone",
        "phone_landline",
        "phone_mobile",
        "phone_other",
        "quantity",
        "department",
        "owner",
        "status",
        "transport",
        "brand",
        "model",
        "remark",
    ]
    hidden_columns = {"base_id", "source_file", "source_type", "version_no", "updated_at", "is_deleted"}
    value_counts: Counter[str] = Counter()
    example_values: dict[str, str] = {}
    for row in rows:
        flat = flatten_record_for_export(row)
        for key, value in flat.items():
            if key.startswith("_") or key in {"extra_json", "content_hash"}:
                continue
            text = canonicalize_value(value)
            if text:
                value_counts[key] += 1
                example_values.setdefault(key, text)

    min_count = 1 if len(rows) <= 3 else max(2, math.ceil(len(rows) * 0.15))
    def allow_extra_column(key: str) -> bool:
        if key.startswith("_") or key in {"extra_json", "content_hash", "__sheet_name__"} | hidden_columns:
            return False
        if re.fullmatch(r"column_\d+", key):
            return False
        if EXTRA_SUFFIX_PATTERN.search(key):
            return False
        if value_counts.get(key, 0) < min_count:
            return False
        canonical = canonical_field_from_label(key)
        if canonical and canonical != key and value_counts.get(canonical, 0) > 0:
            return False
        return True

    seen = set(preferred)
    columns: list[str] = []
    for row in rows:
        flat = flatten_record_for_export(row)
        for key in flat:
            if key in seen or not allow_extra_column(key):
                continue
            columns.append(key)
            seen.add(key)
    return columns


def phone_export_extra_columns(rows: list[dict[str, Any]]) -> list[str]:
    title_markers = ("职务", "title", "岗位", "科室")
    extras = filtered_extra_export_columns(rows, min_ratio=0.05)
    return [key for key in extras if any(marker in key.lower() for marker in title_markers)]


def choose_export_columns(
    rows: list[dict[str, Any]],
    plan: QueryPlan | None = None,
    grouped_rows: list[tuple[str, int]] | None = None,
) -> list[str]:
    if plan and plan.intent == "group":
        return template_present_columns(
            build_group_export_rows(grouped_rows or [], plan.group_field),
            ["group_label", "group_value", "record_count"],
            always_include={"group_label", "group_value", "record_count"},
        )

    if plan and (plan.answer_field == "phone" or plan.dedupe_key == "phone"):
        base = template_present_columns(
            rows,
            [
                "ledger_name",
                "record_id",
                "entity_name",
                "department",
                "phone_landline",
                "phone_mobile",
                "phone_other",
                "phone",
            ],
            always_include={"ledger_name", "entity_name"},
        )
        return base + [key for key in phone_export_extra_columns(rows) if key not in base]

    if plan and (plan.answer_field == "ip" or plan.dedupe_key == "ip"):
        return template_present_columns(
            rows,
            ["ledger_name", "entity_name", "ip", "department", "owner", "location", "status", "transport", "brand", "model", "remark"],
            always_include={"ledger_name", "entity_name", "ip"},
        )

    if plan and plan.dedupe_key == "person":
        return template_present_columns(
            rows,
            ["ledger_name", "entity_name", "department", "owner", "phone", "ip", "status", "transport", "brand", "model", "quantity", "remark"],
            always_include={"ledger_name", "entity_name"},
        )

    if plan and plan.intent == "detail":
        base = template_present_columns(
            rows,
            ["ledger_name", "record_id", "entity_name", "location", "department", "owner", "ip", "phone", "quantity", "status", "transport", "brand", "model", "remark"],
            always_include={"ledger_name", "entity_name"},
        )
        return base + [key for key in filtered_extra_export_columns(rows, min_ratio=0.05) if key not in base]

    if plan and plan.intent == "count":
        base = template_present_columns(
            rows,
            ["ledger_name", "record_id", "entity_name", "location", "department", "owner", "ip", "phone", "quantity", "status", "transport", "brand", "model", "remark"],
            always_include={"ledger_name"},
        )
        return base + [key for key in filtered_extra_export_columns(rows, min_ratio=0.2) if key not in base]

    preferred = [
        "ledger_name",
        "record_id",
        "entity_name",
        "location",
        "ip",
        "phone",
        "phone_landline",
        "phone_mobile",
        "phone_other",
        "quantity",
        "department",
        "owner",
        "status",
        "transport",
        "brand",
        "model",
        "remark",
    ]
    base = template_present_columns(rows, preferred, always_include={"ledger_name"})
    return base + [key for key in filtered_extra_export_columns(rows) if key not in base]


def merged_ledger_names(primary: list[str], secondary: list[str]) -> list[str]:
    ordered: list[str] = []
    for ledger_name in [*primary, *secondary]:
        if ledger_name and ledger_name not in ordered:
            ordered.append(ledger_name)
    return ordered


def visible_ledger_representatives(rows_by_ledger: dict[str, list[dict[str, Any]]]) -> list[str]:
    available = [ledger_name for ledger_name in sorted(rows_by_ledger) if rows_by_ledger[ledger_name]]
    candidates = [ledger_name for ledger_name in available if ledger_name not in INTERNAL_LEDGERS]
    by_family: dict[str, list[str]] = defaultdict(list)
    for ledger_name in candidates:
        by_family[ledger_family_name(ledger_name, available)].append(ledger_name)
    representatives: list[str] = []
    for family_name, members in by_family.items():
        members.sort(
            key=lambda ledger_name: (
                normalize_ledger_name(ledger_name) != family_name,
                -len(rows_by_ledger.get(ledger_name, [])),
                ledger_name,
            )
        )
        representatives.append(members[0])
    return sorted(representatives)


def store_export(
    rows: list[dict[str, Any]],
    question: str,
    plan: QueryPlan | None = None,
    grouped_rows: list[tuple[str, int]] | None = None,
) -> str | None:
    if not rows and not grouped_rows:
        return None
    token = uuid.uuid4().hex
    EXPORT_CACHE[token] = {
        "rows": rows,
        "question": question,
        "plan": plan,
        "grouped_rows": grouped_rows or [],
        "created_at": utc_now(),
    }
    if len(EXPORT_CACHE) > 20:
        oldest = sorted(EXPORT_CACHE.items(), key=lambda item: item[1]["created_at"])[:-20]
        for key, _ in oldest:
            EXPORT_CACHE.pop(key, None)
    return token


def export_rows_to_csv(
    rows: list[dict[str, Any]],
    plan: QueryPlan | None = None,
    grouped_rows: list[tuple[str, int]] | None = None,
) -> StreamingResponse:
    export_rows = build_export_rows(rows, plan, grouped_rows) if plan else rows
    columns = choose_export_columns(export_rows, plan, grouped_rows)
    projected: list[dict[str, Any]] = []
    labels = [export_label(column) for column in columns]
    for row in export_rows:
        projected.append({export_label(column): export_cell_value(column, row.get(column, "")) for column in columns})
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=labels)
    writer.writeheader()
    writer.writerows(projected)
    file_name = f"query-result-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
    )


def build_record_cards(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    title_fields = title_fields_for_plan(plan)
    for row in rows:
        title = title_for_row_with_fields(row, title_fields)
        cards.append(
            {
                "title": title,
                "subtitle": subtitle_for_row(row, title),
                "ledger_name": normalize_ledger_name(row.get("ledger_name", "")),
                "pairs": [(prettify_key(key), value) for key, value in visible_pairs(row)],
            }
        )
    return cards


def build_display_rows(rows: list[dict[str, Any]], plan: QueryPlan, grouped_rows: list[tuple[str, int]] | None = None) -> list[dict[str, Any]]:
    if plan.intent == "detail" and plan.answer_field:
        return build_detail_export_rows(rows, plan)
    if plan.intent == "group":
        return []
    if plan.intent == "count" and plan.dedupe_key in {"ip", "phone", "person"}:
        return build_count_export_rows(rows, plan)
    return rows


def answer_for_detail(plan: QueryPlan, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "没有匹配到明确记录。"
    row = rows[0]
    if plan.answer_field:
        subject = row_subject_label(row)
        values = detail_answer_values(rows, plan)
        if values:
            label = ANSWER_FIELD_LABELS.get(plan.answer_field, plan.answer_field)
            if len(values) == 1:
                return f"{subject or '该记录'} 的 {label} 是 {values[0]}。"
            if plan.answer_field in {"ip", "phone"}:
                return f"{subject or '该记录'} 的 {label} 有 {len(values)} 个：{'、'.join(values)}。"
            return f"{subject or '该记录'} 关联到 {len(values)} 个 {label}：{'、'.join(values)}。"
    return f"共匹配到 {len(rows)} 条记录，详细结果见下方明细。"


def row_subject_label(row: dict[str, Any]) -> str:
    for field in ("owner", "entity_name", "location", "department"):
        value = display_value_for_field(row, field)
        if value:
            return value
    return ""


def resolved_subject_label(plan: QueryPlan, selected_candidates: list[dict[str, Any]]) -> str:
    if not plan.subject:
        return ""
    for candidate in selected_candidates:
        if candidate["field"] in {"department", "owner", "entity_name", "brand", "model", "transport", "status"}:
            value = normalize_display_text(candidate["value"])
            if value:
                return value
    location_candidates = [candidate for candidate in selected_candidates if candidate["field"] == "location"]
    if location_candidates:
        for hint in FIELD_HINTS["location"]:
            if len(hint) < 2:
                continue
            matched = sum(1 for candidate in location_candidates if hint in normalize_text(candidate["value"]))
            if matched >= max(1, math.ceil(len(location_candidates) * 0.6)):
                return normalize_display_text(hint)
    return normalize_display_text(plan.subject)


def quantity_total(rows: list[dict[str, Any]]) -> tuple[int, int]:
    total = 0
    covered_rows = 0
    for row in rows:
        values = row_quantity_values(row)
        if not values:
            continue
        total += values[0]
        covered_rows += 1
    return total, covered_rows


def row_match_score(row: dict[str, Any], plan: QueryPlan) -> float:
    score = 0.0
    subject_compact = normalize_compact(plan.subject)
    if question_mentions_ledger(plan.question, canonicalize_value(row.get("ledger_name", ""))):
        score += 2.5
    for field in DETAIL_SUBJECT_FIELDS:
        value = canonicalize_value(row.get(field, ""))
        if not value:
            continue
        value_compact = normalize_compact(value)
        if subject_compact and value_compact == subject_compact:
            if plan.answer_field == "phone" and field == "entity_name":
                score += 3.4
            else:
                score += 3.0 if field in ("owner", "entity_name") else 2.4
        elif subject_compact and value_compact and subject_compact in value_compact:
            score += 1.6
    if plan.answer_field and row_answer_value(row, plan.answer_field, subject=plan.subject):
        score += 2.0
    if plan.existence_fields:
        score += sum(0.6 for field in plan.existence_fields if row_has_field_value(row, field))
    return score


def choose_primary_detail_ledger(rows: list[dict[str, Any]], plan: QueryPlan, rows_by_ledger: dict[str, list[dict[str, Any]]]) -> str | None:
    if not rows:
        return None
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[normalize_ledger_name(row.get("ledger_name", ""))].append(row)

    best_score: float | None = None
    best_ledger: str | None = None
    for ledger_name, ledger_rows in grouped.items():
        profile = build_ledger_profile(ledger_name, rows_by_ledger.get(ledger_name, []))
        profile_ratios = profile["ratios"]
        semantic_scores = profile.get("semantic_type_scores", {})
        distinct_numbers = len(
            {
                entry["value"]
                for row in ledger_rows
                for entry in phone_entries(row, plan.subject)
                if entry["value"]
            }
        )
        ledger_score = max(row_match_score(row, plan) for row in ledger_rows)
        ledger_score += distinct_numbers * 1.8
        ledger_score += profile_ratios.get("phone", 0.0) * 3.2
        ledger_score += math.log1p(float(semantic_scores.get("contact", 0.0))) * 0.75
        if question_mentions_ledger(plan.question, ledger_name):
            ledger_score += 2.5
        candidate = (ledger_score, distinct_numbers, -len(ledger_rows), ledger_name)
        if best_score is None or candidate > (best_score, 0, 0, best_ledger or ""):
            best_score = ledger_score
            best_ledger = ledger_name
    return best_ledger


def answer_for_count(plan: QueryPlan, rows: list[dict[str, Any]], deduped_rows: list[dict[str, Any]], subject_label: str = "") -> str:
    normalized = normalize_text(plan.question)
    if plan.dedupe_key == "ip":
        ip_count = len(distinct_field_values(rows, "ip"))
        return f"按IP去重后，具有IP的记录共 {ip_count} 条。"
    if plan.dedupe_key == "phone":
        phone_summary = distinct_phone_summary(rows, plan.subject)
        summary_parts: list[str] = []
        if phone_summary["landline"]:
            summary_parts.append(f"办公/座机号码 {phone_summary['landline']} 个")
        if phone_summary["mobile"]:
            summary_parts.append(f"手机号码 {phone_summary['mobile']} 个")
        if phone_summary["unknown"]:
            summary_parts.append(f"其他号码 {phone_summary['unknown']} 个")
        summary_text = "，".join(summary_parts) if summary_parts else "没有识别到有效号码"
        return f"按电话号码去重后，{summary_text}，合计 {phone_summary['total']} 个号码，涉及 {len(rows)} 条记录。"
    if plan.dedupe_key == "person":
        return f"按人员去重后，{subject_label + ' ' if subject_label else ''}相关记录共 {len(deduped_rows)} 条。"
    if should_sum_quantity(plan.question, plan):
        total_quantity, covered_rows = quantity_total(rows)
        if total_quantity and covered_rows:
            return f"按数量字段汇总，{subject_label + ' ' if subject_label else ''}共 {total_quantity} 项，涉及 {covered_rows} 条记录。"
    return f"{subject_label + ' ' if subject_label else ''}相关记录共 {len(rows)} 条。"


def answer_for_group(group_field: str, grouped: list[tuple[str, int]]) -> str:
    if not grouped:
        return "没有可分组的结果。"
    top_preview = "；".join(f"{label} {count}" for label, count in grouped[:6])
    return f"按{infer_group_label(group_field)}分组，共 {len(grouped)} 组：{top_preview}。"


def render_group_rows(rows: list[dict[str, Any]], field: str) -> list[tuple[str, int]]:
    counts: Counter[str] = Counter()
    for row in rows:
        value = canonicalize_value(row.get(field, ""))
        if value:
            counts[value] += 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def run_query(question: str, requested_ledger: str, page: int) -> dict[str, Any]:
    rows_by_ledger = load_rows_by_ledger()
    plan = make_query_plan(question)
    if is_ledger_inventory_query(plan, requested_ledger):
        representative_ledgers = visible_ledger_representatives(rows_by_ledger)
        ledger_summary_map = {item["ledger_name"]: item["count"] for item in list_ledger_summaries()}
        ledger_cards = [
            {
                "ledger_name": ledger_name,
                "total_count": ledger_summary_map.get(ledger_name, len(rows_by_ledger.get(ledger_name, []))),
                "hit_count": ledger_summary_map.get(ledger_name, len(rows_by_ledger.get(ledger_name, []))),
            }
            for ledger_name in representative_ledgers
        ]
        total_ledgers = len(representative_ledgers)
        return {
            "answer": f"当前共有 {total_ledgers} 本有效台账（按重复账本家族压缩后）。",
            "selection_mode": "ledger_inventory",
            "included_ledgers": representative_ledgers,
            "excluded_ledgers": [],
            "rows": [],
            "cards": [],
            "total_count": total_ledgers,
            "page": 1,
            "total_pages": 1,
            "page_size": PAGE_SIZE,
            "showing_from": 0,
            "showing_to": 0,
            "export_token": None,
            "group_field": None,
            "grouped_rows": [],
            "cross_ledger_stats": None,
            "family_dedupe_stats": None,
            "plan": plan,
            "ledger_cards": ledger_cards,
            "raw_hit_count": total_ledgers,
        }
    if is_bare_field_query(plan):
        return {
            "answer": "输入过于模糊，请补充查询对象或统计口径，例如“张三的IP是多少”或“统计所有IP地址”。",
            "selection_mode": "invalid_bare_field",
            "included_ledgers": [],
            "excluded_ledgers": [],
            "rows": [],
            "cards": [],
            "total_count": 0,
            "page": 1,
            "total_pages": 1,
            "page_size": PAGE_SIZE,
            "showing_from": 0,
            "showing_to": 0,
            "export_token": None,
            "group_field": None,
            "grouped_rows": [],
            "cross_ledger_stats": None,
            "family_dedupe_stats": None,
            "plan": plan,
            "ledger_cards": [],
            "raw_hit_count": 0,
        }
    selected_ledgers, excluded_ledgers, selection_mode = select_ledgers(plan, requested_ledger, rows_by_ledger)
    candidate_ledgers = selected_ledgers or list(rows_by_ledger)

    rows: list[dict[str, Any]] = []
    for ledger_name in candidate_ledgers:
        rows.extend(rows_by_ledger.get(ledger_name, []))

    field_candidates = []
    if not (plan.intent == "count" and plan.global_scope and plan.existence_fields and not plan.subject):
        field_candidates = match_value_candidates(
            plan.question,
            plan.subject,
            candidate_ledgers,
            rows_by_ledger,
            plan.filter_fields,
        )
    selected_candidates = select_filter_candidates(plan, field_candidates)
    if (
        plan.subject
        and not selected_candidates
        and any(field in plan.filter_fields for field in ("location", "entity_name"))
    ):
        rows = [row for row in rows if row_matches_terms(row, (plan.subject,))]
    else:
        combine_mode = "or" if plan.intent == "detail" else "and"
        rows = apply_candidate_filters(rows, selected_candidates, combine_mode=combine_mode)
    rows = apply_existence_filters(rows, plan.existence_fields)

    consumed_terms = {normalize_compact(candidate["value"]) for candidate in selected_candidates}
    weak_terms = {normalize_compact(term) for term in ENTITY_OBJECT_TERMS}
    if plan.answer_field:
        weak_terms.update(normalize_compact(hint) for hint in FIELD_HINTS.get(plan.answer_field, ()))
    for field in plan.existence_fields:
        weak_terms.update(normalize_compact(hint) for hint in FIELD_HINTS.get(field, ()))
    residual_terms = tuple(
        term
        for term in plan.free_terms
        if normalize_compact(term) not in consumed_terms and normalize_compact(term) not in weak_terms
    )
    skip_residual_terms = plan.intent == "count" and plan.global_scope and plan.existence_fields and not plan.subject
    if residual_terms and not skip_residual_terms:
        matched_rows = [row for row in rows if row_matches_terms(row, residual_terms)]
        if matched_rows:
            rows = matched_rows

    if plan.intent == "detail" and plan.answer_field:
        rows_with_answer = [row for row in rows if row_has_field_value(row, plan.answer_field)]
        if rows_with_answer:
            rows = rows_with_answer
    if plan.intent == "detail" and plan.answer_field == "phone" and plan.subject:
        primary_ledger = choose_primary_detail_ledger(rows, plan, rows_by_ledger)
        if primary_ledger:
            primary_rows = [
                row
                for row in rows
                if normalize_ledger_name(row.get("ledger_name", "")) == primary_ledger
            ]
            if primary_rows:
                rows = primary_rows

    rows, family_dedupe_stats = collapse_duplicate_family_rows(rows, candidate_ledgers, requested_ledger)

    rows.sort(
        key=lambda row: (
            row_match_score(row, plan),
            canonicalize_value(row.get("updated_at", "")),
        ),
        reverse=True,
    )
    deduped_rows = dedupe_rows(rows, plan.dedupe_key)

    subject_label = resolved_subject_label(plan, selected_candidates)

    if plan.intent == "group" and plan.group_field:
        grouped = render_group_rows(rows, plan.group_field)
        answer = answer_for_group(plan.group_field, grouped)
    elif plan.intent == "detail":
        answer = answer_for_detail(plan, rows)
        grouped = []
    elif plan.intent == "count":
        answer = answer_for_count(plan, rows, deduped_rows, subject_label)
        grouped = []
    else:
        answer = f"共匹配到 {len(rows)} 条记录，详细结果见下方明细。"
        grouped = []

    rows_for_display = build_display_rows(rows, plan, grouped)
    total_count = len(rows_for_display)
    total_pages = max(1, math.ceil(total_count / PAGE_SIZE)) if total_count else 1
    page = min(max(page, 1), total_pages)
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE
    page_rows = rows_for_display[start:end]

    if plan.intent == "group":
        rows_for_export = []
    elif plan.answer_field == "phone" or plan.dedupe_key == "phone":
        rows_for_export = rows
    elif plan.dedupe_key == "ip" and plan.intent == "count":
        rows_for_export = rows
    elif plan.intent == "detail" and plan.answer_field:
        rows_for_export = rows
    else:
        rows_for_export = rows_for_display

    export_token = store_export(rows_for_export, question, plan, grouped)
    ledger_cards = []
    hit_counts = Counter(normalize_ledger_name(row.get("ledger_name", "")) for row in rows)
    ledger_summary_map = {item["ledger_name"]: item["count"] for item in list_ledger_summaries()}
    for ledger_name in selected_ledgers or candidate_ledgers:
        if hit_counts.get(ledger_name, 0) <= 0:
            continue
        ledger_cards.append(
            {
                "ledger_name": ledger_name,
                "total_count": ledger_summary_map.get(ledger_name, 0),
                "hit_count": hit_counts.get(ledger_name, 0),
            }
        )

    visible_included_ledgers = [
        name
        for name in (selected_ledgers or candidate_ledgers)
        if name not in INTERNAL_LEDGERS and hit_counts.get(name, 0) > 0
    ]
    visible_excluded_ledgers = [
        name
        for name in merged_ledger_names(selected_ledgers or candidate_ledgers, excluded_ledgers)
        if name not in INTERNAL_LEDGERS and name not in visible_included_ledgers
    ]

    query_output = {
        "answer": answer,
        "selection_mode": selection_mode,
        "included_ledgers": visible_included_ledgers or (selected_ledgers or candidate_ledgers),
        "excluded_ledgers": visible_excluded_ledgers,
        "rows": page_rows,
        "cards": build_record_cards(page_rows, plan),
        "total_count": total_count,
        "page": page,
        "total_pages": total_pages,
        "page_size": PAGE_SIZE,
        "showing_from": start + 1 if page_rows else 0,
        "showing_to": min(end, total_count),
        "export_token": export_token,
        "group_field": plan.group_field,
        "grouped_rows": grouped,
        "cross_ledger_stats": summarize_cross_ledger_dedupe(rows, plan.dedupe_key) if len(selected_ledgers or candidate_ledgers) > 1 and plan.dedupe_key else None,
        "family_dedupe_stats": family_dedupe_stats,
        "plan": plan,
        "ledger_cards": ledger_cards,
        "raw_hit_count": len(rows),
    }
    return query_output


def safe_ledger_from_filename(file_name: str, existing: set[str]) -> str:
    stem = Path(file_name).stem or "ledger"
    base = normalize_ledger_name(stem)
    if base not in existing:
        existing.add(base)
        return base
    index = 2
    while f"{base}-{index}" in existing:
        index += 1
    ledger_name = f"{base}-{index}"
    existing.add(ledger_name)
    return ledger_name


def base_ledger_from_filename(file_name: str) -> str:
    stem = Path(file_name).stem or "ledger"
    return normalize_ledger_name(stem)


def delete_ledger(ledger_name: str) -> int:
    normalized = normalize_ledger_name(ledger_name)
    connection = connect_db()
    initialize_database(connection)
    try:
        current_count = connection.execute(
            "SELECT COUNT(*) AS total FROM records_current WHERE ledger_name = ?",
            (normalized,),
        ).fetchone()["total"]
        connection.execute("BEGIN")
        connection.execute("DELETE FROM records_current WHERE ledger_name = ?", (normalized,))
        connection.execute("DELETE FROM records_history WHERE ledger_name = ?", (normalized,))
        connection.execute("DELETE FROM operations_log WHERE ledger_name = ?", (normalized,))
        connection.execute("DELETE FROM embeddings_meta WHERE ledger_name = ?", (normalized,))
        rebuild_ledger_semantics(connection)
        connection.commit()
    finally:
        connection.close()
    invalidate_runtime_cache()
    return int(current_count)


def import_uploaded_files(
    files: list[UploadFile],
    mode: str,
    merged_ledger: str,
    on_conflict: str,
    duplicate_strategy: str,
) -> list[dict[str, Any]]:
    existing_ledgers = {summary["ledger_name"] for summary in list_ledger_summaries()}
    results: list[dict[str, Any]] = []
    touched_ledgers: list[str] = []
    for upload in files:
        if not upload.filename:
            continue
        suffix = Path(upload.filename).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            handle.write(upload.file.read())
            temp_path = Path(handle.name)
        if mode == "merge":
            ledger_name = normalize_ledger_name(merged_ledger)
            ledger_exists = ledger_name in existing_ledgers
        else:
            base_ledger = base_ledger_from_filename(upload.filename)
            ledger_exists = base_ledger in existing_ledgers
            if ledger_exists and on_conflict == "reject":
                temp_path.unlink(missing_ok=True)
                raise LedgerError(f"账本已存在：{base_ledger}")
            if ledger_exists and on_conflict == "replace":
                delete_ledger(base_ledger)
                existing_ledgers.discard(base_ledger)
                ledger_name = base_ledger
                ledger_exists = False
            elif ledger_exists and on_conflict == "merge":
                ledger_name = base_ledger
            else:
                ledger_name = safe_ledger_from_filename(upload.filename, existing_ledgers)
        if mode == "merge" and ledger_exists and on_conflict == "replace":
            delete_ledger(ledger_name)
            existing_ledgers.discard(ledger_name)
        command = [
            sys.executable,
            str(SCRIPTS_DIR / "import_table.py"),
            str(temp_path),
            "--ledger",
            ledger_name,
            "--duplicate-strategy",
            duplicate_strategy,
        ]
        completed: CompletedProcess[str] = run(command, text=True, capture_output=True)
        temp_path.unlink(missing_ok=True)
        if completed.returncode != 0:
            raise LedgerError(completed.stderr.strip() or completed.stdout.strip() or f"Import failed for {upload.filename}")
        payload = json.loads(completed.stdout)
        payload["import_ledger"] = ledger_name
        payload["conflict_mode"] = on_conflict
        results.append(payload)
        touched_ledgers.append(ledger_name)
    if touched_ledgers:
        invalidate_runtime_cache()
    return results


def base_context() -> dict[str, Any]:
    visible_ledgers = visible_ledger_summaries()
    return {
        "ledger_summaries": visible_ledgers,
        "ledger_options": ["auto", *[item["ledger_name"] for item in visible_ledgers]],
        "query_form": {"question": "", "ledger": "auto"},
        "import_form": {
            "mode": "each",
            "merged_ledger": "default",
            "on_conflict": "create-copy",
            "duplicate_strategy": "keep-last",
        },
        "query_output": None,
        "import_result": None,
        "ledger_action_result": None,
    }


@app.on_event("startup")
def startup() -> None:
    connection = connect_db()
    initialize_database(connection)
    ensure_ledger_semantics(connection)
    connection.close()
    load_all_active_rows(force_refresh=True)
    load_ledger_semantic_profiles(force_refresh=True)


@app.get("/", response_class=HTMLResponse)
def root() -> RedirectResponse:
    return RedirectResponse(url="/query", status_code=302)


@app.get("/query", response_class=HTMLResponse)
def query_page(
    request: Request,
    question: str = Query("", alias="q"),
    ledger: str = Query("auto"),
    page: int = Query(1, ge=1),
) -> HTMLResponse:
    context = base_context()
    context["query_form"] = {"question": question, "ledger": ledger}
    if question.strip():
        context["query_output"] = run_query(question.strip(), ledger, page)
    return templates.TemplateResponse(request, "index.html", context)


@app.post("/import", response_class=HTMLResponse)
async def import_page(
    request: Request,
    files: list[UploadFile] = File(default=[]),
    mode: str = Form("each"),
    merged_ledger: str = Form("default"),
    on_conflict: str = Form("create-copy"),
    duplicate_strategy: str = Form("keep-last"),
) -> HTMLResponse:
    context = base_context()
    context["import_form"] = {
        "mode": mode,
        "merged_ledger": merged_ledger,
        "on_conflict": on_conflict,
        "duplicate_strategy": duplicate_strategy,
    }
    try:
        results = import_uploaded_files(files, mode, merged_ledger, on_conflict, duplicate_strategy)
        context["import_result"] = {"ok": True, "results": results}
        context.update(base_context())
        context["import_result"] = {"ok": True, "results": results}
        context["import_form"] = {
            "mode": mode,
            "merged_ledger": merged_ledger,
            "on_conflict": on_conflict,
            "duplicate_strategy": duplicate_strategy,
        }
    except Exception as exc:
        context["import_result"] = {"ok": False, "message": str(exc)}
    return templates.TemplateResponse(request, "index.html", context)


@app.post("/ledger/delete", response_class=HTMLResponse)
async def delete_ledger_page(
    request: Request,
    ledger_name: str = Form(...),
) -> HTMLResponse:
    context = base_context()
    try:
        deleted_count = delete_ledger(ledger_name)
        context.update(base_context())
        context["ledger_action_result"] = {
            "ok": True,
            "message": f"已删除账本 {ledger_name}，共移除 {deleted_count} 条当前记录。",
        }
    except Exception as exc:
        context["ledger_action_result"] = {"ok": False, "message": str(exc)}
    return templates.TemplateResponse(request, "index.html", context)


@app.get("/export/current")
def export_current(token: str) -> StreamingResponse:
    payload = EXPORT_CACHE.get(token)
    if not payload:
        raise HTTPException(status_code=404, detail="Export token expired.")
    return export_rows_to_csv(payload["rows"], payload.get("plan"), payload.get("grouped_rows"))
