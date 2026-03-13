from __future__ import annotations

import ipaddress
import json
import re
from collections import Counter, defaultdict
from typing import Any, Iterable, Sequence

try:
    from common import (
        canonical_field_from_label,
        canonicalize_value,
        extract_ipv4_values,
        fetch_current_records,
        flatten_record_for_export,
        infer_dominant_ipv4_prefix,
        normalize_header,
        normalize_ledger_name,
        safe_json_loads,
    )
except ModuleNotFoundError:  # pragma: no cover - import path differs between scripts and tests
    from scripts.common import (
        canonical_field_from_label,
        canonicalize_value,
        extract_ipv4_values,
        fetch_current_records,
        flatten_record_for_export,
        infer_dominant_ipv4_prefix,
        normalize_header,
        normalize_ledger_name,
        safe_json_loads,
    )

SEMANTIC_STOP_TOKENS = {
    "记录",
    "数据",
    "台账",
    "账本",
    "信息",
    "情况",
    "内容",
    "相关",
    "资产",
    "设备",
    "名称",
    "地点",
    "位置",
    "部门",
    "负责人",
    "状态",
    "品牌",
    "型号",
    "备注",
}
VALUE_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_.:/-]+|[\u4e00-\u9fff]{2,}")
EXTRA_SUFFIX_PATTERN = re.compile(r"__(\d+)$")
MOBILE_PATTERN = re.compile(r"(?<!\d)(1[3-9]\d{9})(?!\d)")
EXTENSION_PATTERN = re.compile(r"(?<!\d)(\d{3,5})(?!\d)")
SEMANTIC_FIELD_HINTS: dict[str, tuple[str, ...]] = {
    "phone": ("电话", "电话号码", "手机号", "手机", "手机号码", "联系电话", "联系方式", "办电", "座机", "办公电话", "移动电话", "phone", "mobile", "tel"),
    "department": ("部门", "科室", "管理段", "单位", "组织", "处室"),
    "owner": ("负责人", "责任人", "联系人", "所有人", "归属人", "使用人", "持有人", "领用人"),
    "location": ("位置", "地点", "点位", "地址", "桩号", "站点", "坝", "辅道", "铁塔", "存放地点"),
    "transport": ("传输", "传输方式", "4g", "5g", "光纤", "专线", "无线", "wifi"),
    "brand": ("品牌", "厂商", "vendor", "maker"),
    "model": ("型号", "机型", "规格"),
    "status": ("状态", "启用", "停用", "active", "使用状况"),
    "quantity": ("数量", "数目", "台数", "个数", "数量单位"),
    "asset": ("资产", "固定资产", "资产名称", "资产类别"),
}
SEMANTIC_TYPE_TERMS = {
    "camera": ("摄像", "监控", "点位", "桩号", "铁塔", "辅道", "坝", "云台", "险工"),
    "asset": ("资产", "固定资产", "盘点", "防火墙", "交换机", "电脑", "终端", "主机", "机房"),
    "contact": ("通讯录", "电话", "手机号", "办电", "联系人"),
    "network": ("ip", "ip地址", "网络", "mac", "路由器", "交换机", "终端", "地址"),
}
PROFILE_VALUE_FIELDS = ("entity_name", "department", "owner", "location", "brand", "model", "status", "transport", "asset", "phone")
SEMANTIC_INTERNAL_LEDGERS = {"default", "sample", "sample-2"}


def extra_key_base(label: str) -> str:
    return EXTRA_SUFFIX_PATTERN.sub("", canonicalize_value(label))


def normalize_compact(value: str) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", canonicalize_value(value).lower())


def looks_like_ip(value: str) -> bool:
    text = canonicalize_value(value)
    if not text:
        return False
    try:
        ipaddress.ip_address(text)
    except ValueError:
        return False
    return True


def extract_phone_numbers(value: str) -> list[str]:
    text = canonicalize_value(value)
    if not text:
        return []
    numbers: list[str] = []
    for match in MOBILE_PATTERN.findall(text):
        if match not in numbers:
            numbers.append(match)
    for match in EXTENSION_PATTERN.findall(text):
        if match not in numbers:
            numbers.append(match)
    return numbers


def parse_brand_model_value(value: str) -> tuple[str, str]:
    text = canonicalize_value(value)
    if not text:
        return "", ""
    parts = [segment.strip() for segment in re.split(r"[\/｜|]+", text) if segment.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    if "（" in text or "(" in text:
        head = re.split(r"[（(]", text, maxsplit=1)[0].strip()
        tail_match = re.search(r"[（(]([^）)]+)[）)]", text)
        tail = tail_match.group(1).strip() if tail_match else ""
        return head, tail or head
    return text, text


def tokenize_value(value: str) -> list[str]:
    tokens: list[str] = []
    for token in VALUE_TOKEN_PATTERN.findall(canonicalize_value(value)):
        compact = normalize_compact(token)
        if not compact or compact in SEMANTIC_STOP_TOKENS:
            continue
        if len(compact) < 2:
            continue
        tokens.append(token.strip())
    return tokens


def infer_extra_semantic_field(label: str) -> str | None:
    base_label = extra_key_base(label)
    canonical = canonical_field_from_label(base_label) or canonical_field_from_label(label)
    if canonical:
        return canonical
    normalized = normalize_header(base_label)
    if "品牌" in normalized and ("型号" in normalized or "规格" in normalized):
        return "brand_model"
    for field, hints in SEMANTIC_FIELD_HINTS.items():
        if any(hint.lower() in base_label.lower() for hint in hints):
            return field
    return None


def row_phone_values(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    direct = canonicalize_value(row.get("phone", ""))
    for number in extract_phone_numbers(direct):
        if number not in values:
            values.append(number)
    extras = safe_json_loads(row.get("extra_json"))
    for label, raw_value in extras.items():
        if infer_extra_semantic_field(label) != "phone":
            continue
        for number in extract_phone_numbers(raw_value):
            if number not in values:
                values.append(number)
    return values


def profile_rows(ledger_name: str, rows: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, set[str]]]:
    row_count = len(rows)
    field_presence: Counter[str] = Counter()
    field_labels: defaultdict[str, Counter[str]] = defaultdict(Counter)
    value_examples: defaultdict[str, Counter[str]] = defaultdict(Counter)
    semantic_terms: Counter[str] = Counter()
    unique_ip_values: set[str] = set()
    unique_phone_values: set[str] = set()
    base_ids: set[str] = set()
    content_hashes: set[str] = set()
    raw_ip_values: list[str] = []

    for token in tokenize_value(ledger_name):
        semantic_terms[token] += 2

    for row in rows:
        direct_ip = canonicalize_value(row.get("ip", ""))
        if direct_ip:
            raw_ip_values.append(direct_ip)
        extras = safe_json_loads(row.get("extra_json"))
        for label, raw_value in extras.items():
            if infer_extra_semantic_field(label) == "ip":
                raw_ip_values.append(raw_value)

    dominant_ip_prefix = infer_dominant_ipv4_prefix(raw_ip_values)

    for row in rows:
        row_fields: set[str] = set()
        flat = flatten_record_for_export(row)
        base_id = canonicalize_value(row.get("base_id") or row.get("record_id"))
        if base_id:
            base_ids.add(base_id)
        content_hash = canonicalize_value(row.get("content_hash"))
        if content_hash:
            content_hashes.add(content_hash)

        for field in ("entity_name", "location", "department", "owner", "status", "transport", "brand", "model", "remark"):
            value = canonicalize_value(row.get(field, ""))
            if not value:
                continue
            row_fields.add(field)
            if field in PROFILE_VALUE_FIELDS:
                value_examples[field][value] += 1
            for token in tokenize_value(value):
                semantic_terms[token] += 1

        direct_ip = canonicalize_value(row.get("ip", ""))
        for ip_value in extract_ipv4_values(direct_ip, dominant_ip_prefix):
            row_fields.add("ip")
            unique_ip_values.add(ip_value)
            value_examples["ip"][ip_value] += 1

        for phone in row_phone_values(row):
            row_fields.add("phone")
            unique_phone_values.add(phone)
            value_examples["phone"][phone] += 1

        extras = safe_json_loads(row.get("extra_json"))
        for label, raw_value in extras.items():
            value = canonicalize_value(raw_value)
            if not value:
                continue
            semantic_field = infer_extra_semantic_field(label)
            base_label = extra_key_base(label) or label
            if semantic_field:
                field_labels[semantic_field][base_label] += 1
            if semantic_field == "brand_model":
                brand_value, model_value = parse_brand_model_value(value)
                if brand_value:
                    row_fields.add("brand")
                    value_examples["brand"][brand_value] += 1
                    for token in tokenize_value(brand_value):
                        semantic_terms[token] += 1
                if model_value:
                    row_fields.add("model")
                    value_examples["model"][model_value] += 1
                    for token in tokenize_value(model_value):
                        semantic_terms[token] += 1
                continue
            if semantic_field == "phone":
                for phone in extract_phone_numbers(value):
                    row_fields.add("phone")
                    unique_phone_values.add(phone)
                    value_examples["phone"][phone] += 1
                continue
            if semantic_field == "ip":
                for ip_value in extract_ipv4_values(value, dominant_ip_prefix):
                    row_fields.add("ip")
                    unique_ip_values.add(ip_value)
                    value_examples["ip"][ip_value] += 1
                continue
            if semantic_field:
                row_fields.add(semantic_field)
                if semantic_field in PROFILE_VALUE_FIELDS:
                    value_examples[semantic_field][value] += 1
            for token in tokenize_value(base_label):
                semantic_terms[token] += 1
            for token in tokenize_value(value):
                semantic_terms[token] += 1

        for field in row_fields:
            field_presence[field] += 1

    field_ratios = {
        field: round(field_presence.get(field, 0) / row_count, 6) if row_count else 0.0
        for field in sorted(set(field_presence) | {"entity_name", "location", "ip", "department", "owner", "status", "transport", "brand", "model", "phone", "quantity", "asset"})
    }

    semantic_type_scores = {
        "contact": 0.0,
        "camera": 0.0,
        "network": 0.0,
        "asset": 0.0,
    }
    semantic_type_scores["contact"] += field_ratios.get("phone", 0.0) * 3.2
    semantic_type_scores["contact"] += field_ratios.get("owner", 0.0) * 1.0
    semantic_type_scores["contact"] += field_ratios.get("department", 0.0) * 0.6
    semantic_type_scores["camera"] += field_ratios.get("location", 0.0) * 1.5
    semantic_type_scores["camera"] += field_ratios.get("transport", 0.0) * 1.3
    semantic_type_scores["camera"] += field_ratios.get("brand", 0.0) * 0.9
    semantic_type_scores["camera"] += field_ratios.get("model", 0.0) * 0.9
    semantic_type_scores["network"] += field_ratios.get("ip", 0.0) * 2.2
    semantic_type_scores["network"] += field_ratios.get("owner", 0.0) * 0.8
    semantic_type_scores["network"] += field_ratios.get("department", 0.0) * 0.6
    semantic_type_scores["asset"] += field_ratios.get("asset", 0.0) * 1.8
    semantic_type_scores["asset"] += field_ratios.get("brand", 0.0) * 0.8
    semantic_type_scores["asset"] += field_ratios.get("model", 0.0) * 0.8
    semantic_type_scores["asset"] += field_ratios.get("status", 0.0) * 0.9
    semantic_type_scores["asset"] += field_ratios.get("location", 0.0) * 0.7
    semantic_type_scores["asset"] += field_ratios.get("owner", 0.0) * 0.7
    semantic_type_scores["asset"] += field_ratios.get("quantity", 0.0) * 1.1

    compact_terms = {normalize_compact(term): count for term, count in semantic_terms.items() if term}
    for kind, terms in SEMANTIC_TYPE_TERMS.items():
        for term in terms:
            compact = normalize_compact(term)
            if compact and compact in compact_terms:
                semantic_type_scores[kind] += compact_terms[compact] * 0.15
            if compact and compact in normalize_compact(ledger_name):
                semantic_type_scores[kind] += 1.0

    dominant_type = max(semantic_type_scores.items(), key=lambda item: item[1])[0] if semantic_type_scores else "generic"

    profile = {
        "ledger_name": normalize_ledger_name(ledger_name),
        "row_count": row_count,
        "field_ratios": field_ratios,
        "field_labels": {field: [label for label, _ in counter.most_common(20)] for field, counter in field_labels.items()},
        "value_examples": {field: [value for value, _ in counter.most_common(40)] for field, counter in value_examples.items()},
        "semantic_terms": [term for term, _ in semantic_terms.most_common(80)],
        "semantic_type_scores": {kind: round(score, 6) for kind, score in semantic_type_scores.items()},
        "dominant_type": dominant_type,
        "unique_ip_count": len(unique_ip_values),
        "dominant_ip_prefix": dominant_ip_prefix,
        "unique_phone_count": len(unique_phone_values),
        "base_id_count": len(base_ids),
        "content_hash_count": len(content_hashes),
    }
    fingerprints = {
        "base_ids": base_ids,
        "content_hashes": content_hashes,
    }
    return profile, fingerprints


class _UnionFind:
    def __init__(self, items: Iterable[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def ledger_base_name(ledger_name: str) -> str:
    return re.sub(r"-\d+$", "", normalize_ledger_name(ledger_name))


def determine_family_and_links(profiles: dict[str, dict[str, Any]], fingerprints: dict[str, dict[str, set[str]]]) -> tuple[dict[str, str], list[dict[str, Any]]]:
    ledgers = sorted(profiles)
    union_find = _UnionFind(ledgers)
    links: list[dict[str, Any]] = []

    for index, left in enumerate(ledgers):
        left_base = ledger_base_name(left)
        left_base_ids = fingerprints[left]["base_ids"]
        left_hashes = fingerprints[left]["content_hashes"]
        left_type = canonicalize_value(profiles[left].get("dominant_type", "generic"))
        for right in ledgers[index + 1 :]:
            right_base = ledger_base_name(right)
            right_base_ids = fingerprints[right]["base_ids"]
            right_hashes = fingerprints[right]["content_hashes"]
            right_type = canonicalize_value(profiles[right].get("dominant_type", "generic"))
            shared_base = len(left_base_ids & right_base_ids)
            shared_hash = len(left_hashes & right_hashes)
            min_base = min(len(left_base_ids), len(right_base_ids)) or 1
            min_hash = min(len(left_hashes), len(right_hashes)) or 1
            overlap_base = shared_base / min_base if min_base else 0.0
            overlap_hash = shared_hash / min_hash if min_hash else 0.0
            relation_type = None
            comparable = left not in SEMANTIC_INTERNAL_LEDGERS and right not in SEMANTIC_INTERNAL_LEDGERS
            same_type = left_type == right_type
            same_base = left_base == right_base and left_base not in SEMANTIC_INTERNAL_LEDGERS
            if comparable and (
                same_base
                or overlap_hash >= 0.85
                or (same_type and overlap_base >= 0.85 and overlap_hash >= 0.15)
            ):
                relation_type = "duplicate_family"
                union_find.union(left, right)
            elif overlap_hash >= 0.35 or (same_type and overlap_base >= 0.35):
                relation_type = "overlap"
            if relation_type:
                links.append(
                    {
                        "ledger_name": left,
                        "related_ledger_name": right,
                        "relation_type": relation_type,
                        "shared_base_count": shared_base,
                        "shared_hash_count": shared_hash,
                        "overlap_base": round(overlap_base, 6),
                        "overlap_hash": round(overlap_hash, 6),
                    }
                )
                links.append(
                    {
                        "ledger_name": right,
                        "related_ledger_name": left,
                        "relation_type": relation_type,
                        "shared_base_count": shared_base,
                        "shared_hash_count": shared_hash,
                        "overlap_base": round(overlap_base, 6),
                        "overlap_hash": round(overlap_hash, 6),
                    }
                )

    components: defaultdict[str, list[str]] = defaultdict(list)
    for ledger_name in ledgers:
        components[union_find.find(ledger_name)].append(ledger_name)

    family_names: dict[str, str] = {}
    for component_ledgers in components.values():
        normalized_ledgers = sorted(component_ledgers)
        preferred = [ledger for ledger in normalized_ledgers if ledger == ledger_base_name(ledger)]
        family_name = preferred[0] if preferred else normalized_ledgers[0]
        for ledger_name in normalized_ledgers:
            family_names[ledger_name] = family_name
    return family_names, links


def rebuild_ledger_semantics(connection, ledger_names: Sequence[str] | None = None) -> dict[str, dict[str, Any]]:
    rows = fetch_current_records(connection, ledger_name=None, include_deleted=False)
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        ledger_name = normalize_ledger_name(row["ledger_name"])
        grouped[ledger_name].append(dict(row))

    profiles: dict[str, dict[str, Any]] = {}
    fingerprints: dict[str, dict[str, set[str]]] = {}
    for ledger_name, ledger_rows in grouped.items():
        profile, fingerprint = profile_rows(ledger_name, ledger_rows)
        profiles[ledger_name] = profile
        fingerprints[ledger_name] = fingerprint

    family_names, links = determine_family_and_links(profiles, fingerprints)
    updated_at = connection.execute("SELECT datetime('now')").fetchone()[0]

    connection.execute("DELETE FROM ledger_semantics")
    connection.execute("DELETE FROM ledger_family_links")
    for ledger_name, profile in profiles.items():
        connection.execute(
            """
            INSERT INTO ledger_semantics (
                ledger_name, family_name, dominant_type, row_count, profile_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                ledger_name,
                family_names.get(ledger_name, ledger_name),
                profile.get("dominant_type", "generic"),
                int(profile.get("row_count", 0)),
                json.dumps(profile, ensure_ascii=False, sort_keys=True),
                updated_at,
            ),
        )
    for link in links:
        connection.execute(
            """
            INSERT INTO ledger_family_links (
                ledger_name, related_ledger_name, relation_type,
                shared_base_count, shared_hash_count, overlap_base, overlap_hash, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                link["ledger_name"],
                link["related_ledger_name"],
                link["relation_type"],
                int(link["shared_base_count"]),
                int(link["shared_hash_count"]),
                float(link["overlap_base"]),
                float(link["overlap_hash"]),
                updated_at,
            ),
        )
    return {ledger_name: {**profile, "family_name": family_names.get(ledger_name, ledger_name)} for ledger_name, profile in profiles.items()}


def ensure_ledger_semantics(connection) -> dict[str, dict[str, Any]]:
    active_counts = {
        normalize_ledger_name(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT ledger_name, COUNT(*) FROM records_current WHERE is_deleted = 0 GROUP BY ledger_name"
        ).fetchall()
    }
    stored = {
        normalize_ledger_name(row[0]): int(row[1])
        for row in connection.execute("SELECT ledger_name, row_count FROM ledger_semantics").fetchall()
    }
    if active_counts != stored:
        return rebuild_ledger_semantics(connection)
    profiles = load_ledger_semantics(connection)
    required_keys = {"field_ratios", "field_labels", "semantic_terms", "semantic_type_scores", "dominant_ip_prefix"}
    known_ledgers = set(active_counts)
    for ledger_name, profile in profiles.items():
        if not required_keys.issubset(profile):
            return rebuild_ledger_semantics(connection)
        family_name = normalize_ledger_name(profile.get("family_name"))
        if family_name and family_name not in known_ledgers and family_name != ledger_name:
            return rebuild_ledger_semantics(connection)
    return profiles


def load_ledger_semantics(connection) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    for row in connection.execute(
        "SELECT ledger_name, family_name, dominant_type, row_count, profile_json, updated_at FROM ledger_semantics"
    ).fetchall():
        profile = safe_json_loads(row[4])
        profile["ledger_name"] = normalize_ledger_name(row[0])
        profile["family_name"] = normalize_ledger_name(row[1])
        profile["dominant_type"] = canonicalize_value(row[2]) or profile.get("dominant_type", "generic")
        profile["row_count"] = int(row[3])
        profile["updated_at"] = canonicalize_value(row[5])
        profiles[profile["ledger_name"]] = profile
    return profiles
