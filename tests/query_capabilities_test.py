#!/usr/bin/env python3
from __future__ import annotations

import csv
import importlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
PYTHON = sys.executable


def run_script(script_name: str, *args: str, env: dict[str, str]) -> dict:
    result = subprocess.run(
        [PYTHON, str(SCRIPTS_DIR / script_name), *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


class QueryCapabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.original_ledger_data_dir = os.environ.get("LEDGER_DATA_DIR")
        cls.original_embedding_provider = os.environ.get("LEDGER_EMBEDDING_PROVIDER")
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.data_dir = Path(cls.temp_dir.name) / "data"
        cls.env = os.environ.copy()
        cls.env["LEDGER_DATA_DIR"] = str(cls.data_dir)
        cls.env["LEDGER_EMBEDDING_PROVIDER"] = "mock"

        network_csv = Path(cls.temp_dir.name) / "网络资产表示例.csv"
        device_csv = Path(cls.temp_dir.name) / "设备点位表示例.csv"
        contact_csv = Path(cls.temp_dir.name) / "通讯录表示例.csv"
        asset_csv = Path(cls.temp_dir.name) / "资产台账表示例.csv"

        write_csv(
            network_csv,
            ["Name", "Department", "Owner", "IP Address", "Status"],
            [
                ["办公终端A", "Backoffice", "Alice", "10.0.0.1", "active"],
                ["办公终端B", "Backoffice", "Bob", "10.0.0.2", "active"],
                ["财务打印机C", "Finance", "Carol", "10.0.0.3", "active"],
                ["档案终端D", "Finance", "", "", "inactive"],
            ],
        )
        write_csv(
            device_csv,
            ["Name", "Location", "Department", "IP Address", "Transport", "Status", "Remark"],
            [
                ["北塔设备", "北塔点位", "现场运维部", "10.0.0.3", "4G", "active", "shared ip"],
                ["南塔设备", "南塔点位", "现场运维部", "10.0.0.4", "4G", "active", ""],
                ["东门设备", "东门点位", "现场运维部", "10.0.0.5", "Fiber", "active", ""],
                ["库房设备", "库房点位", "现场运维部", "", "Fiber", "inactive", ""],
            ],
        )
        write_csv(
            contact_csv,
            ["Name", "Department", "Phone", "Mobile Number"],
            [
                ["Alice", "Backoffice", "4001", "13800001111"],
                ["Bob", "Backoffice", "4002", "13800002222"],
                ["Carol", "Finance", "4003", "13800003333"],
            ],
        )
        write_csv(
            asset_csv,
            ["资产名称", "存放地点", "使用人", "使用状况", "品牌、规格型号", "数量"],
            [
                ["防火墙系统", "主机房", "Alice", "在用", "VenusTech / USG-1000", "1"],
                ["接入交换机", "主机房", "Bob", "在用", "H3C / S5120", "2"],
                ["办公笔记本", "财务室", "Carol", "在用", "Lenovo / ThinkPad T14", "3"],
            ],
        )

        run_script("init_db.py", env=cls.env)
        run_script("import_table.py", str(network_csv), "--ledger", "网络资产表示例", env=cls.env)
        run_script("import_table.py", str(device_csv), "--ledger", "设备点位表示例", env=cls.env)
        run_script("import_table.py", str(device_csv), "--ledger", "设备点位表示例-2", env=cls.env)
        run_script("import_table.py", str(contact_csv), "--ledger", "通讯录表示例", env=cls.env)
        run_script("import_table.py", str(asset_csv), "--ledger", "资产台账表示例", env=cls.env)

        os.environ["LEDGER_DATA_DIR"] = str(cls.data_dir)
        os.environ["LEDGER_EMBEDDING_PROVIDER"] = "mock"
        sys.path.insert(0, str(ROOT))
        cls.web_ui = importlib.import_module("web_ui")
        cls.web_ui.invalidate_runtime_cache()
        cls.web_ui.load_all_active_rows(force_refresh=True)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()
        if cls.original_ledger_data_dir is None:
            os.environ.pop("LEDGER_DATA_DIR", None)
        else:
            os.environ["LEDGER_DATA_DIR"] = cls.original_ledger_data_dir
        if cls.original_embedding_provider is None:
            os.environ.pop("LEDGER_EMBEDDING_PROVIDER", None)
        else:
            os.environ["LEDGER_EMBEDDING_PROVIDER"] = cls.original_embedding_provider

    def run_query(self, question: str) -> dict:
        self.web_ui.invalidate_runtime_cache()
        return self.web_ui.run_query(question, "auto", 1)

    def test_cross_ledger_unique_ip_count(self) -> None:
        result = self.run_query("有多少ip地址")
        self.assertEqual(result["total_count"], 5)
        self.assertEqual(result["included_ledgers"], ["网络资产表示例", "设备点位表示例"])
        self.assertIsNotNone(result["cross_ledger_stats"])
        self.assertEqual(result["cross_ledger_stats"]["total_unique"], 5)

    def test_ledger_inventory_count_collapses_duplicate_families(self) -> None:
        result = self.run_query("台账数量")
        self.assertEqual(result["total_count"], 4)
        self.assertEqual(
            sorted(result["included_ledgers"]),
            sorted(["网络资产表示例", "资产台账表示例", "设备点位表示例", "通讯录表示例"]),
        )
        self.assertIn("4 本有效台账", result["answer"])

    def test_bare_ip_query_is_rejected(self) -> None:
        result = self.run_query("ip地址")
        self.assertEqual(result["total_count"], 0)
        self.assertEqual(result["included_ledgers"], [])
        self.assertIn("输入过于模糊", result["answer"])

    def test_object_scoped_ip_count_prefers_device_ledger(self) -> None:
        result = self.run_query("有IP地址的设备有几个")
        self.assertEqual(result["total_count"], 3)
        self.assertEqual(result["included_ledgers"], ["设备点位表示例"])
        self.assertIn("3", result["answer"])

    def test_object_scoped_ip_count_with_exists_phrase(self) -> None:
        result = self.run_query("带有IP地址的设备有几个")
        self.assertEqual(result["total_count"], 3)
        self.assertEqual(result["included_ledgers"], ["设备点位表示例"])
        self.assertIn("3", result["answer"])

    def test_list_department_points(self) -> None:
        result = self.run_query("列出现场运维部的点位")
        self.assertEqual(result["total_count"], 4)
        self.assertEqual(result["included_ledgers"], ["设备点位表示例"])
        titles = [card["title"] for card in result["cards"]]
        self.assertIn("北塔点位", titles)
        self.assertIn("南塔点位", titles)

    def test_detail_lookup_owner_ip(self) -> None:
        result = self.run_query("Alice的IP是多少")
        self.assertEqual(result["included_ledgers"], ["网络资产表示例"])
        self.assertIn("10.0.0.1", result["answer"])

    def test_phone_lookup_with_explicit_contact_context(self) -> None:
        result = self.run_query("通讯录里Alice的电话")
        self.assertEqual(result["total_count"], 1)
        self.assertIn("4001", result["answer"])
        self.assertIn("13800001111", result["answer"])
        self.assertEqual(result["included_ledgers"], ["通讯录表示例"])
        titles = [card["title"] for card in result["cards"]]
        self.assertIn("Alice", titles)
        export_payload = self.web_ui.EXPORT_CACHE[result["export_token"]]
        export_rows = self.web_ui.build_export_rows(export_payload["rows"], export_payload["plan"], export_payload.get("grouped_rows"))
        export_columns = self.web_ui.choose_export_columns(export_rows, export_payload["plan"], export_payload.get("grouped_rows"))
        self.assertNotIn("source_file", export_columns)
        self.assertNotIn("version_no", export_columns)
        self.assertNotIn("is_deleted", export_columns)

    def test_filtered_object_count(self) -> None:
        result = self.run_query("有多少塔设备")
        self.assertEqual(result["total_count"], 2)
        self.assertEqual(result["included_ledgers"], ["设备点位表示例"])

    def test_phone_count_across_ledgers(self) -> None:
        result = self.run_query("统计下有多少电话号码")
        self.assertIn("办公/座机号码 3 个", result["answer"])
        self.assertIn("手机号码 3 个", result["answer"])
        self.assertIn("合计 6 个号码", result["answer"])
        self.assertEqual(result["included_ledgers"], ["通讯录表示例"])
        export_payload = self.web_ui.EXPORT_CACHE[result["export_token"]]
        export_rows = self.web_ui.build_export_rows(export_payload["rows"], export_payload["plan"], export_payload.get("grouped_rows"))
        self.assertEqual(len(export_rows), 3)
        self.assertEqual(
            sorted(self.web_ui.choose_export_columns(export_rows, export_payload["plan"], export_payload.get("grouped_rows"))),
            sorted(["ledger_name", "record_id", "entity_name", "department", "phone", "phone_landline", "phone_mobile"]),
        )
        self.assertTrue(all("::phone::" not in row["record_id"] for row in export_rows))

    def test_duplicate_ledger_family_does_not_double_count(self) -> None:
        result = self.run_query("统计下有多少设备")
        self.assertEqual(result["total_count"], 4)
        self.assertEqual(result["included_ledgers"], ["设备点位表示例"])

    def test_semantic_profiles_and_duplicate_family_links_exist(self) -> None:
        connection = self.web_ui.connect_db()
        try:
            rows = connection.execute(
                "SELECT ledger_name, family_name, dominant_type, row_count FROM ledger_semantics "
                "WHERE ledger_name IN ('设备点位表示例', '设备点位表示例-2') ORDER BY ledger_name"
            ).fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["family_name"], "设备点位表示例")
            self.assertEqual(rows[1]["family_name"], "设备点位表示例")
            self.assertEqual(rows[0]["row_count"], 4)
            links = connection.execute(
                "SELECT relation_type FROM ledger_family_links WHERE ledger_name = '设备点位表示例' AND related_ledger_name = '设备点位表示例-2'"
            ).fetchall()
            self.assertEqual([row["relation_type"] for row in links], ["duplicate_family"])
        finally:
            connection.close()

    def test_asset_ledger_count_and_brand_lookup(self) -> None:
        count_result = self.run_query("资产相关台账共有多少资产?")
        self.assertEqual(count_result["total_count"], 3)
        self.assertEqual(count_result["included_ledgers"], ["资产台账表示例"])
        status_count = self.run_query("在用资产有多少")
        self.assertEqual(status_count["total_count"], 3)
        self.assertIn("相关记录共 3 条", status_count["answer"])
        self.assertEqual(status_count["included_ledgers"], ["资产台账表示例"])
        brand_result = self.run_query("防火墙品牌")
        self.assertIn("VenusTech", brand_result["answer"])
        self.assertEqual(brand_result["included_ledgers"], ["资产台账表示例"])

    def test_phone_detail_export_prefers_single_contact_row(self) -> None:
        result = self.run_query("Alice的电话")
        export_payload = self.web_ui.EXPORT_CACHE[result["export_token"]]
        export_rows = self.web_ui.build_export_rows(export_payload["rows"], export_payload["plan"], export_payload.get("grouped_rows"))
        self.assertEqual(len(export_rows), 1)
        self.assertEqual(export_rows[0]["entity_name"], "Alice")
        self.assertEqual(export_rows[0]["phone_landline"], "4001")
        self.assertEqual(export_rows[0]["phone_mobile"], "13800001111")
        first_card_values = [value for _, value in result["cards"][0]["pairs"]]
        self.assertNotIn("示例旧值", first_card_values)

    def test_detail_ip_answer_and_export_deduplicate_duplicate_values(self) -> None:
        plan = self.web_ui.QueryPlan(
            question="Alice的IP是多少",
            intent="detail",
            answer_field="ip",
            group_field=None,
            dedupe_key=None,
            global_scope=False,
            existence_fields=(),
            subject="Alice",
            filter_fields=(),
            free_terms=(),
        )
        rows = [
            {
                "ledger_name": "网络资产表示例",
                "record_id": "网络资产表示例::10.0.0.1",
                "entity_name": "Alice",
                "owner": "Alice",
                "department": "Backoffice",
                "ip": "10.0.0.1",
            },
            {
                "ledger_name": "网络资产表示例-2",
                "record_id": "网络资产表示例-2::10.0.0.6",
                "entity_name": "Alice",
                "owner": "Alice",
                "department": "Backoffice",
                "ip": "10.0.0.6",
            },
            {
                "ledger_name": "网络资产表示例-3",
                "record_id": "网络资产表示例-3::10.0.0.1",
                "entity_name": "Alice",
                "owner": "Alice",
                "department": "Backoffice",
                "ip": "10.0.0.1",
            },
        ]
        answer = self.web_ui.answer_for_detail(plan, rows)
        self.assertIn("2 个", answer)
        self.assertIn("10.0.0.1", answer)
        self.assertIn("10.0.0.6", answer)

        export_rows = self.web_ui.build_export_rows(rows, plan)
        self.assertEqual(len(export_rows), 2)
        self.assertEqual(sorted(row["ip"] for row in export_rows), ["10.0.0.1", "10.0.0.6"])
        columns = self.web_ui.choose_export_columns(export_rows, plan)
        self.assertNotIn("record_id", columns)

    def test_group_export_uses_group_template_columns(self) -> None:
        result = self.run_query("各部门分别多少个设备")
        export_payload = self.web_ui.EXPORT_CACHE[result["export_token"]]
        export_rows = self.web_ui.build_export_rows(export_payload["rows"], export_payload["plan"], export_payload.get("grouped_rows"))
        self.assertGreaterEqual(len(export_rows), 1)
        columns = self.web_ui.choose_export_columns(export_rows, export_payload["plan"], export_payload.get("grouped_rows"))
        self.assertEqual(columns, ["group_label", "group_value", "record_count"])


if __name__ == "__main__":
    unittest.main()
