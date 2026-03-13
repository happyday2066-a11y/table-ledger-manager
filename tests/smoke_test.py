#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


SKILL_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = SKILL_ROOT / "scripts"
PYTHON = sys.executable


def run_script(script_name: str, *args: str, env: dict[str, str]) -> dict:
    result = subprocess.run(
        [PYTHON, str(SCRIPTS_DIR / script_name), *args],
        cwd=SKILL_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def build_messy_xlsx(path: Path) -> None:
    rows = [
        ["Asset Ledger", "", "", "", "", "", "", "", ""],
        ["Generated:2026-03-07", "", "", "", "", "", "", "", ""],
        ["No", "Name", "Location", "Brand", "Model", "IP Address", "Transport", "Remark", "Owner"],
        [1, "Example Camera C", "Zone-B", "DAHUA", "DH-SD-6CMXK", "198.51.100.12", "4G", "from-messy-a", "ops-a"],
        [2, "Example Camera C", "Zone-B", "DAHUA", "DH-SD-6CMXK", "198.51.100.12", "4G", "from-messy-b", "ops-b"],
        [3, "Example Sensor E", "Zone-D", "HIKVISION", "DS-2CD", "198.51.100.21", "Fiber", "new sensor", "ops-c"],
    ]
    pd.DataFrame(rows).to_excel(path, header=False, index=False)


def build_contacts_csv(path: Path) -> None:
    rows = [
        {"Full Name": "Alice Zhang", "Team": "Ops", "Phone": "13800001111", "Status": "active", "City": "Shanghai"},
        {"Full Name": "Bob Li", "Team": "Ops", "Phone": "13800002222", "Status": "inactive", "City": "Nanjing"},
        {"Full Name": "Carol Wu", "Team": "Finance", "Phone": "13800003333", "Status": "active", "City": "Suzhou"},
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def build_multi_sheet_xlsx(path: Path) -> None:
    keep_rows = [
        ["Name", "Department", "Status"],
        ["Alice Zhang", "Ops", "active"],
        ["Bob Li", "Ops", "inactive"],
    ]
    skip_rows = [
        ["Name", "Department", "Status"],
        ["Should Skip", "Temp", "active"],
    ]
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(keep_rows).to_excel(writer, sheet_name="Contacts", header=False, index=False)
        pd.DataFrame(skip_rows).to_excel(writer, sheet_name="Router Stats", header=False, index=False)


def main() -> int:
    with tempfile.TemporaryDirectory() as temp_dir:
        env = os.environ.copy()
        data_dir = Path(temp_dir) / "data"
        env["LEDGER_DATA_DIR"] = str(data_dir)
        env["LEDGER_EMBEDDING_PROVIDER"] = "mock"

        init_payload = run_script("init_db.py", env=env)
        assert init_payload["status"] == "initialized"

        csv_payload = run_script("import_table.py", str(SKILL_ROOT / "tests" / "sample.csv"), "--ledger", "assets", env=env)
        assert csv_payload["imported"] == 3

        md_payload = run_script("import_table.py", str(SKILL_ROOT / "tests" / "sample.md"), "--ledger", "assets", env=env)
        assert md_payload["updated"] == 1
        assert md_payload["imported"] == 1

        ask_count_payload = run_script("query_records.py", "--ledger", "assets", "--ask", "how many records are tracked", env=env)
        assert ask_count_payload["type"] == "count"
        assert ask_count_payload["count"] == 4

        ask_detail_payload = run_script("query_records.py", "--ledger", "assets", "--ask", "show details in Zone-B", env=env)
        assert ask_detail_payload["type"] == "detail"
        assert ask_detail_payload["count"] == 2

        update_payload = run_script(
            "update_record.py",
            "--ledger",
            "assets",
            "--ip",
            "198.51.100.11",
            "--set",
            "transport=4G",
            "--message",
            "switch transport to 4g",
            env=env,
        )
        assert update_payload["operation"] == "update"

        ask_4g_payload = run_script("query_records.py", "--ledger", "assets", "--ask", "how many use 4g transport", env=env)
        assert ask_4g_payload["count"] == 3

        messy_xlsx = Path(temp_dir) / "messy.xlsx"
        build_messy_xlsx(messy_xlsx)
        messy_payload = run_script(
            "import_table.py",
            str(messy_xlsx),
            "--ledger",
            "assets",
            "--duplicate-strategy",
            "keep-last",
            env=env,
        )
        assert messy_payload["duplicates"] >= 1
        assert messy_payload["imported"] >= 1

        verify_payload = run_script("query_records.py", "--ledger", "assets", "--ip", "198.51.100.12", env=env)
        assert verify_payload["count"] == 1
        assert "from-messy-b" in verify_payload["records"][0]["remark"]

        semantic_payload = run_script("semantic_search.py", "handover", "--ledger", "assets", env=env)
        assert semantic_payload["count"] >= 1

        export_payload = run_script(
            "export_table.py",
            "--ledger",
            "assets",
            "--format",
            "csv",
            "--columns",
            "Name,IP Address",
            env=env,
        )
        export_file = Path(export_payload["output_file"])
        assert export_file.is_file()
        exported = pd.read_csv(export_file)
        assert list(exported.columns) == ["Name", "IP Address"]
        assert len(exported.index) >= 5

        contacts_csv = Path(temp_dir) / "contacts.csv"
        build_contacts_csv(contacts_csv)
        contacts_payload = run_script("import_table.py", str(contacts_csv), "--ledger", "contacts", env=env)
        assert contacts_payload["imported"] == 3

        multi_sheet = Path(temp_dir) / "contacts-multi.xlsx"
        build_multi_sheet_xlsx(multi_sheet)
        filtered_payload = run_script(
            "import_table.py",
            str(multi_sheet),
            "--ledger",
            "contacts-sheet",
            "--sheet",
            "Contacts",
            "--exclude-sheet",
            "Router Stats",
            env=env,
        )
        assert filtered_payload["processed"] == 2
        assert filtered_payload["sheet_filters"]["include"] == ["Contacts"]

        contacts_count_payload = run_script(
            "query_records.py",
            "--ledger",
            "contacts",
            "--ask",
            "how many active contacts",
            env=env,
        )
        assert contacts_count_payload["type"] == "count"
        assert contacts_count_payload["count"] == 2

        contacts_export_payload = run_script(
            "export_table.py",
            "--ledger",
            "contacts",
            "--format",
            "csv",
            "--columns",
            "Full Name,Phone",
            env=env,
        )
        contacts_export_file = Path(contacts_export_payload["output_file"])
        assert contacts_export_file.is_file()
        contacts_exported = pd.read_csv(contacts_export_file)
        assert list(contacts_exported.columns) == ["Full Name", "Phone"]
        assert len(contacts_exported.index) == 3

        assets_total_payload = run_script("query_records.py", "--ledger", "assets", "--count", env=env)
        contacts_total_payload = run_script("query_records.py", "--ledger", "contacts", "--count", env=env)
        assert assets_total_payload["count"] == 5
        assert contacts_total_payload["count"] == 3

    print("smoke test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
