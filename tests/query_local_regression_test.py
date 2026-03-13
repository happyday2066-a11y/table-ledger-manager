#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOCAL_CASES = ROOT / "tests" / "regression_cases.local.json"


def load_cases() -> list[dict]:
    if not LOCAL_CASES.exists():
        return []
    return json.loads(LOCAL_CASES.read_text(encoding="utf-8"))


class LocalQueryRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        sys.path.insert(0, str(ROOT))
        os.environ.pop("LEDGER_DATA_DIR", None)
        module = importlib.import_module("web_ui")
        cls.web_ui = importlib.reload(module)
        cls.web_ui.invalidate_runtime_cache()
        cls.web_ui.load_all_active_rows(force_refresh=True)

    def run_query(self, question: str, ledger: str = "auto") -> dict:
        self.web_ui.invalidate_runtime_cache()
        return self.web_ui.run_query(question, ledger, 1)

    def test_local_regression_cases(self) -> None:
        cases = load_cases()
        if not cases:
            self.skipTest("local regression cases are not configured")

        for case in cases:
            with self.subTest(case=case["name"]):
                result = self.run_query(case["question"], case.get("ledger", "auto"))
                expected = case["expected"]

                if "total_count" in expected:
                    self.assertEqual(result["total_count"], expected["total_count"])
                if "raw_hit_count" in expected:
                    self.assertEqual(result["raw_hit_count"], expected["raw_hit_count"])
                if "included_ledgers" in expected:
                    self.assertEqual(sorted(result["included_ledgers"]), sorted(expected["included_ledgers"]))
                if "excluded_ledgers_contains" in expected:
                    for ledger_name in expected["excluded_ledgers_contains"]:
                        self.assertIn(ledger_name, result["excluded_ledgers"])
                if "answer_contains" in expected:
                    self.assertIn(expected["answer_contains"], result["answer"])
                if "answer_contains_all" in expected:
                    for fragment in expected["answer_contains_all"]:
                        self.assertIn(fragment, result["answer"])
                if "cards_min" in expected:
                    self.assertGreaterEqual(len(result["cards"]), expected["cards_min"])
                if "card_titles_include" in expected:
                    titles = [card["title"] for card in result["cards"]]
                    for title in expected["card_titles_include"]:
                        self.assertIn(title, titles)
                if "cross_ledger_total_unique" in expected:
                    self.assertIsNotNone(result["cross_ledger_stats"])
                    self.assertEqual(result["cross_ledger_stats"]["total_unique"], expected["cross_ledger_total_unique"])


if __name__ == "__main__":
    unittest.main()
