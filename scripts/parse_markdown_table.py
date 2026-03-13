#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from common import LedgerError, MARKDOWN_DELIMITER_PATTERN, print_json


def split_markdown_row(line: str) -> list[str]:
    clean_line = line.lstrip("\ufeff").strip()
    return [cell.strip() for cell in clean_line.strip("|").split("|")]


def extract_candidate_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current_block: list[str] = []
    for line in lines:
        if "|" in line and line.strip():
            current_block.append(line.rstrip())
        else:
            if current_block:
                blocks.append(current_block)
                current_block = []
    if current_block:
        blocks.append(current_block)
    return blocks


def parse_markdown_table_file(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path).expanduser().resolve()
    if not path.is_file():
        raise LedgerError(f"Input file does not exist: {path}")
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    blocks = extract_candidate_blocks(lines)
    combined_rows: list[dict[str, str]] = []
    headers_reference: list[str] | None = None
    for block in blocks:
        if len(block) < 3:
            continue
        if not MARKDOWN_DELIMITER_PATTERN.match(block[1]):
            continue
        headers = split_markdown_row(block[0])
        if not headers or any(not cell for cell in headers):
            raise LedgerError("Markdown table header contains blank columns.")
        for line_number, line in enumerate(block[2:], start=3):
            if not line.strip():
                continue
            cells = split_markdown_row(line)
            if len(cells) != len(headers):
                raise LedgerError(
                    f"Markdown table row has {len(cells)} cells but expected {len(headers)}: line {line_number}"
                )
            if headers_reference and headers_reference != headers:
                raise LedgerError("Multiple Markdown tables with different headers are not supported.")
            headers_reference = headers
            combined_rows.append(dict(zip(headers, cells)))
    if not combined_rows:
        raise LedgerError("Markdown file does not contain a valid table.")
    return pd.DataFrame(combined_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse a Markdown table file into JSON.")
    parser.add_argument("input_file", help="Path to the Markdown file.")
    args = parser.parse_args()
    try:
        dataframe = parse_markdown_table_file(args.input_file)
    except LedgerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print_json(dataframe.to_dict(orient="records"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
