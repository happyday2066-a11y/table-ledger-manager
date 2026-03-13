---
name: table-ledger-manager
description: 通用台账/ledger 管理技能。用于导入、查询、更新、导出台账（CSV/XLSX/Markdown）。当用户提到“台账工具”或“导入台账/查询台账/更新台账/导出台账”时优先触发；适用于设备、联系人、资产、IP、点位等任意结构化台账，不限单一领域。
---

# Table Ledger Manager

## Overview

Use this skill for dynamic台账/表格管理：把 `xlsx`、`csv`、规范 Markdown 表格导入 SQLite，维护当前版本与历史版本，并用 embedding 索引辅助语义搜索。默认只回答当前最新版本；历史记录仅在用户明确要求时再查。

## When To Use

- 用户消息包含“台账工具”时，必须优先使用本 skill。
- 用户消息包含“导入台账 / 查询台账 / 更新台账 / 导出台账”时，必须优先使用本 skill。
- 用户要对任意结构化台账做导入、查询、更新、恢复、导出（如联系人、资产、IP、摄像头点位等）。
- 用户要求“默认最新版本、可追溯历史”的台账操作。

## Workflow

1. 安装或首次部署时运行一次 `scripts/init_db.py` 建库。
2. 导入数据时使用 `scripts/import_table.py`（必须显式 `--ledger`）。
3. 普通查询只走 `scripts/query_records.py --ask`（默认 `--ledger auto`）。
4. 更新、软删除、恢复走 `scripts/update_record.py`。
5. 当前版本导出走 `scripts/export_table.py`。
6. 仅在明确需要时才执行 `scripts/rebuild_embeddings.py`（不是普通查询前置步骤）。

## Core Rules

- 低自由度执行：触发后直接执行 `scripts/query_records.py` 或 `scripts/import_table.py`。
- 查询请求的首选命令必须是：
  `.venv/bin/python scripts/query_records.py --ledger auto --ask "<用户原话>" --limit 20`
- 如果是 `/new` 后第一条可执行任务，直接执行，不先寒暄、不复述策略。
- 只允许在正式目录 `skills/table-ledger-manager` 执行，不允许从 `table-ledger-manager.bak.*` 目录执行。
- 普通查询禁止前置步骤：`ls/pwd/find`、读取 `README.md`、读取脚本源码、`Memory Search`、`grep` 全盘、inline sqlite、先跑 `--help`。
- Python 解释器必须固定为 `.venv/bin/python`，禁止使用系统 `python`/`python3` 作为主路径。
- 查询和导入必须显式传 `--ledger`；用户未指定时查询默认 `auto`（跨账本），导入默认 `default`，并在用户回复里明确告知。
- 第一次查询无命中时，仅允许一次回退查询（例如 `--contains-location`），不允许多轮探测。
- 保持通用台账定位：不将技能限定为 IP 专用或摄像头专用。
- 禁止把 `scripts/init_db.py` 作为每次查询前置动作。

## Common Commands

- 初始化数据库（仅首次部署）：`python scripts/init_db.py`
- 导入表格：`python scripts/import_table.py <file>`
- 自然语言查询（推荐）：`python scripts/query_records.py --ledger auto --ask "三和里桩号"`
- 查字段：`python scripts/query_records.py --ledger auto --owner 张三`
- 查历史：`python scripts/query_records.py --history --record-id contacts::alice-zhang`
- 语义搜索：`python scripts/semantic_search.py "查备注里提到移交的记录"`
- 更新字段：`python scripts/update_record.py --record-id contacts::alice-zhang --set status=停用`
- 软删除：`python scripts/update_record.py --record-id contacts::alice-zhang --delete`
- 恢复记录：`python scripts/update_record.py --record-id contacts::alice-zhang --restore`
- 导出 CSV：`python scripts/export_table.py --format csv`
- 导出 Excel：`python scripts/export_table.py --format xlsx`

## Trigger Examples

- “用台账工具查一下这条记录”
- “导入台账：把这个 Excel 入库”
- “查询台账：河道邢村那个点位桩号是多少”
- “更新台账：把这条记录状态改成停用”
- “导出台账：给我最新版 Excel”
- “请在 default 账本里查 owner=张三 的记录”
- “请用台账工具查询：--ledger auto 三和里桩号”

## Do Not Use

- 不要用它处理 Word、OCR、图片、纯自由文本知识库
- 不要把向量结果直接当最终答案
- 不要在字段识别失败时自行猜测列含义
- 不要在未回查 current 表时返回历史/过期版本
