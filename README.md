# table-ledger-manager

`table-ledger-manager` 是一个面向动态结构化台账的通用 skill，也可以独立作为本地工具或 FastAPI Web UI 使用。它把 `CSV`、`Excel`、规范 Markdown 表格导入 SQLite，维护当前版本与历史版本，并用可替换的 embedding provider 为当前记录构建向量索引。

仓库中的样例数据均为合成示例，仅用于测试与文档说明，不包含真实业务台账、真实内网地址或个人隐私信息。

## 功能概述

- 导入 `xlsx` / `csv` / Markdown 表格
- 字段识别与标准化映射
- SQLite 维护 `records_current` / `records_history` / `operations_log` / `embeddings_meta`
- 默认只查询当前最新版本
- 修改、软删除、恢复都可追溯
- 语义搜索先召回，再回查 SQLite current 表
- 导出当前最新版本为 `CSV` 或 `Excel`
- 场景无关，可复用到联系人、资产、设备、IP 台账等结构化数据

## 项目结构

```text
table-ledger-manager/
├── .github/workflows/ci.yml
├── CONTRIBUTING.md
├── LICENSE
├── README.md
├── SKILL.md
├── install.ps1
├── install.sh
├── requirements.txt
├── examples/
│   └── sample_queries.md
├── scripts/
│   ├── build_package.py
│   ├── common.py
│   ├── export_table.py
│   ├── import_table.py
│   ├── init_db.py
│   ├── ledger_semantics.py
│   ├── parse_markdown_table.py
│   ├── query_records.py
│   ├── rebuild_embeddings.py
│   ├── semantic_search.py
│   └── update_record.py
├── static/
│   └── app.css
├── templates/
│   └── index.html
├── tests/
│   ├── query_capabilities_test.py
│   ├── query_local_regression_test.py
│   ├── sample.csv
│   ├── sample.md
│   └── smoke_test.py
└── web_ui.py
```

## 快速开始

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/init_db.py
```

默认数据库文件路径：

- `~/.table-ledger-manager/data/db/ledger.db`

默认运行数据放在用户目录下，重装或替换 skill 目录不会清空数据库与索引。

## 环境变量

所有敏感信息都从环境变量读取：

- `LEDGER_EMBEDDING_PROVIDER`
- `LEDGER_EMBEDDING_BASE_URL`
- `LEDGER_EMBEDDING_API_KEY`
- `LEDGER_EMBEDDING_MODEL`
- `LEDGER_DB_PATH`（可选）
- `LEDGER_INDEX_PATH`（可选）
- `LEDGER_DATA_DIR`（可选，用于覆盖整个数据根目录）

生产环境可使用 OpenAI 兼容 embedding 接口，例如：

```bash
export LEDGER_EMBEDDING_PROVIDER=openai-compatible
export LEDGER_EMBEDDING_BASE_URL=https://your-host/v1
export LEDGER_EMBEDDING_API_KEY=your_key
export LEDGER_EMBEDDING_MODEL=text-embedding-3-small
```

本地测试可使用内置 mock provider：

```bash
export LEDGER_EMBEDDING_PROVIDER=mock
```

## 使用方式

### 作为命令行工具

导入 CSV：

```bash
python scripts/import_table.py tests/sample.csv --ledger assets
```

导入指定 Excel Sheet：

```bash
python scripts/import_table.py ./ledger.xlsx --ledger assets --sheet AssetLedger
python scripts/import_table.py ./ledger.xlsx --ledger assets --sheet-regex "asset|contact"
python scripts/import_table.py ./ledger.xlsx --ledger assets --exclude-sheet Archive --exclude-sheet Sheet1
```

导入 Markdown 表格：

```bash
python scripts/import_table.py tests/sample.md --ledger assets
```

同步删除缺失记录：

```bash
python scripts/import_table.py ./ledger.csv --ledger assets --sync-delete-missing
```

精确查询：

```bash
python scripts/query_records.py --ledger assets --ip 198.51.100.11
python scripts/query_records.py --ledger assets --department Operations --status active
python scripts/query_records.py --ledger assets --history --ip 198.51.100.11
```

自然语言查询：

```bash
python scripts/query_records.py --ledger assets --ask "一共有多少条记录"
python scripts/query_records.py --ledger assets --ask "列出 Zone-B 的明细"
python scripts/query_records.py --ledger contacts --ask "how many active contacts"
```

语义搜索：

```bash
python scripts/semantic_search.py "查备注里提到移交的记录" --ledger assets
python scripts/semantic_search.py "找仍在使用旧 IP 的示例设备" --ledger assets
```

更新 / 删除 / 恢复：

```bash
python scripts/update_record.py --ledger assets --ip 198.51.100.11 --set owner=ExampleOwner --message "owner updated"
python scripts/update_record.py --ledger assets --ip 198.51.100.11 --delete --message "record retired"
python scripts/update_record.py --ledger assets --ip 198.51.100.11 --restore --message "record restored"
```

导出：

```bash
python scripts/export_table.py --ledger assets --format csv
python scripts/export_table.py --ledger assets --format xlsx
```

重建 embedding 索引：

```bash
python scripts/rebuild_embeddings.py
```

### 作为 Web UI

```bash
uvicorn web_ui:app --reload
```

启动后访问本地 FastAPI 页面进行导入、查询和导出。

## OpenClaw 安装方式

### 方式一：直接拷贝到 OpenClaw skill 目录

把整个 `table-ledger-manager` 文件夹复制到 OpenClaw 的 skill 根目录下，然后执行：

```bash
cd /path/to/openclaw/skills/table-ledger-manager
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/init_db.py
```

### 方式二：一键安装脚本

```bash
./install.sh /path/to/openclaw/skills
```

如果不想自动创建虚拟环境：

```bash
CREATE_VENV=0 ./install.sh /path/to/openclaw/skills python3
```

Windows 下可用：

```powershell
.\install.ps1 -TargetRoot D:\OpenClaw\skills
```

### 方式三：先打包，再发给 OpenClaw 安装

```bash
python scripts/build_package.py --format all
```

会在 `dist/` 下生成：

- `table-ledger-manager-<timestamp>.zip`
- `table-ledger-manager-<timestamp>.tar.gz`

## 测试

运行端到端 smoke test：

```bash
python tests/smoke_test.py
```

运行查询回归测试：

```bash
python -m unittest -q tests.query_capabilities_test tests.query_local_regression_test
```

说明：`tests/query_local_regression_test.py` 在没有本地回归用例文件时会自动跳过，避免把内部测试台账提交到仓库。

## 数据与隐私

- 仓库中的 `tests/sample.csv`、`tests/sample.md` 以及 README 示例全部为合成数据。
- 运行时数据默认写入用户目录 `~/.table-ledger-manager/`，不写入仓库。
- `.gitignore` 已排除数据库、导出文件、运行时表格、`.env` 和本地回归样例。
- 如果使用真实业务台账，请放在仓库外部路径，并通过环境变量或命令行参数传入。

## 常见错误

- `Input file does not exist`：输入文件路径错误
- `Markdown file does not contain a valid table`：Markdown 中没有规范表格
- `Unrecognized columns`：表头无法映射到标准字段
- `Target record does not exist`：更新、删除或恢复的目标记录不存在
- `Embedding provider is not configured`：执行语义搜索或需要刷新索引时未设置 embedding 环境变量
- `Vector index is out of sync with current records`：索引和 `records_current` 不一致，需要执行 `python scripts/rebuild_embeddings.py`
