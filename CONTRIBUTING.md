# Contributing

Contributions are welcome. The project is intentionally small and script-first, so the main bar is keeping behavior predictable and keeping private data out of the repository.

## Ground Rules

- Use synthetic sample data only. Do not commit real ledgers, exports, screenshots, API keys, or local regression fixtures.
- Keep runtime data outside the repository. The default runtime path is `~/.table-ledger-manager/`.
- Prefer focused pull requests. Separate behavior changes from docs-only cleanup when possible.

## Local Setup

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/init_db.py
```

## Tests

```bash
python tests/smoke_test.py
python -m unittest -q tests.query_capabilities_test tests.query_local_regression_test
```

`tests/query_local_regression_test.py` is optional by design. If `tests/regression_cases.local.json` is absent, the suite skips instead of failing.

## Pull Request Checklist

- Update or add tests when behavior changes.
- Keep README examples aligned with the current CLI and Web UI behavior.
- Re-scan changed files for secrets, internal addresses, and personal data before pushing.
