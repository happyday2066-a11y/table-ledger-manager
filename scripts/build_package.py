#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import tarfile
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

SKILL_ROOT = Path(__file__).resolve().parent.parent
SKILL_NAME = SKILL_ROOT.name
RUNTIME_DIRS = [
    Path('data/raw'),
    Path('data/db'),
    Path('data/index'),
    Path('data/logs'),
    Path('data/exports'),
]
EXCLUDED_TOP_LEVEL = {'.venv', '.tmpdata', 'dist', '__pycache__'}
EXCLUDED_PARTS = {'__pycache__', '.pytest_cache'}
EXCLUDED_SUFFIXES = {'.pyc', '.pyo'}


def iter_included_files() -> list[Path]:
    files: list[Path] = []
    for path in SKILL_ROOT.rglob('*'):
        relative = path.relative_to(SKILL_ROOT)
        if not relative.parts:
            continue
        if relative.parts[0] in EXCLUDED_TOP_LEVEL:
            continue
        if any(part in EXCLUDED_PARTS for part in relative.parts):
            continue
        if path.is_dir():
            continue
        if path.suffix in EXCLUDED_SUFFIXES:
            continue
        if len(relative.parts) >= 2 and relative.parts[0] == 'data':
            continue
        files.append(relative)
    return sorted(files)


def ensure_runtime_dirs(destination_root: Path) -> None:
    for runtime_dir in RUNTIME_DIRS:
        (destination_root / runtime_dir).mkdir(parents=True, exist_ok=True)


def copy_tree(destination_root: Path) -> None:
    if destination_root.exists():
        shutil.rmtree(destination_root)
    destination_root.mkdir(parents=True, exist_ok=True)
    for relative in iter_included_files():
        source = SKILL_ROOT / relative
        target = destination_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    ensure_runtime_dirs(destination_root)


def build_archives(dist_dir: Path, package_format: str) -> list[Path]:
    dist_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    created: list[Path] = []
    with tempfile.TemporaryDirectory() as temp_dir:
        staged_root = Path(temp_dir) / SKILL_NAME
        copy_tree(staged_root)
        if package_format in {'all', 'zip'}:
            zip_path = dist_dir / f'{SKILL_NAME}-{timestamp}.zip'
            with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                for path in staged_root.rglob('*'):
                    archive.write(path, path.relative_to(staged_root.parent))
            created.append(zip_path)
        if package_format in {'all', 'tar.gz'}:
            tar_path = dist_dir / f'{SKILL_NAME}-{timestamp}.tar.gz'
            with tarfile.open(tar_path, 'w:gz') as archive:
                archive.add(staged_root, arcname=SKILL_NAME)
            created.append(tar_path)
    return created


def main() -> int:
    parser = argparse.ArgumentParser(description='Build a clean distribution package for the skill.')
    parser.add_argument('--format', choices=('all', 'zip', 'tar.gz', 'dir'), default='all')
    parser.add_argument('--dist-dir', default='dist', help='Directory for archive outputs.')
    parser.add_argument('--output', help='Required when --format dir; destination directory for a clean copy.')
    args = parser.parse_args()

    if args.format == 'dir':
        if not args.output:
            raise SystemExit('--output is required when --format dir is used.')
        destination = Path(args.output).expanduser().resolve()
        copy_tree(destination)
        print(destination)
        return 0

    created = build_archives(Path(args.dist_dir).expanduser().resolve(), args.format)
    for path in created:
        print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
