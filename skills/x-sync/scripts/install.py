#!/usr/bin/env python3
"""Install the canonical x-sync skill for Codex, Claude Code, or both."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
import tempfile
from typing import Iterable


SKILL_NAME = "x-sync"
MARKER_NAME = ".x-sync-install.json"
MARKER_FORMAT = 2
MAX_MARKER_BYTES = 1024 * 1024
SOURCE_DIR = Path(__file__).resolve().parent.parent

HOST_PATHS = {
    "codex": Path(".agents") / "skills" / SKILL_NAME,
    "claude": Path(".claude") / "skills" / SKILL_NAME,
}


class InstallError(RuntimeError):
    """Raised when an installation cannot be completed safely."""


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Install x-sync into Codex and/or Claude Code skill directories."
    )
    parser.add_argument(
        "--host",
        choices=("codex", "claude", "all"),
        default="all",
        help="host to install for (default: all)",
    )
    parser.add_argument(
        "--scope",
        choices=("user", "project"),
        default="user",
        help="installation scope (default: user)",
    )
    parser.add_argument(
        "--project",
        type=Path,
        help="project root for project scope (default: current directory)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and print planned changes without writing files",
    )
    args = parser.parse_args(argv)
    if args.scope == "user" and args.project is not None:
        parser.error("--project can only be used with --scope project")
    return args


def selected_hosts(host: str) -> tuple[str, ...]:
    return ("codex", "claude") if host == "all" else (host,)


def install_root(scope: str, project: Path | None) -> Path:
    root = Path.home() if scope == "user" else (project or Path.cwd())
    expanded = root.expanduser()
    if scope == "user":
        return expanded.resolve()
    # Keep the lexical project path so symlinks at or below the project root
    # remain observable during validation.
    return Path(os.path.abspath(os.fspath(expanded)))


def destination(host: str, root: Path) -> Path:
    return root / HOST_PATHS[host]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ignored_source_name(name: str) -> bool:
    return (
        name in {MARKER_NAME, "__pycache__", ".DS_Store"}
        or name.endswith((".pyc", ".pyo"))
    )


def source_manifest(root: Path | None = None) -> dict[str, str]:
    source = SOURCE_DIR if root is None else root
    files: dict[str, str] = {}
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if any(ignored_source_name(part) for part in relative.parts):
            continue
        if path.is_file():
            files[relative.as_posix()] = sha256_file(path)
    return files


def manifest_digest(files: dict[str, str]) -> str:
    encoded = json.dumps(
        files, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def marker_payload(
    host: str, scope: str, files: dict[str, str] | None = None
) -> dict[str, object]:
    recorded_files = source_manifest() if files is None else dict(files)
    return {
        "format": MARKER_FORMAT,
        "managed_by": "x-sync-installer",
        "skill": SKILL_NAME,
        "host": host,
        "scope": scope,
        "source": str(SOURCE_DIR),
        "content": {
            "algorithm": "sha256",
            "digest": manifest_digest(recorded_files),
            "files": recorded_files,
        },
    }


def valid_recorded_files(value: object) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    files: dict[str, str] = {}
    for name, digest in value.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            return None
        relative = PurePosixPath(name)
        if (
            relative.is_absolute()
            or not relative.parts
            or any(part in {"", ".", ".."} for part in relative.parts)
            or relative.as_posix() != name
            or "\\" in name
            or "\0" in name
            or name == MARKER_NAME
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            return None
        files[name] = digest
    # Every install produced by this installer contains both its entrypoint and
    # the installer itself. Requiring them prevents a marker-only directory
    # from authorizing replacement.
    if not {"SKILL.md", "scripts/install.py"}.issubset(files):
        return None
    return files


def target_matches_manifest(target: Path, files: dict[str, str]) -> bool:
    try:
        resolved_target = target.resolve()
    except (OSError, RuntimeError):
        return False
    for name, expected_digest in files.items():
        relative = PurePosixPath(name)
        candidate = target.joinpath(*relative.parts)
        cursor = target
        for part in relative.parts:
            cursor /= part
            if cursor.is_symlink():
                return False
        if not candidate.is_file():
            return False
        try:
            candidate.resolve().relative_to(resolved_target)
        except (OSError, RuntimeError, ValueError):
            return False
        try:
            if sha256_file(candidate) != expected_digest:
                return False
        except OSError:
            return False
    return True


def is_managed(target: Path, host: str, scope: str) -> bool:
    marker = target / MARKER_NAME
    try:
        if marker.is_symlink() or marker.stat().st_size > MAX_MARKER_BYTES:
            return False
        data = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False
    if not (
        isinstance(data, dict)
        and data.get("managed_by") == "x-sync-installer"
        and data.get("skill") == SKILL_NAME
        and data.get("format") == MARKER_FORMAT
        and data.get("host") == host
        and data.get("scope") == scope
    ):
        return False
    content = data.get("content")
    if not isinstance(content, dict) or content.get("algorithm") != "sha256":
        return False
    files = valid_recorded_files(content.get("files"))
    if files is None or content.get("digest") != manifest_digest(files):
        return False
    return target_matches_manifest(target, files)


def validate_project_path(project_root: Path, target: Path) -> None:
    try:
        relative = target.relative_to(project_root)
    except ValueError as exc:
        raise InstallError(f"project target is outside the project root: {target}") from exc

    cursor = project_root
    paths_to_check = [cursor]
    for part in relative.parts[:-1]:
        cursor /= part
        paths_to_check.append(cursor)
    for path in paths_to_check:
        if path.is_symlink():
            raise InstallError(f"refusing project path with symlink parent: {path}")

    resolved_project = project_root.resolve(strict=False)
    resolved_target = target.resolve(strict=False)
    try:
        resolved_target.relative_to(resolved_project)
    except ValueError as exc:
        raise InstallError(
            f"resolved project target is outside the project root: {target}"
        ) from exc


def validate_target(
    target: Path, host: str, scope: str, project_root: Path | None = None
) -> None:
    if scope == "project":
        if project_root is None:
            raise InstallError("project root is required for project scope")
        validate_project_path(project_root, target)
    if target.is_symlink():
        raise InstallError(f"refusing to replace symlink: {target}")
    if target.exists() and (
        not target.is_dir() or not is_managed(target, host, scope)
    ):
        raise InstallError(f"refusing to overwrite unmanaged path: {target}")

    source = SOURCE_DIR.resolve()
    resolved_target = target.resolve(strict=False)
    if resolved_target == source or source in resolved_target.parents:
        raise InstallError(f"refusing to install inside the source skill: {target}")


def validate_source() -> None:
    if not (SOURCE_DIR / "SKILL.md").is_file():
        raise InstallError(f"canonical skill is missing SKILL.md: {SOURCE_DIR}")
    for path in SOURCE_DIR.rglob("*"):
        if path.is_symlink():
            raise InstallError(f"source contains an unsafe symlink: {path}")


def ignore_source_entries(directory: str, names: list[str]) -> set[str]:
    del directory
    return {name for name in names if ignored_source_name(name)}


def replace_from_source(target: Path, host: str, scope: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(tempfile.mkdtemp(prefix=f".{SKILL_NAME}-", dir=target.parent))
    staged = staging_root / SKILL_NAME
    backup = staging_root / "previous"
    moved_previous = False
    try:
        shutil.copytree(
            SOURCE_DIR,
            staged,
            symlinks=False,
            ignore=ignore_source_entries,
            copy_function=shutil.copy2,
        )
        # Hash the staged copy rather than the source so the marker always
        # describes exactly what is about to be installed.
        payload = marker_payload(host, scope, source_manifest(staged))
        (staged / MARKER_NAME).write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if target.exists():
            os.replace(target, backup)
            moved_previous = True
        try:
            os.replace(staged, target)
        except BaseException:
            if moved_previous and not target.exists():
                os.replace(backup, target)
            raise
        if moved_previous:
            shutil.rmtree(backup)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def run(args: argparse.Namespace) -> int:
    validate_source()
    root = install_root(args.scope, args.project)
    plans = [
        (host, destination(host, root))
        for host in selected_hosts(args.host)
    ]
    for host, target in plans:
        validate_target(
            target, host, args.scope, root if args.scope == "project" else None
        )

    action = "would install" if args.dry_run else "installed"
    for host, target in plans:
        if not args.dry_run:
            # Repeat path validation immediately before mutation to narrow the
            # window in which a parent directory could be replaced.
            validate_target(
                target, host, args.scope, root if args.scope == "project" else None
            )
            replace_from_source(target, host, args.scope)
        print(f"{action} {host} skill at {target}")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except InstallError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
