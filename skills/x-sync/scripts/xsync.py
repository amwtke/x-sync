#!/usr/bin/env python3
"""Deterministic, offline runtime for the x-sync skill.

The host agent creates question banks and reviews free-text answers.  This
runtime deliberately contains no model client and never makes network calls.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import errno
import getpass
import hashlib
import http.server
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import stat as statlib
import subprocess
import sys
import tempfile
import time
import unicodedata
import urllib.parse
import uuid
import webbrowser


SCHEMA_VERSION = 1
LEARNER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}\Z")
GIT_OID_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
SECRET_NAMES = {
    ".dockercfg", ".env", ".envrc", ".netrc", ".npmrc", ".pypirc",
    "auth.json", "credentials", "credentials.json", "id_ed25519", "id_rsa",
    "secret.json", "secrets.json", "terraform.tfstate",
}
SECRET_CONFIG_STEMS = {"credential", "credentials", "secret", "secrets", "token", "tokens"}
SECRET_CONFIG_SUFFIXES = {
    ".conf", ".ini", ".json", ".properties", ".text", ".tfvars", ".toml",
    ".txt", ".xml", ".yaml", ".yml",
}
SENSITIVE_PARTS = {
    ".aws", ".azure", ".docker", ".gcloud", ".git", ".gnupg", ".kube",
    ".ssh", ".x-sync", "credentials", "secrets",
}
SENSITIVE_SUFFIXES = {
    ".jks", ".kdbx", ".key", ".keystore", ".mobileprovision", ".p12",
    ".pem", ".pfx", ".tfstate",
}
GENERATED_DIRECTORY_PARTS = {
    ".cache", ".gradle", ".m2", ".mypy_cache", ".next", ".nuxt",
    ".pnpm-store", ".pytest_cache", ".ruff_cache", ".terraform", ".tox",
    ".venv", "__pycache__", "bower_components", "build", "coverage",
    "deriveddata", "dist", "node_modules", "out", "pods", "target",
    "vendor", "venv",
}
MAX_FOCUSED_EVIDENCE_BYTES = 1_000_000
MAX_COMMIT_EVIDENCE_BYTES = 5_000_000
MAX_SCAN_INDEX_BYTES = 64 * 1024 * 1024
MAX_SCAN_FILES = 200_000
MAX_SCAN_TOTAL_BYTES = 256 * 1024 * 1024
MAX_SCAN_MANIFEST_BYTES = 64 * 1024 * 1024
SCAN_POLICY_VERSION = "safe-engineering-tree-v1"
DEFAULT_STYLE = "socratic"
DEFAULT_CHANNEL = "web"
DEFAULT_FOCUS = "mixed"
DEFAULT_QUESTION_COUNT = 5


class XSyncError(Exception):
    """An expected, user-facing error."""


class EvidenceStaleError(XSyncError):
    """Raised when evidence no longer matches the bank snapshot."""

    def __init__(self, evidence_ids: list[str]):
        self.evidence_ids = sorted(set(evidence_ids))
        super().__init__(
            "题库 evidence 已过期，需重新生成/校验: " + ", ".join(self.evidence_ids)
        )


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")) + "\n").encode("utf-8")


def idempotency_key(namespace: str, *parts: object) -> str:
    """Encode an unambiguous, opaque key for one logical state transition."""
    digest = hashlib.sha256(canonical_bytes([namespace, *parts])).hexdigest()
    return f"{namespace}.{digest}"


def load_json(path: Path) -> object:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError as exc:
        raise XSyncError(f"文件不存在: {path}") from exc
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise XSyncError(f"JSON 格式错误 {path}: {exc}") from exc


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        try:
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


def atomic_json(path: Path, value: object) -> None:
    atomic_write(path, json.dumps(value, ensure_ascii=False, indent=2,
                                  sort_keys=True).encode("utf-8") + b"\n")


def validate_component(value: str, kind: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or value in {".", ".."} or not pattern.fullmatch(value):
        raise XSyncError(f"非法{kind}: {value!r}")
    return value


def validate_learner(value: str) -> str:
    return validate_component(value, " learner", LEARNER_RE)


def default_learner() -> str:
    """Resolve a stable local profile without exposing an email address."""
    try:
        raw = getpass.getuser().strip()
    except (KeyError, OSError):
        raw = ""
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip("._-")
    normalized = normalized[:64].rstrip("._-")
    if normalized and LEARNER_RE.fullmatch(normalized):
        return normalized
    if raw:
        return "user-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    return "local-user"


def safe_child(root: Path, *parts: str) -> Path:
    root = root.resolve()
    candidate = root.joinpath(*parts)
    # Existing parents may not be symlinks. This also prevents a pre-created
    # learner directory from redirecting writes outside .x-sync.
    cursor = candidate
    while cursor != root and not cursor.exists():
        cursor = cursor.parent
    if cursor.exists() and cursor.is_symlink():
        raise XSyncError(f"拒绝符号链接路径: {cursor}")
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise XSyncError("路径越界") from exc
    return resolved


def git(repo: Path, *args: str, check: bool = True) -> str:
    try:
        result = subprocess.run(["git", "-C", str(repo), *args], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as exc:
        if check:
            raise XSyncError("未找到 git；请先安装 Git") from exc
        return ""
    if check and result.returncode:
        raise XSyncError(result.stderr.strip() or "git 命令失败")
    return result.stdout.strip() if result.returncode == 0 else ""


def find_repo(value: str) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_dir():
        raise XSyncError(f"仓库目录不存在: {path}")
    top = git(path, "rev-parse", "--show-toplevel", check=False)
    return Path(top).resolve() if top else path


def repo_id(repo: Path) -> str:
    remote = git(repo, "remote", "get-url", "origin", check=False)
    identity = remote or str(repo.resolve())
    return hashlib.sha256(identity.encode()).hexdigest()[:16]


def working_tree_state(repo: Path) -> dict:
    if git(repo, "rev-parse", "--is-inside-work-tree", check=False) != "true":
        return {"dirty": False}
    status = git_scan_bytes(
        repo, "status", "--porcelain=v1", "-z", "--untracked-files=all"
    )
    if not status:
        return {"dirty": False}
    head = git(repo, "rev-parse", "HEAD", check=False)
    diff = (git_scan_bytes(repo, "diff", "--binary", "--no-ext-diff", "HEAD")
            if head else b"")
    payload = b"x-sync-working-tree-v1\0" + status + b"\0--DIFF--\0" + diff
    return {"dirty": True, "diff_hash": f"sha256:{hashlib.sha256(payload).hexdigest()}"}


def is_sensitive_path(path: Path) -> bool:
    """Return true for paths x-sync must never inspect as evidence."""
    lowered = {part.lower() for part in path.parts}
    name = path.name.lower()
    stem_tokens = set(re.split(r"[._-]+", path.stem.lower()))
    secret_config = (path.suffix.lower() in SECRET_CONFIG_SUFFIXES
                     and bool(stem_tokens & SECRET_CONFIG_STEMS))
    return (bool(lowered & SENSITIVE_PARTS) or bool(lowered & SECRET_NAMES)
            or name.startswith(".env.") or secret_config
            or ".tfstate" in name or ".tfvars" in name
            or path.suffix.lower() in SENSITIVE_SUFFIXES)


def process_alive(pid: object) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        return True
    return True


def git_bytes_limited(repo: Path, args: list[str], limit: int) -> bytes:
    """Capture at most limit bytes from a stable, non-interactive Git command."""
    environment = os.environ.copy()
    environment.update({"LC_ALL": "C", "LANG": "C", "GIT_NO_REPLACE_OBJECTS": "1"})
    with tempfile.TemporaryFile() as error_stream:
        try:
            process = subprocess.Popen(
                ["git", "-C", str(repo), *args], stdout=subprocess.PIPE,
                stderr=error_stream, env=environment,
            )
        except FileNotFoundError as exc:
            raise XSyncError("未找到 git；请先安装 Git") from exc
        assert process.stdout is not None
        data = process.stdout.read(limit + 1)
        if len(data) > limit:
            process.kill()
            process.wait()
            process.stdout.close()
            raise XSyncError(
                f"commit evidence 超过 {MAX_COMMIT_EVIDENCE_BYTES} bytes；请用 --path 缩小范围"
            )
        returncode = process.wait()
        process.stdout.close()
        error_stream.seek(0)
        stderr = error_stream.read(64 * 1024)
    if returncode:
        message = stderr.decode("utf-8", errors="replace").strip()
        raise XSyncError(message or "git 证据命令失败")
    return data


def commit_snapshot_bytes(repo: Path, commit: str, relative: Path | None) -> bytes:
    """Bind a commit object and its raw object-ID delta without display config."""
    commit_object = git_bytes_limited(
        repo, ["cat-file", "commit", commit], MAX_COMMIT_EVIDENCE_BYTES
    )
    remaining = MAX_COMMIT_EVIDENCE_BYTES - len(commit_object)
    diff_args = ["--literal-pathspecs", "diff-tree", "--raw", "-z", "--full-index",
                 "--root", "--no-renames", "-r", "-m", "--no-commit-id", commit]
    if relative is not None:
        diff_args.extend(["--", relative.as_posix()])
    raw_delta = git_bytes_limited(repo, diff_args, remaining)
    if relative is not None and not raw_delta:
        raise XSyncError(f"commit {commit[:12]} 未触及路径: {relative.as_posix()}")
    scope = relative.as_posix().encode("utf-8") if relative is not None else b""
    return b"".join([
        b"x-sync-commit-evidence-v1\0",
        len(scope).to_bytes(4, "big"), scope,
        len(commit_object).to_bytes(8, "big"), commit_object,
        len(raw_delta).to_bytes(8, "big"), raw_delta,
    ])


def focused_file_bytes(path: Path, start: int | None = None,
                       end: int | None = None) -> bytes:
    """Read one bounded UTF-8 text artifact, optionally by inclusive lines."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise XSyncError(f"无法读取 evidence 文件: {path}") from exc
    try:
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not statlib.S_ISREG(metadata.st_mode):
                raise XSyncError(f"evidence 必须是普通文件: {path}")
            raw = stream.read(MAX_FOCUSED_EVIDENCE_BYTES + 1)
    except OSError as exc:
        raise XSyncError(f"无法读取 evidence 文件: {path}") from exc
    if len(raw) > MAX_FOCUSED_EVIDENCE_BYTES:
        raise XSyncError(
            f"evidence 文件超过 {MAX_FOCUSED_EVIDENCE_BYTES} bytes；请选择更小的文本证据"
        )
    if b"\0" in raw:
        raise XSyncError("evidence 文件包含二进制数据")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise XSyncError("evidence 文件必须是 UTF-8 文本") from exc
    if start is None and end is None:
        return raw
    if start is None or end is None or start < 1 or end < start:
        raise XSyncError("evidence 行号必须是合法的一起始包含范围")
    lines = text.splitlines(keepends=True)
    if start > len(lines) or end > len(lines):
        raise XSyncError(f"evidence 行号超出文件范围（共 {len(lines)} 行）")
    return "".join(lines[start - 1:end]).encode("utf-8")


def git_scan_bytes(repo: Path, *args: str) -> bytes:
    """Return bounded raw Git output for repository discovery."""
    environment = os.environ.copy()
    environment.update({"LC_ALL": "C", "LANG": "C", "GIT_OPTIONAL_LOCKS": "0"})
    with tempfile.TemporaryFile() as error_stream:
        try:
            process = subprocess.Popen(
                ["git", "-C", str(repo), *args], stdout=subprocess.PIPE,
                stderr=error_stream, env=environment,
            )
        except FileNotFoundError as exc:
            raise XSyncError("首次全工程扫描需要 Git") from exc
        assert process.stdout is not None
        raw = process.stdout.read(MAX_SCAN_INDEX_BYTES + 1)
        if len(raw) > MAX_SCAN_INDEX_BYTES:
            process.kill()
            process.wait()
            process.stdout.close()
            raise XSyncError("Git 扫描输出过大；请先排除生成目录或缩小仓库")
        returncode = process.wait()
        process.stdout.close()
        error_stream.seek(0)
        stderr = error_stream.read(64 * 1024)
    if returncode:
        message = stderr.decode("utf-8", errors="replace").strip()
        raise XSyncError(message or "Git 工程文件枚举失败")
    return raw


def git_nul_paths(repo: Path, *args: str) -> list[str]:
    """Return raw NUL-delimited Git paths without quote/display ambiguity."""
    raw = git_scan_bytes(repo, *args)
    fields = [field for field in raw.split(b"\0") if field]
    if len(fields) > MAX_SCAN_FILES:
        raise XSyncError(
            f"工程候选文件超过 {MAX_SCAN_FILES} 个；请先排除依赖或生成目录"
        )
    paths = []
    for field in fields:
        try:
            paths.append(field.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise XSyncError("工程包含非 UTF-8 文件名，无法生成可移植扫描清单") from exc
    return paths


def repository_state_token(repo: Path) -> tuple[str, bytes, str]:
    head = git(repo, "rev-parse", "HEAD", check=False)
    raw_status = git_scan_bytes(
        repo, "status", "--porcelain=v1", "-z", "--untracked-files=all"
    )
    raw_index = git_scan_bytes(repo, "ls-files", "--stage", "-z")
    digest = hashlib.sha256(
        b"x-sync-repository-state-v1\0" + raw_status + b"\0--INDEX--\0" + raw_index
    ).hexdigest()
    return head, raw_status, f"sha256:{digest}"


def git_index_gitlinks(repo: Path) -> set[str]:
    gitlinks = set()
    for entry in git_scan_bytes(repo, "ls-files", "--stage", "-z").split(b"\0"):
        if not entry:
            continue
        metadata, separator, raw_path = entry.partition(b"\t")
        if separator and metadata.split(b" ", 1)[0] == b"160000":
            try:
                gitlinks.add(raw_path.decode("utf-8"))
            except UnicodeDecodeError as exc:
                raise XSyncError("工程包含非 UTF-8 submodule 路径") from exc
    return gitlinks


def is_generated_path(path: Path) -> bool:
    """Return true when a file lives below a dependency/build/cache directory."""
    return any(part.lower() in GENERATED_DIRECTORY_PARTS for part in path.parts[:-1])


def scan_file_kind(path: Path) -> str:
    """Classify an inventory entry without inferring its business meaning."""
    parts = {part.lower() for part in path.parts}
    name = path.name.lower()
    suffix = path.suffix.lower()
    if parts & {"test", "tests", "spec", "specs", "__tests__"} or name.startswith("test_"):
        return "test"
    if parts & {"docs", "doc", "adr", "adrs", "stories"} or suffix in {
        ".md", ".rst", ".adoc", ".txt",
    }:
        return "documentation"
    if parts & {"migrations", "migration"}:
        return "migration"
    if parts & {"docker", "k8s", "kubernetes", "helm", "terraform", "deploy"} or name in {
        "dockerfile", "docker-compose.yml", "docker-compose.yaml",
    }:
        return "infrastructure"
    if name in {
        "build.gradle", "cargo.toml", "go.mod", "go.sum", "makefile",
        "package-lock.json", "package.json", "pom.xml", "pyproject.toml",
        "requirements.txt",
    }:
        return "build"
    if suffix in {".conf", ".ini", ".json", ".properties", ".toml", ".xml", ".yaml", ".yml"}:
        return "configuration"
    return "source"


def scan_relative_file(repo: Path, relative: Path) -> tuple[dict | None, str | None]:
    """Read one repository file through no-follow directory descriptors."""
    if not relative.parts or relative.is_absolute() or ".." in relative.parts:
        raise XSyncError(f"非法工程相对路径: {relative}")
    directory_fds: list[int] = []
    file_fd: int | None = None
    try:
        root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        root_fd = os.open(repo, root_flags)
        directory_fds.append(root_fd)
        current_fd = root_fd
        for part in relative.parts[:-1]:
            flags = (os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                     | getattr(os, "O_NOFOLLOW", 0))
            if os.open in os.supports_dir_fd:
                next_fd = os.open(part, flags, dir_fd=current_fd)
            else:  # pragma: no cover - fallback for platforms without dir_fd
                lexical = repo.joinpath(*relative.parts[:len(directory_fds)])
                if lexical.is_symlink():
                    return None, "symlink"
                next_fd = os.open(lexical, flags)
            metadata = os.fstat(next_fd)
            if not statlib.S_ISDIR(metadata.st_mode):
                os.close(next_fd)
                return None, "symlink"
            directory_fds.append(next_fd)
            current_fd = next_fd
        file_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        if os.open in os.supports_dir_fd:
            file_fd = os.open(relative.parts[-1], file_flags, dir_fd=current_fd)
        else:  # pragma: no cover - fallback for platforms without dir_fd
            lexical = repo.joinpath(*relative.parts)
            resolved = lexical.resolve(strict=False)
            try:
                resolved.relative_to(repo.resolve())
            except ValueError:
                return None, "symlink"
            if lexical.is_symlink():
                return None, "symlink"
            file_fd = os.open(lexical, file_flags)
        metadata = os.fstat(file_fd)
        if not statlib.S_ISREG(metadata.st_mode):
            return None, "special"
        if metadata.st_size > MAX_FOCUSED_EVIDENCE_BYTES:
            return None, "oversized"
        with os.fdopen(file_fd, "rb") as stream:
            file_fd = None
            raw = stream.read(MAX_FOCUSED_EVIDENCE_BYTES + 1)
            after = os.fstat(stream.fileno())
        before_token = (metadata.st_dev, metadata.st_ino, metadata.st_size,
                        metadata.st_mtime_ns, metadata.st_ctime_ns, metadata.st_mode)
        after_token = (after.st_dev, after.st_ino, after.st_size,
                       after.st_mtime_ns, after.st_ctime_ns, after.st_mode)
        if before_token != after_token:
            return None, "changed_during_scan"
        if len(raw) > MAX_FOCUSED_EVIDENCE_BYTES:
            return None, "oversized"
        if b"\0" in raw:
            return None, "binary_or_non_utf8"
        try:
            raw.decode("utf-8")
        except UnicodeDecodeError:
            return None, "binary_or_non_utf8"
        return {
            "path": relative.as_posix(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
            "lines": raw.count(b"\n") + int(bool(raw) and not raw.endswith(b"\n")),
            "executable": bool(metadata.st_mode & 0o111),
            "kind": scan_file_kind(relative),
            "lfs_pointer": raw.startswith(b"version https://git-lfs.github.com/spec/v1\n"),
        }, None
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            return None, "symlink"
        if exc.errno == errno.ENOENT:
            return None, "changed_during_scan"
        raise XSyncError(f"无法完整扫描工程文件 {relative}: {exc}") from exc
    finally:
        if file_fd is not None:
            with contextlib.suppress(OSError):
                os.close(file_fd)
        for descriptor in reversed(directory_fds):
            with contextlib.suppress(OSError):
                os.close(descriptor)


def scan_relative_metadata_token(repo: Path, relative: Path) -> tuple[object, ...]:
    """Read no-follow metadata used to detect same-status content races."""
    directory_fds: list[int] = []
    try:
        root_fd = os.open(repo, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        directory_fds.append(root_fd)
        current_fd = root_fd
        for index, part in enumerate(relative.parts[:-1], start=1):
            flags = (os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                     | getattr(os, "O_NOFOLLOW", 0))
            if os.open in os.supports_dir_fd:
                next_fd = os.open(part, flags, dir_fd=current_fd)
            else:  # pragma: no cover - fallback for platforms without dir_fd
                lexical = repo.joinpath(*relative.parts[:index])
                if lexical.is_symlink():
                    return ("unsafe",)
                next_fd = os.open(lexical, flags)
            directory_fds.append(next_fd)
            current_fd = next_fd
        if os.stat in os.supports_dir_fd:
            metadata = os.stat(relative.parts[-1], dir_fd=current_fd, follow_symlinks=False)
        else:  # pragma: no cover - fallback for platforms without dir_fd
            metadata = os.lstat(repo.joinpath(*relative.parts))
        return (metadata.st_dev, metadata.st_ino, metadata.st_size,
                metadata.st_mtime_ns, metadata.st_ctime_ns, metadata.st_mode)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return ("missing",)
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            return ("unsafe",)
        raise XSyncError(f"无法核对工程文件元数据 {relative}: {exc}") from exc
    finally:
        for descriptor in reversed(directory_fds):
            with contextlib.suppress(OSError):
                os.close(descriptor)


def normalize_scan_paths(paths: list[str] | None) -> list[str]:
    normalized = []
    for raw in paths or []:
        value = raw.rstrip("/") or "."
        path = Path(value)
        if (path.is_absolute() or ".." in path.parts or is_sensitive_path(path)
                or is_generated_path(path)):
            raise XSyncError("--paths 必须是安全、无 .. 的仓库相对路径")
        if value != ".":
            normalized.append(path.as_posix())
    return sorted(set(normalized))


def path_in_scan_scope(path: str, scopes: list[str]) -> bool:
    return not scopes or any(path == scope or path.startswith(scope + "/") for scope in scopes)


def verify_scan_candidates(repo: Path, observations: dict[str, dict | str]) -> None:
    """Recheck every opened candidate before declaring the scan complete."""
    keys = {"path", "sha256", "bytes", "lines", "executable", "kind", "lfs_pointer"}
    for path, expected in observations.items():
        actual, reason = scan_relative_file(repo, Path(path))
        if isinstance(expected, str):
            consistent = reason == expected
        else:
            consistent = reason is None and actual == {key: expected[key] for key in keys}
        if not consistent:
            raise XSyncError("工程在扫描期间发生变化；请重试 x-sync")


def verify_scan_metadata(repo: Path, observations: dict[str, tuple[object, ...]]) -> None:
    for path, expected in observations.items():
        if scan_relative_metadata_token(repo, Path(path)) != expected:
            raise XSyncError("工程在扫描期间发生变化；请重试 x-sync")


def repository_scan_manifest(repo: Path, paths: list[str] | None = None) -> dict:
    """Build a complete manifest of the safe, project-owned engineering tree."""
    repo = repo.resolve()
    if git(repo, "rev-parse", "--is-inside-work-tree", check=False) != "true":
        raise XSyncError("首次全工程扫描需要 Git 仓库；当前目录不是 Git worktree")
    if git(repo, "config", "--bool", "core.sparseCheckout", check=False) == "true":
        raise XSyncError("完整工程扫描暂不支持 sparse checkout；请使用完整 worktree")
    baseline_commit, start_status, state_token = repository_state_token(repo)
    if not GIT_OID_RE.fullmatch(baseline_commit):
        raise XSyncError("首次全工程扫描需要至少一个 Git commit")
    scopes = normalize_scan_paths(paths)
    tracked = set(git_nul_paths(repo, "ls-files", "-z", "--cached"))
    untracked = set(git_nul_paths(
        repo, "ls-files", "-z", "--others", "--exclude-standard"
    ))
    deleted = set(git_nul_paths(repo, "ls-files", "-z", "--deleted"))
    gitlinks = git_index_gitlinks(repo)
    candidates = sorted(tracked | untracked)
    if len(candidates) > MAX_SCAN_FILES:
        raise XSyncError(
            f"工程候选文件超过 {MAX_SCAN_FILES} 个；请先排除依赖或生成目录"
        )
    exclusions = {
        "sensitive": 0, "generated_or_vendor": 0, "symlink": 0,
        "oversized": 0, "binary_or_non_utf8": 0, "special": 0,
        "deleted": 0, "gitlink": 0,
    }
    records = []
    observations: dict[str, dict | str] = {}
    metadata_observations: dict[str, tuple[object, ...]] = {}
    scoped_candidates = 0
    included_bytes = 0
    for raw_path in candidates:
        if not path_in_scan_scope(raw_path, scopes):
            continue
        scoped_candidates += 1
        relative = Path(raw_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise XSyncError(f"Git 返回非法工程路径: {raw_path!r}")
        if is_sensitive_path(relative):
            exclusions["sensitive"] += 1
            continue
        if is_generated_path(relative):
            exclusions["generated_or_vendor"] += 1
            continue
        if raw_path in deleted:
            exclusions["deleted"] += 1
            continue
        if raw_path in gitlinks:
            exclusions["gitlink"] += 1
            continue
        metadata_observations[raw_path] = scan_relative_metadata_token(repo, relative)
        record, reason = scan_relative_file(repo, relative)
        if reason == "changed_during_scan":
            raise XSyncError("工程在扫描期间发生变化；请重试 x-sync")
        if reason:
            exclusions[reason] += 1
            observations[raw_path] = reason
            continue
        assert record is not None
        observations[raw_path] = dict(record)
        record["source"] = "tracked" if raw_path in tracked else "untracked"
        included_bytes += record["bytes"]
        if included_bytes > MAX_SCAN_TOTAL_BYTES:
            raise XSyncError(
                f"安全文本总量超过 {MAX_SCAN_TOTAL_BYTES} bytes；请拆分仓库或排除生成内容"
            )
        records.append(record)
    records.sort(key=lambda item: item["path"])
    verify_scan_candidates(repo, observations)
    verify_scan_metadata(repo, metadata_observations)
    end_head, end_status, end_state_token = repository_state_token(repo)
    if (end_head, end_status) != (baseline_commit, start_status):
        raise XSyncError("工程在扫描期间发生变化；请重试 x-sync")
    if end_state_token != state_token:
        raise XSyncError("工程状态指纹在扫描期间发生变化；请重试 x-sync")
    kinds: dict[str, int] = {}
    for record in records:
        kinds[record["kind"]] = kinds.get(record["kind"], 0) + 1
    try:
        log_text = git_scan_bytes(
            repo, "log", "-n", "100", "--format=%H%x09%s"
        ).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise XSyncError("Git commit summary 必须是 UTF-8") from exc
    log_lines = log_text.splitlines()
    recent_commits = [{
        "sha": line.split("\t", 1)[0],
        "subject": line.split("\t", 1)[1] if "\t" in line else "",
    } for line in log_lines]
    scope = {"type": "full" if not scopes else "paths", "paths": scopes}
    fingerprint_payload = {
        "policy_version": SCAN_POLICY_VERSION,
        "repo_id": repo_id(repo),
        "baseline_commit": baseline_commit,
        "state_token": state_token,
        "scope": scope,
        "files": records,
    }
    fingerprint = hashlib.sha256(canonical_bytes(fingerprint_payload)).hexdigest()
    scan_id = f"scan.{fingerprint}"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "repository_scan",
        "policy_version": SCAN_POLICY_VERSION,
        "scan_id": scan_id,
        "snapshot_id": scan_id,
        "repo_id": repo_id(repo),
        "baseline_commit": baseline_commit,
        "state_token": state_token,
        "working_tree": {"dirty": bool(start_status), "status_hash": state_token},
        "scope": scope,
        "complete": True,
        "created_at": utc_now(),
        "fingerprint": f"sha256:{fingerprint}",
        "summary": {
            "candidate_files": scoped_candidates,
            "included_files": len(records),
            "included_bytes": included_bytes,
            "top_level_entries": sorted({item["path"].split("/", 1)[0] for item in records}),
            "kinds": kinds,
            "excluded": exclusions,
            "gitignored": "out_of_scope",
            "recent_commits": len(recent_commits),
        },
        "files": records,
        "commits": recent_commits,
    }
    if len(canonical_bytes(manifest)) > MAX_SCAN_MANIFEST_BYTES:
        raise XSyncError("repository scan manifest 过大；请拆分仓库")
    manifest["integrity_hash"] = "sha256:" + hashlib.sha256(
        canonical_bytes(manifest)
    ).hexdigest()
    return manifest


def validate_repository_scan(manifest: object, require_current_policy: bool = True) -> dict:
    if not isinstance(manifest, dict):
        raise XSyncError("repository scan 格式错误")
    required = {
        "schema_version", "record_type", "policy_version", "scan_id", "repo_id",
        "snapshot_id", "baseline_commit", "state_token", "working_tree", "scope",
        "complete", "created_at", "fingerprint", "summary", "files", "commits",
        "integrity_hash",
    }
    if not required.issubset(manifest):
        raise XSyncError("repository scan 缺少必填字段")
    if (manifest.get("schema_version") != SCHEMA_VERSION
            or manifest.get("record_type") != "repository_scan"
            or not isinstance(manifest.get("policy_version"), str)
            or (require_current_policy and manifest.get("policy_version") != SCAN_POLICY_VERSION)
            or manifest.get("complete") is not True):
        raise XSyncError("repository scan schema/policy 非法")
    files = manifest.get("files")
    scope = manifest.get("scope")
    if (not isinstance(files, list) or not isinstance(scope, dict)
            or not re.fullmatch(r"[0-9a-f]{16}", str(manifest.get("repo_id", "")))
            or not GIT_OID_RE.fullmatch(str(manifest.get("baseline_commit", "")))
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(manifest.get("state_token", "")))):
        raise XSyncError("repository scan files/scope 非法")
    if scope.get("type") not in {"full", "paths"} or not isinstance(scope.get("paths"), list):
        raise XSyncError("repository scan scope 非法")
    seen_paths = []
    allowed_kinds = {"build", "configuration", "documentation", "infrastructure",
                     "migration", "source", "test"}
    for index, item in enumerate(files):
        if not isinstance(item, dict):
            raise XSyncError(f"repository scan files[{index}] 非法")
        path_value = item.get("path")
        relative = Path(path_value) if isinstance(path_value, str) else Path("..")
        if (not isinstance(path_value, str) or not path_value
                or relative.is_absolute() or ".." in relative.parts
                or is_sensitive_path(relative) or is_generated_path(relative)
                or not re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256", "")))
                or not isinstance(item.get("bytes"), int) or isinstance(item.get("bytes"), bool)
                or item["bytes"] < 0
                or not isinstance(item.get("lines"), int) or isinstance(item.get("lines"), bool)
                or item["lines"] < 0
                or not isinstance(item.get("executable"), bool)
                or not isinstance(item.get("lfs_pointer"), bool)
                or item.get("kind") not in allowed_kinds
                or item.get("source") not in {"tracked", "untracked"}):
            raise XSyncError(f"repository scan files[{index}] 字段非法")
        seen_paths.append(path_value)
    if seen_paths != sorted(set(seen_paths)):
        raise XSyncError("repository scan files 必须按唯一 path 排序")
    summary = manifest.get("summary")
    kinds: dict[str, int] = {}
    for item in files:
        kinds[item["kind"]] = kinds.get(item["kind"], 0) + 1
    if (not isinstance(summary, dict)
            or summary.get("included_files") != len(files)
            or summary.get("included_bytes") != sum(item["bytes"] for item in files)
            or summary.get("kinds") != kinds
            or summary.get("top_level_entries") != sorted({
                item["path"].split("/", 1)[0] for item in files
            })):
        raise XSyncError("repository scan summary 与 files 不一致")
    fingerprint_payload = {
        "policy_version": manifest["policy_version"],
        "repo_id": manifest["repo_id"],
        "baseline_commit": manifest["baseline_commit"],
        "state_token": manifest["state_token"],
        "scope": scope,
        "files": files,
    }
    fingerprint = hashlib.sha256(canonical_bytes(fingerprint_payload)).hexdigest()
    if (manifest.get("scan_id") != f"scan.{fingerprint}"
            or manifest.get("snapshot_id") != manifest.get("scan_id")
            or manifest.get("fingerprint") != f"sha256:{fingerprint}"):
        raise XSyncError("repository scan fingerprint 不一致")
    without_integrity = dict(manifest)
    claimed_integrity = without_integrity.pop("integrity_hash", None)
    actual_integrity = "sha256:" + hashlib.sha256(canonical_bytes(without_integrity)).hexdigest()
    if claimed_integrity != actual_integrity:
        raise XSyncError("repository scan integrity 不一致")
    return manifest


class Store:
    def __init__(self, repo: Path, learner: str | None = None):
        self.repo = repo
        self.root = repo / ".x-sync"
        if self.root.exists() and self.root.is_symlink():
            raise XSyncError(".x-sync 不得是符号链接")
        self.learner = validate_learner(learner) if learner else None
        self.project = None
        if learner:
            self.project = safe_child(
                self.root, "users", self.learner, "projects", repo_id(repo))

    @contextlib.contextmanager
    def lock(self, timeout: float = 5.0):
        lock_dir = safe_child(self.root, "locks", f"{repo_id(self.repo)}.lock")
        lock_dir.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + timeout
        while True:
            try:
                lock_dir.mkdir()
                atomic_json(lock_dir / "owner.json", {"pid": os.getpid(), "at": utc_now()})
                break
            except FileExistsError:
                owner_path = lock_dir / "owner.json"
                owner = None
                with contextlib.suppress(XSyncError):
                    owner = load_json(owner_path)
                try:
                    age = time.time() - lock_dir.stat().st_mtime
                except FileNotFoundError:
                    continue
                abandoned = ((isinstance(owner, dict) and not process_alive(owner.get("pid")))
                             or (owner is None and age > 30))
                if abandoned:
                    quarantine = lock_dir.with_name(f"{lock_dir.name}.stale-{uuid.uuid4().hex}")
                    try:
                        os.replace(lock_dir, quarantine)
                    except FileNotFoundError:
                        continue
                    shutil.rmtree(quarantine, ignore_errors=True)
                    continue
                if time.monotonic() >= deadline:
                    raise XSyncError("x-sync 状态正被另一进程修改")
                time.sleep(0.05)
        try:
            yield
        finally:
            shutil.rmtree(lock_dir, ignore_errors=True)

    def session_dir(self, session_id: str) -> Path:
        if not self.project:
            raise XSyncError("缺少 learner")
        validate_component(session_id, " session id", ID_RE)
        return safe_child(self.project, "sessions", session_id)

    def active_id(self) -> str:
        if not self.project:
            raise XSyncError("缺少 learner")
        data = load_json(self.project / "active.json")
        if not isinstance(data, dict) or not isinstance(data.get("session_id"), str):
            raise XSyncError("active.json 已损坏")
        return data["session_id"]

    def resolve_session(self, requested: str | None) -> tuple[str, Path]:
        sid = requested or self.active_id()
        directory = self.session_dir(sid)
        if not directory.is_dir():
            raise XSyncError(f"会话不存在: {sid}")
        return sid, directory

    def mutate(self, directory: Path, event_type: str, payload: dict,
               transform) -> dict:
        with self.lock():
            state_path = directory / "state.json"
            state = recover_state(directory)
            next_state = transform(json.loads(json.dumps(state)))
            if next_state == state:
                return state
            next_state["state_version"] = int(state.get("state_version", 0)) + 1
            next_state["updated_at"] = utc_now()
            events = directory / "events"
            events.mkdir(parents=True, exist_ok=True)
            seqs = [int(p.stem.split("-", 1)[0]) for p in events.glob("[0-9]*-*.json")]
            seq = max(seqs, default=0) + 1
            event_id = uuid.uuid4().hex
            event = {
                "schema_version": SCHEMA_VERSION,
                "record_type": "session_event", "event_id": event_id,
                "session_id": next_state["session_id"], "learner": self.learner,
                "sequence": seq, "event_type": event_type,
                "occurred_at": utc_now(),
                "from_version": state.get("state_version", 0),
                "to_version": next_state["state_version"],
                "payload": payload,
                "state_after": next_state,
            }
            label = f"new {event_type} event"
            validate_materialized_state(next_state, next_state["session_id"], label)
            validate_event_payload(event_type, payload, label)
            validate_event_transition({"state_after": state}, event, next_state, label)
            atomic_json(events / f"{seq:06d}-{event_id}.json", event)
            atomic_json(state_path, next_state)
            return next_state


def init_profile(store: Store) -> dict:
    assert store.project and store.learner
    ensure_private_git_exclude(store.repo)
    profile = safe_child(store.root, "users", store.learner, "profile.json")
    with store.lock():
        if profile.exists():
            data = load_json(profile)
        else:
            data = {"schema_version": SCHEMA_VERSION, "learner": store.learner,
                    "created_at": utc_now()}
            atomic_json(profile, data)
        store.project.mkdir(parents=True, exist_ok=True)
    return data  # type: ignore[return-value]


def ensure_private_git_exclude(repo: Path) -> None:
    """Ignore private learning data locally without changing the team's .gitignore."""
    repo = repo.resolve()
    exclude_value = git(repo, "rev-parse", "--git-path", "info/exclude", check=False)
    if not exclude_value:
        return
    exclude = Path(exclude_value)
    if not exclude.is_absolute():
        exclude = Path(os.path.abspath(repo / exclude))
    cursor = exclude
    while cursor != cursor.parent:
        if cursor.is_symlink():
            raise XSyncError(f"拒绝通过符号链接修改 Git exclude: {cursor}")
        cursor = cursor.parent
    if exclude.is_symlink():
        raise XSyncError(f"拒绝通过符号链接修改 Git exclude: {exclude}")
    rule = "/.x-sync/"
    existing = exclude.read_text(encoding="utf-8") if exclude.is_file() else ""
    if any(line.strip() == rule for line in existing.splitlines()):
        return
    updated = existing
    if updated and not updated.endswith("\n"):
        updated += "\n"
    updated += f"\n# Private x-sync learner data\n{rule}\n"
    atomic_write(exclude, updated.encode("utf-8"))


def repository_scan_directory(store: Store) -> Path:
    return safe_child(store.root, "repositories", repo_id(store.repo))


def repository_scan_response(manifest: dict, path: Path, reused: bool) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "scan_id": manifest["scan_id"],
        "repo_id": manifest["repo_id"],
        "baseline_commit": manifest["baseline_commit"],
        "policy_version": manifest["policy_version"],
        "complete": manifest["complete"],
        "created_at": manifest["created_at"],
        "summary": manifest["summary"],
        "manifest_path": str(path),
        "reused": reused,
    }


def scan_repository(store: Store) -> dict:
    """Persist or reuse the materialized repository-wide safe engineering scan."""
    ensure_private_git_exclude(store.repo)
    manifest = repository_scan_manifest(store.repo)
    scan_dir = repository_scan_directory(store)
    destination = safe_child(scan_dir, "scan.json")
    reused = False
    with store.lock():
        if destination.is_symlink():
            raise XSyncError("拒绝通过符号链接读写 repository scan")
        if destination.exists() and not destination.is_file():
            raise XSyncError("repository scan 路径必须是普通文件")
        current_head, _, current_token = repository_state_token(store.repo)
        if (current_head != manifest["baseline_commit"]
                or current_token != manifest["state_token"]):
            raise XSyncError("工程在扫描完成后又发生变化；请重试 x-sync")
        existing = None
        if destination.is_file():
            with contextlib.suppress(XSyncError):
                existing = validate_repository_scan(load_json(destination))
        if isinstance(existing, dict) and existing.get("scan_id") == manifest["scan_id"]:
            manifest = existing
            reused = True
        else:
            atomic_json(destination, manifest)
    return repository_scan_response(manifest, destination, reused)


def repository_scan_status(store: Store) -> dict:
    """Read the shared initial-scan gate without enumerating repository files."""
    scan_dir = repository_scan_directory(store)
    record = safe_child(scan_dir, "scan.json")
    missing = {
        "state": "missing", "required": True, "complete": False,
        "initial_scan_complete": False,
        "policy_version": SCAN_POLICY_VERSION,
    }
    if not record.is_file() or record.is_symlink():
        return missing
    try:
        manifest = validate_repository_scan(load_json(record), require_current_policy=False)
        if manifest.get("repo_id") != repo_id(store.repo) or manifest.get("scope", {}).get("type") != "full":
            raise XSyncError("repository scan 不属于当前完整工程")
    except XSyncError as exc:
        return {
            "state": "invalid", "required": True, "complete": False,
            "initial_scan_complete": False,
            "policy_version": SCAN_POLICY_VERSION, "reason": str(exc),
        }
    probe_error = None
    try:
        current_head, current_status, current_token = repository_state_token(store.repo)
        if manifest["policy_version"] != SCAN_POLICY_VERSION:
            state = "stale"
        elif current_head != manifest["baseline_commit"]:
            state = "stale"
        elif current_token != manifest.get("state_token"):
            state = "stale"
        elif current_status:
            # The scan captured this dirty tree exactly, but a cheap status call
            # cannot prove that same-name untracked content is still identical.
            state = "captured_dirty"
        else:
            state = "fresh"
    except XSyncError as exc:
        # A freshness probe must not hide a recoverable unfinished session.
        state = "unknown"
        probe_error = str(exc)
    policy_current = manifest["policy_version"] == SCAN_POLICY_VERSION
    result = {
        "state": state,
        "required": not policy_current,
        "complete": True,
        "initial_scan_complete": policy_current,
        "scan_id": manifest["scan_id"],
        "policy_version": manifest["policy_version"],
        "baseline_commit": manifest["baseline_commit"],
        "created_at": manifest["created_at"],
        "summary": manifest["summary"],
        "manifest_path": str(record),
    }
    if probe_error:
        result["reason"] = probe_error
    return result


def require_initial_repository_scan(store: Store) -> None:
    status = repository_scan_status(store)
    if status.get("required"):
        raise XSyncError(
            "首次运行必须先扫描整个安全工程目录；请执行 `xsync scan --repo <repo> --json`"
        )


def snapshot_evidence(repo: Path, store: Store, paths: list[str] | None,
                      kind: str | None = None, one_path: str | None = None,
                      lines: str | None = None, summary: str | None = None,
                      commit_ref: str | None = None,
                      claim_type: str | None = None) -> dict:
    """Create either a focused portable evidence record or a repo inventory."""
    repo = repo.resolve()
    allowed_claim_types = {"requirement", "decision", "implementation",
                           "general_knowledge", "inference", "conflict"}
    if claim_type is not None and claim_type not in allowed_claim_types:
        raise XSyncError("--claim-type 非法")
    relative = Path(one_path) if one_path else None
    if relative is not None and (
        relative.is_absolute() or ".." in relative.parts or is_sensitive_path(relative)
    ):
        raise XSyncError("evidence --path 必须是安全、无 .. 的仓库相对路径")
    if commit_ref:
        if lines:
            raise XSyncError("commit evidence 不支持 --lines")
        if kind not in {None, "commit", "bug"}:
            raise XSyncError("--commit 的 kind 必须为 commit 或 bug")
        commit = git(repo, "rev-parse", "--verify", f"{commit_ref}^{{commit}}")
        raw = commit_snapshot_bytes(repo, commit, relative)
        digest = hashlib.sha256(raw).hexdigest()
        now = utc_now()
        source = {"type": "commit", "commit": commit}
        if relative is not None:
            source["path"] = relative.as_posix()
        identity = hashlib.sha256((commit + str(relative) + digest).encode()).hexdigest()[:20]
        record = {
            "schema_version": 1, "record_type": "evidence", "id": f"ev.{identity}",
            "kind": kind or "commit", "claim_type": claim_type or "implementation",
            "authority": "repository_behavior", "title": summary or commit[:12],
            "claim": summary or f"Canonical object-delta snapshot of commit {commit[:12]}",
            "source": source, "repository": {"root_id": repo_id(repo),
            "baseline_commit": git(repo, "rev-parse", "HEAD"),
            "branch": git(repo, "rev-parse", "--abbrev-ref", "HEAD", check=False)},
            "content_hash": f"sha256:{digest}", "status": "active",
            "created_at": now, "verified_at": now,
        }
        destination = safe_child(store.root, "evidence", f"{record['id']}.json")
        atomic_json(destination, record)
        return {**record, "path": str(destination)}
    if one_path:
        assert relative is not None
        absolute = (repo / relative).resolve()
        try:
            absolute.relative_to(repo)
        except ValueError as exc:
            raise XSyncError("evidence path 越界") from exc
        if absolute.is_symlink() or not absolute.is_file():
            raise XSyncError(f"evidence 文件不存在或为符号链接: {one_path}")
        start = end = None
        if lines:
            match = re.fullmatch(r"([1-9][0-9]*):([1-9][0-9]*)", lines)
            if not match or int(match.group(2)) < int(match.group(1)):
                raise XSyncError("--lines 必须为 start:end，且 end >= start")
            start, end = map(int, match.groups())
        raw = focused_file_bytes(absolute, start, end)
        commit = git(repo, "rev-parse", "HEAD")
        branch = git(repo, "rev-parse", "--abbrev-ref", "HEAD", check=False)
        allowed = {"spec", "adr", "commit", "bug", "test", "code", "config",
                   "official_documentation", "inference", "conflict"}
        kind = kind or "code"
        if kind not in allowed or kind in {"commit", "official_documentation", "inference", "conflict"}:
            raise XSyncError("文件快照 kind 必须为 spec/adr/bug/test/code/config")
        source = {"type": "file", "path": relative.as_posix()}
        if start is not None:
            source.update({"start_line": start, "end_line": end})
        digest = hashlib.sha256(raw).hexdigest()
        identity = hashlib.sha256((one_path + str(start) + str(end) + digest).encode()).hexdigest()[:20]
        now = utc_now()
        claim_type = claim_type or {"spec": "requirement", "adr": "decision"}.get(
            kind, "implementation"
        )
        authority = "repository_intent" if kind in {"spec", "adr"} else "repository_behavior"
        record = {"schema_version": 1, "record_type": "evidence", "id": f"ev.{identity}",
                  "kind": kind, "claim_type": claim_type, "authority": authority,
                  "title": summary or one_path, "claim": summary or f"Snapshot of {one_path}",
                  "source": source, "repository": {"root_id": repo_id(repo),
                  "baseline_commit": commit, "branch": branch},
                  "content_hash": f"sha256:{digest}", "status": "active",
                  "created_at": now, "verified_at": now}
        destination = safe_child(store.root, "evidence", f"{record['id']}.json")
        atomic_json(destination, record)
        return {**record, "path": str(destination)}
    snapshot = repository_scan_manifest(repo, paths)
    destination = safe_child(store.root, "evidence", f"{snapshot['scan_id']}.json")
    with store.lock():
        if destination.is_symlink():
            raise XSyncError("拒绝通过符号链接写入 repository inventory")
        if destination.exists():
            snapshot = validate_repository_scan(load_json(destination))
        else:
            atomic_json(destination, snapshot)
    return {**snapshot, "path": str(destination)}


def normalize_question(question: dict, index: int) -> dict:
    q = dict(question)
    qid = q.get("id")
    if q.get("schema_version") != SCHEMA_VERSION or q.get("record_type") != "question":
        raise XSyncError(f"questions[{index}] schema_version/record_type 非法")
    if not isinstance(qid, str) or not ID_RE.fullmatch(qid):
        raise XSyncError(f"questions[{index}].id 非法")
    if not isinstance(q.get("version", 1), int) or isinstance(q.get("version", 1), bool) or q.get("version", 1) < 1:
        raise XSyncError(f"题目 {qid} version 必须为正整数")
    q["version"] = q.get("version", 1)
    if q.get("status", "active") not in {"draft", "active", "stale", "disputed", "retired"}:
        raise XSyncError(f"题目 {qid} status 非法")
    q["status"] = q.get("status", "active")
    if not GIT_OID_RE.fullmatch(str(q.get("baseline_commit", ""))):
        raise XSyncError(f"题目 {qid} baseline_commit 必须为完整 Git SHA")
    aliases = {"mcq": "single_choice", "choice": "single_choice", "open": "free_text",
               "free_response": "free_text"}
    q["type"] = aliases.get(q.get("type"), q.get("type"))
    if q["type"] not in {"single_choice", "free_text"}:
        raise XSyncError(f"题目 {qid} type 必须为 single_choice 或 free_text")
    q["domain"] = q.get("domain", q.get("axis"))
    q.pop("axis", None)
    if q["domain"] not in {"business", "technical"}:
        raise XSyncError(f"题目 {qid} domain 必须为 business 或 technical")
    q["depth"] = q.get("depth", q.get("level"))
    q.pop("level", None)
    if isinstance(q["depth"], str) and re.fullmatch(r"L[0-4]", q["depth"]):
        q["depth"] = int(q["depth"][1:]) + 1
    if (not isinstance(q["depth"], int) or isinstance(q["depth"], bool)
            or not 1 <= q["depth"] <= 5):
        raise XSyncError(f"题目 {qid} depth/level 必须为 1..5 或 L0..L4")
    if not isinstance(q.get("prompt"), str) or not q["prompt"].strip():
        raise XSyncError(f"题目 {qid} 缺少 prompt")
    topics = q.get("topics")
    if (not isinstance(topics, list) or not topics
            or not all(isinstance(topic, str) and topic.strip() for topic in topics)
            or len(set(topics)) != len(topics)):
        raise XSyncError(f"题目 {qid} topics 必须是非空字符串数组")
    styles = q.get("styles", q.get("modes"))
    q.pop("modes", None)
    if (not isinstance(styles, list) or not styles
            or not all(isinstance(style, str) for style in styles)
            or len(set(styles)) != len(styles)
            or not set(styles) <= {"regular", "socratic"}):
        raise XSyncError(f"题目 {qid} styles 必须是 regular/socratic 的非空子集")
    q["styles"] = styles
    answer = q.get("answer", q.get("answer_key"))
    q.pop("answer_key", None)
    if not isinstance(answer, dict):
        raise XSyncError(f"题目 {qid} 缺少 answer/answer_key")
    answer = dict(answer)
    if "correct_choice_id" in answer:
        answer.setdefault("correct_choice", answer["correct_choice_id"])
        answer.pop("correct_choice_id", None)
    if "rubric" not in answer and isinstance(q.get("rubric"), list):
        answer["rubric"] = q["rubric"]
    q.pop("rubric", None)
    q["answer"] = answer
    if q["type"] == "single_choice":
        choices = q.get("choices")
        if not isinstance(choices, list) or not 2 <= len(choices) <= 6:
            raise XSyncError(f"题目 {qid} 需要 2..6 个 choices")
        ids = []
        normalized = []
        for pos, choice in enumerate(choices):
            if isinstance(choice, str):
                cid, text = chr(65 + pos), choice
            elif isinstance(choice, dict):
                cid, text = choice.get("id"), choice.get("text")
            else:
                raise XSyncError(f"题目 {qid} choice 格式错误")
            if (not isinstance(cid, str) or not cid.strip() or not ID_RE.fullmatch(cid)
                    or not isinstance(text, str) or not text.strip() or cid in ids):
                raise XSyncError(f"题目 {qid} choice id/text 非法或重复")
            ids.append(cid)
            normalized_choice = {"id": cid, "text": text}
            for key in ("misconception", "evidence_ids"):
                if isinstance(choice, dict) and key in choice:
                    normalized_choice[key] = choice[key]
            normalized.append(normalized_choice)
        q["choices"] = normalized
        if answer.get("correct_choice") not in ids:
            raise XSyncError(f"题目 {qid} correct_choice 不在 choices 中")
        for choice in normalized:
            choice_evidence = choice.get("evidence_ids")
            if not isinstance(choice_evidence, list) or not choice_evidence or not all(
                isinstance(evidence_id, str) for evidence_id in choice_evidence
            ) or len(set(choice_evidence)) != len(choice_evidence):
                raise XSyncError(f"题目 {qid} choice {choice['id']} 缺少 evidence_ids")
            misconception = choice.get("misconception")
            if choice["id"] == answer["correct_choice"] and misconception is not None:
                raise XSyncError(f"题目 {qid} 正确选项 misconception 必须为 null")
            if choice["id"] != answer["correct_choice"] and (
                not isinstance(misconception, str) or not misconception.strip()
            ):
                raise XSyncError(f"题目 {qid} 干扰项必须说明 misconception")
        if not isinstance(answer.get("explanation"), str) or not answer["explanation"].strip():
            raise XSyncError(f"题目 {qid} 缺少 answer.explanation")
    else:
        rubric = answer.get("rubric")
        if not isinstance(rubric, list) or not rubric:
            raise XSyncError(f"主观题 {qid} 需要非空 rubric")
        rubric_ids = set()
        weight_sum = 0.0
        for item in rubric:
            points = item.get("points", item.get("weight")) if isinstance(item, dict) else None
            criterion = item.get("criterion", item.get("description")) if isinstance(item, dict) else None
            if (not isinstance(item, dict) or not isinstance(item.get("id"), str)
                    or item["id"] in rubric_ids or not isinstance(points, (int, float))
                    or isinstance(points, bool) or points <= 0
                    or not isinstance(criterion, str) or not criterion.strip()):
                raise XSyncError(f"题目 {qid} rubric 非法")
            evidence_ids = item.get("evidence_ids")
            if not isinstance(evidence_ids, list) or not evidence_ids or not all(
                isinstance(evidence_id, str) for evidence_id in evidence_ids
            ) or len(set(evidence_ids)) != len(evidence_ids):
                raise XSyncError(f"题目 {qid} rubric {item['id']} 缺少 evidence_ids")
            if not isinstance(item.get("required"), bool):
                raise XSyncError(f"题目 {qid} rubric {item['id']} required 必须为 boolean")
            weight_sum += float(item.get("weight", points))
            rubric_ids.add(item["id"])
        if abs(weight_sum - 1.0) > 0.000001:
            raise XSyncError(f"题目 {qid} rubric weight 总和必须为 1")
        if not isinstance(answer.get("reference_answer"), str) or not answer["reference_answer"].strip():
            raise XSyncError(f"题目 {qid} 缺少 answer.reference_answer")
        if not isinstance(answer.get("explanation"), str) or not answer["explanation"].strip():
            raise XSyncError(f"题目 {qid} 缺少 answer.explanation")
    evidence = q.get("evidence_ids", [])
    if (not isinstance(evidence, list) or not all(isinstance(x, str) for x in evidence)
            or len(set(evidence)) != len(evidence)):
        raise XSyncError(f"题目 {qid} evidence_ids 非法")
    if not evidence:
        raise XSyncError(f"题目 {qid} 必须引用 evidence_ids")
    prerequisites = q.get("prerequisite_question_ids", [])
    if (not isinstance(prerequisites, list)
            or not all(isinstance(item, str) for item in prerequisites)
            or len(set(prerequisites)) != len(prerequisites) or qid in prerequisites):
        raise XSyncError(f"题目 {qid} prerequisite_question_ids 非法")
    q["prerequisite_question_ids"] = prerequisites
    socratic = q.get("socratic")
    if "socratic" in styles:
        if not isinstance(socratic, dict):
            raise XSyncError(f"题目 {qid} socratic 非法")
        maximum = socratic.get("max_attempts")
        probes = socratic.get("probes")
        hints = socratic.get("hints")
        valid_hints = isinstance(hints, list) and all(
            isinstance(item, dict) and item.get("level") in {1, 2, 3, 4}
            and isinstance(item.get("text"), str) and item["text"].strip()
            for item in hints
        )
        levels = [item["level"] for item in hints] if valid_hints else []
        if (not isinstance(maximum, int) or isinstance(maximum, bool) or not 1 <= maximum <= 4
                or not isinstance(probes, list) or not probes
                or not all(isinstance(item, str) and item.strip() for item in probes)
                or not valid_hints or not hints
                or levels != sorted(set(levels))):
            raise XSyncError(f"题目 {qid} socratic 配置不完整")
    validation = q.get("validation")
    if q["status"] == "active" and (
        not isinstance(validation, dict) or validation.get("grounded") is not True
        or validation.get("evidence_valid") is not True
        or validation.get("ambiguity") != "low"
        or not isinstance(validation.get("validator"), str)
        or not isinstance(validation.get("validated_at"), str)
    ):
        raise XSyncError(f"active 题目 {qid} 必须通过 grounding/evidence/ambiguity 校验")
    generation = q.get("generation")
    if not isinstance(generation, dict) or not all(
        isinstance(generation.get(field), str) and generation[field].strip()
        for field in ("generator", "generator_version", "prompt_version")
    ):
        raise XSyncError(f"题目 {qid} generation provenance 不完整")
    try:
        parse_timestamp(str(q.get("created_at")))
        if isinstance(validation, dict):
            parse_timestamp(str(validation.get("validated_at")))
    except XSyncError as exc:
        raise XSyncError(f"题目 {qid} 时间字段非法") from exc
    q.pop("topic", None)
    return q


def validate_bank(value: object) -> dict:
    if not isinstance(value, dict):
        raise XSyncError("题库根节点必须为 object")
    bank = dict(value)
    if bank.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise XSyncError("不支持的题库 schema_version")
    bank_repo_id = bank.get("repo_id")
    if not isinstance(bank_repo_id, str) or not bank_repo_id.strip():
        raise XSyncError("repo_id 不能为空")
    baseline = bank.get("baseline_commit")
    if not GIT_OID_RE.fullmatch(str(baseline)):
        raise XSyncError("baseline_commit 必须为完整 Git SHA")
    created_at = bank.get("created_at")
    try:
        created = dt.datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
    except ValueError as exc:
        raise XSyncError("created_at 必须为 RFC 3339 时间") from exc
    if created.tzinfo is None:
        raise XSyncError("created_at 必须包含时区")
    questions = bank.get("questions")
    if not isinstance(questions, list) or not questions:
        raise XSyncError("题库 questions 不能为空")
    bank["questions"] = [normalize_question(q, i) if isinstance(q, dict)
                         else (_ for _ in ()).throw(XSyncError(f"questions[{i}] 必须为 object"))
                         for i, q in enumerate(questions)]
    if len({q["id"] for q in bank["questions"]}) != len(bank["questions"]):
        raise XSyncError("question id 重复")
    evidence = bank.get("evidence", [])
    if not isinstance(evidence, list):
        raise XSyncError("evidence 必须为 array")
    allowed_kinds = {"spec", "adr", "commit", "bug", "test", "code", "config",
                     "official_documentation", "inference", "conflict"}
    allowed_claims = {"requirement", "decision", "implementation", "general_knowledge",
                      "inference", "conflict"}
    allowed_statuses = {"active", "stale", "disputed", "retired"}
    allowed_authorities = {"repository_intent", "repository_behavior", "official_external",
                           "derived", "conflicted"}
    for item in evidence:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            raise XSyncError("evidence 必须是带 id 的 object")
        if item.get("schema_version") != SCHEMA_VERSION or item.get("record_type") != "evidence":
            raise XSyncError(f"evidence {item['id']} schema_version/record_type 非法")
        if (not isinstance(item.get("title"), str) or not item["title"].strip()
                or not isinstance(item.get("claim"), str) or not item["claim"].strip()):
            raise XSyncError(f"evidence {item['id']} title/claim 不能为空")
        try:
            parse_timestamp(str(item.get("created_at")))
            parse_timestamp(str(item.get("verified_at")))
        except XSyncError as exc:
            raise XSyncError(f"evidence {item['id']} 时间字段非法") from exc
        if item.get("kind") not in allowed_kinds or item.get("claim_type") not in allowed_claims:
            raise XSyncError(f"evidence {item['id']} kind/claim_type 非法")
        if item.get("status") not in allowed_statuses:
            raise XSyncError(f"evidence {item['id']} status 非法")
        if item.get("authority") not in allowed_authorities:
            raise XSyncError(f"evidence {item['id']} authority 非法")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(item.get("content_hash", ""))):
            raise XSyncError(f"evidence {item['id']} content_hash 非法")
        source = item.get("source")
        if not isinstance(source, dict) or source.get("type") not in {"file", "commit", "url"}:
            raise XSyncError(f"evidence {item['id']} source 非法")
        if source["type"] == "file":
            relative = Path(str(source.get("path", "")))
            if not source.get("path") or relative.is_absolute() or ".." in relative.parts or is_sensitive_path(relative):
                raise XSyncError(f"evidence {item['id']} file path 非法")
            has_start, has_end = "start_line" in source, "end_line" in source
            if has_start != has_end or (has_start and (
                not isinstance(source["start_line"], int) or isinstance(source["start_line"], bool)
                or not isinstance(source["end_line"], int) or isinstance(source["end_line"], bool)
                or source["start_line"] < 1 or source["end_line"] < source["start_line"]
            )):
                raise XSyncError(f"evidence {item['id']} 行号非法")
        elif source["type"] == "commit":
            if not GIT_OID_RE.fullmatch(str(source.get("commit", ""))):
                raise XSyncError(f"evidence {item['id']} commit 非法")
            if "path" in source:
                relative = Path(str(source["path"]))
                if (not source["path"] or relative.is_absolute() or ".." in relative.parts
                        or is_sensitive_path(relative)):
                    raise XSyncError(f"evidence {item['id']} commit path 非法")
        else:
            parsed = urllib.parse.urlsplit(str(source.get("url", "")))
            if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                    or parsed.password is not None or parsed.fragment):
                raise XSyncError(f"evidence {item['id']} official URL 非法")
            if (item.get("kind") != "official_documentation"
                    or item.get("authority") != "official_external"
                    or item.get("claim_type") != "general_knowledge"):
                raise XSyncError(f"URL evidence {item['id']} 必须是官方通用技术证据")
        if item.get("kind") == "official_documentation" and source["type"] != "url":
            raise XSyncError(f"official_documentation {item['id']} 必须使用 URL source")
        derived = item.get("derived_from")
        if item.get("kind") in {"inference", "conflict"} or item.get("claim_type") in {"inference", "conflict"}:
            if not isinstance(derived, list) or len(derived) < 2 or not all(isinstance(x, str) for x in derived):
                raise XSyncError(f"evidence {item['id']} 必须有至少两个 derived_from")
        if ((item.get("kind") == "inference" or item.get("claim_type") == "inference")
                and item.get("authority") != "derived"):
            raise XSyncError(f"inference evidence {item['id']} authority 必须为 derived")
        if ((item.get("kind") == "conflict" or item.get("claim_type") == "conflict")
                and item.get("authority") != "conflicted"):
            raise XSyncError(f"conflict evidence {item['id']} authority 必须为 conflicted")
        repository = item.get("repository")
        if (not isinstance(repository, dict)
                or not isinstance(repository.get("root_id"), str)
                or not repository.get("root_id")
                or not GIT_OID_RE.fullmatch(str(repository.get("baseline_commit", "")))):
            raise XSyncError(f"evidence {item['id']} repository snapshot 非法")
        if repository.get("root_id") != bank_repo_id:
            raise XSyncError(f"evidence {item['id']} 不属于 bank.repo_id")
        if repository.get("baseline_commit") != baseline:
            raise XSyncError(f"evidence {item['id']} baseline_commit 与 bank 不一致")
    known = {e.get("id") for e in evidence if isinstance(e, dict)}
    if len(known) != len(evidence) or None in known:
        raise XSyncError("evidence id 缺失或重复")
    for item in evidence:
        missing_derived = set(item.get("derived_from", [])) - known
        if missing_derived:
            raise XSyncError(f"evidence {item['id']} derived_from 不存在: {sorted(missing_derived)}")
    for q in bank["questions"]:
        if q.get("baseline_commit") != baseline:
            raise XSyncError(f"题目 {q['id']} baseline_commit 与 bank 不一致")
        top_level_cited = set(q.get("evidence_ids", []))
        nested_cited = set()
        for choice in q.get("choices", []):
            choice_evidence = choice.get("evidence_ids", [])
            if not isinstance(choice_evidence, list):
                raise XSyncError(f"题目 {q['id']} choice evidence_ids 非法")
            nested_cited.update(choice_evidence)
        for rubric in q.get("answer", {}).get("rubric", []):
            nested_cited.update(rubric.get("evidence_ids", []))
        omitted = nested_cited - top_level_cited
        if omitted:
            raise XSyncError(f"题目 {q['id']} evidence_ids 未覆盖嵌套证据: {sorted(omitted)}")
        cited = top_level_cited | nested_cited
        missing = cited - known
        if missing:
            raise XSyncError(f"题目 {q['id']} 引用了未知 evidence: {sorted(missing)}")
        if q["status"] == "active":
            unusable = {item["id"] for item in evidence
                        if item["id"] in cited and item.get("status") != "active"}
            if unusable:
                raise XSyncError(f"active 题目 {q['id']} 引用了不可用 evidence: {sorted(unusable)}")
        if q["type"] == "single_choice":
            authorities = [item for item in evidence if item["id"] in cited]
            if authorities and all(
                item.get("kind") in {"inference", "conflict"}
                or item.get("claim_type") in {"inference", "conflict"}
                or item.get("authority") in {"derived", "conflicted"}
                for item in authorities
            ):
                raise XSyncError(f"单选题 {q['id']} 不能只依赖 inference/conflict")
    question_ids = {q["id"] for q in bank["questions"]}
    question_by_id = {q["id"]: q for q in bank["questions"]}
    for q in bank["questions"]:
        missing = set(q["prerequisite_question_ids"]) - question_ids
        if missing:
            raise XSyncError(f"题目 {q['id']} prerequisite 不存在: {sorted(missing)}")
        for prerequisite_id in q["prerequisite_question_ids"]:
            prerequisite = question_by_id[prerequisite_id]
            if q["status"] == "active" and prerequisite["status"] != "active":
                raise XSyncError(f"active 题目 {q['id']} 依赖非 active 题目 {prerequisite_id}")
            if not set(q["styles"]) <= set(prerequisite["styles"]):
                raise XSyncError(f"题目 {q['id']} 的 style 缺少可用 prerequisite")
    graph = {q["id"]: set(q["prerequisite_question_ids"]) for q in bank["questions"]}
    pending_graph = {key: set(value) for key, value in graph.items()}
    while pending_graph:
        ready = {key for key, dependencies in pending_graph.items() if not dependencies}
        if not ready:
            raise XSyncError("prerequisite_question_ids 存在循环")
        pending_graph = {key: dependencies - ready for key, dependencies in pending_graph.items()
                         if key not in ready}
    working_tree = bank.get("working_tree")
    if not isinstance(working_tree, dict) or not isinstance(working_tree.get("dirty"), bool):
        raise XSyncError("working_tree.dirty 必须为 boolean")
    if working_tree["dirty"] and not re.fullmatch(
        r"sha256:[0-9a-f]{64}", str(working_tree.get("diff_hash", ""))
    ):
        raise XSyncError("dirty bank 必须记录 working_tree.diff_hash")
    supplied_id = bank.get("bank_id")
    without_id = dict(bank)
    without_id.pop("bank_id", None)
    digest = hashlib.sha256(canonical_bytes(without_id)).hexdigest()[:20]
    if supplied_id is not None:
        validate_component(str(supplied_id), " bank id", ID_RE)
    bank["bank_id"] = str(supplied_id or digest)
    bank["schema_version"] = SCHEMA_VERSION
    return bank


def validate_bank_for_repo(repo: Path, bank: dict) -> None:
    expected = repo_id(repo)
    if bank.get("repo_id") != expected:
        raise XSyncError(
            f"bank.repo_id 与当前仓库不匹配: {bank.get('repo_id')!r} != {expected!r}"
        )


def install_bank(store: Store, source: Path) -> dict:
    assert store.project
    require_initial_repository_scan(store)
    bank = validate_bank(load_json(source))
    validate_bank_for_repo(store.repo, bank)
    validate_evidence_freshness(store.repo, bank)
    init_profile(store)
    destination = safe_child(store.project, "banks", f"{bank['bank_id']}.json")
    with store.lock():
        if destination.exists():
            existing = validate_bank(load_json(destination))
            if canonical_bytes(existing) != canonical_bytes(bank):
                raise XSyncError(f"bank_id {bank['bank_id']} 已存在且内容不同；请使用新 bank_id")
        atomic_json(destination, bank)
    return {"bank_id": bank["bank_id"], "path": str(destination),
            "questions": len(bank["questions"])}


def prioritize_questions(store: Store, questions: list[dict]) -> list[dict]:
    """Prefer due/fragile knowledge while preserving prerequisite order."""
    assert store.project
    mastery_path = store.project / "mastery.json"
    mastery = None
    if mastery_path.is_file():
        with contextlib.suppress(XSyncError):
            mastery = load_json(mastery_path)
    elif (store.project / "sessions").is_dir():
        with contextlib.suppress(XSyncError):
            mastery = rebuild_mastery(store)
    reviews = {}
    if isinstance(mastery, dict):
        reviews = {
            (item.get("question_id"), item.get("question_version", 1)): item
            for item in mastery.get("reviews", []) if isinstance(item, dict)
        }
    blocked = {
        question["id"] for question in questions
        if reviews.get((question["id"], question.get("version", 1)), {}).get("stage") == "disputed"
    }
    changed = True
    while changed:
        changed = False
        for question in questions:
            if question["id"] not in blocked and any(
                prerequisite in blocked
                for prerequisite in question.get("prerequisite_question_ids", [])
            ):
                blocked.add(question["id"])
                changed = True
    now = dt.datetime.now(dt.timezone.utc)
    candidates = []
    for index, question in enumerate(questions):
        review = reviews.get((question["id"], question.get("version", 1)))
        if question["id"] in blocked:
            continue
        due = False
        if review and review.get("due_at"):
            with contextlib.suppress(XSyncError):
                due = parse_timestamp(review["due_at"]) <= now
        if due:
            group = 0
        elif review is None:
            group = 2
        elif review.get("stage") == "learning":
            group = 1
        elif review.get("stage") == "reviewing":
            group = 3
        else:
            group = 4
        candidates.append((question, (group, int(question["depth"]), index)))
    remaining = {item[0]["id"]: item for item in candidates}
    emitted = set()
    ordered = []
    while remaining:
        eligible = [item for item in remaining.values()
                    if all(prerequisite not in remaining or prerequisite in emitted
                           for prerequisite in item[0].get("prerequisite_question_ids", []))]
        if not eligible:
            raise XSyncError("题库 prerequisite_question_ids 存在循环")
        question, _ = min(eligible, key=lambda item: item[1])
        ordered.append(question)
        emitted.add(question["id"])
        del remaining[question["id"]]
    return ordered


def select_session_questions(questions: list[dict], count: int | None,
                             focus: str) -> list[dict]:
    """Select a dependency-safe prefix and make finite mixed sessions truly mixed."""
    if count is None:
        return questions
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise XSyncError("--count 必须为正整数")
    if len(questions) < count:
        raise XSyncError(
            f"题库只有 {len(questions)} 道可用题，少于请求的 {count} 道；请补充题库"
        )
    if focus != "mixed":
        return questions[:count]
    required_domains = {"business", "technical"}
    available_domains = {question["domain"] for question in questions}
    if count < 2 or not required_domains <= available_domains:
        raise XSyncError("mixed 会话必须至少两题，并同时包含 business 与 technical")

    order = {question["id"]: index for index, question in enumerate(questions)}
    by_id = {question["id"]: question for question in questions}
    closure_cache: dict[str, frozenset[str]] = {}

    def prerequisite_closure(question_id: str, visiting: frozenset[str] = frozenset()
                             ) -> frozenset[str]:
        cached = closure_cache.get(question_id)
        if cached is not None:
            return cached
        if question_id in visiting:
            raise XSyncError("题库 prerequisite_question_ids 存在循环")
        question = by_id.get(question_id)
        if question is None:
            raise XSyncError(f"题目 prerequisite 不存在: {question_id}")
        result = {question_id}
        for prerequisite in question.get("prerequisite_question_ids", []):
            result.update(prerequisite_closure(prerequisite, visiting | {question_id}))
        frozen = frozenset(result)
        closure_cache[question_id] = frozen
        return frozen

    business = [question for question in questions if question["domain"] == "business"]
    technical = [question for question in questions if question["domain"] == "technical"]
    feasible: list[tuple[tuple, set[str]]] = []
    for business_question in business:
        for technical_question in technical:
            required = set(prerequisite_closure(business_question["id"]))
            required.update(prerequisite_closure(technical_question["id"]))
            if len(required) <= count:
                indices = sorted(order[question_id] for question_id in required)
                score = (
                    max(order[business_question["id"]], order[technical_question["id"]]),
                    len(required), tuple(indices),
                )
                feasible.append((score, required))
    if not feasible:
        raise XSyncError(
            f"题库无法在 {count} 道题内同时覆盖 business 与 technical；请补充或调整 prerequisite"
        )
    selected_ids = min(feasible, key=lambda item: item[0])[1]
    while len(selected_ids) < count:
        choice = next((question for question in questions
                       if question["id"] not in selected_ids
                       and set(question.get("prerequisite_question_ids", [])) <= selected_ids),
                      None)
        if choice is None:
            raise XSyncError("题库 prerequisite 无法形成可用的会话顺序")
        selected_ids.add(choice["id"])
    return [question for question in questions if question["id"] in selected_ids]


def start_session(store: Store, bank_id: str, style: str, channel: str,
                  count: int | None = None, focus: str = "mixed",
                  max_depth: int | None = None, task_scope: str | None = None) -> dict:
    assert store.project
    require_initial_repository_scan(store)
    validate_component(bank_id, " bank id", ID_RE)
    bank_path = safe_child(store.project, "banks", f"{bank_id}.json")
    bank = validate_bank(load_json(bank_path))
    validate_bank_for_repo(store.repo, bank)
    if focus not in {"business", "technical", "mixed"}:
        raise XSyncError("focus 必须为 business、technical 或 mixed")
    if max_depth is not None and (
        not isinstance(max_depth, int) or isinstance(max_depth, bool) or not 1 <= max_depth <= 5
    ):
        raise XSyncError("max_depth 必须为 1..5")
    if task_scope is not None and (not isinstance(task_scope, str) or not task_scope.strip()):
        raise XSyncError("task_scope 不能为空")
    questions = [
        question for question in bank["questions"]
        if question.get("status", "active") == "active"
        and style in question.get("styles", ["regular", "socratic"])
        and (focus == "mixed" or question["domain"] == focus)
        and (max_depth is None or question["depth"] <= max_depth)
    ]
    if not questions:
        raise XSyncError(f"题库没有可用于 {style} 模式的 active 题目")
    stale_question_ids = []
    fresh_questions = []
    for question in questions:
        try:
            validate_question_evidence(store.repo, bank, question)
        except EvidenceStaleError:
            stale_question_ids.append(question["id"])
        else:
            fresh_questions.append(question)
    available = {question["id"] for question in fresh_questions}
    changed = True
    while changed:
        changed = False
        for question in list(fresh_questions):
            if any(prerequisite not in available
                   for prerequisite in question.get("prerequisite_question_ids", [])):
                fresh_questions.remove(question)
                available.remove(question["id"])
                stale_question_ids.append(question["id"])
                changed = True
    questions = fresh_questions
    if not questions:
        raise XSyncError(
            "当前模式的题目证据均已过期或依赖过期: " + ", ".join(sorted(stale_question_ids))
        )
    questions = prioritize_questions(store, questions)
    if not questions:
        raise XSyncError("题库中的知识点都处于 disputed 状态；请先复核题库")
    questions = select_session_questions(questions, count, focus)
    bank = {**bank, "questions": questions}
    sid = uuid.uuid4().hex
    directory = store.session_dir(sid)
    now = utc_now()
    config = {"schema_version": SCHEMA_VERSION, "session_id": sid,
              "learner": store.learner, "bank_id": bank_id,
              "repo_id": repo_id(store.repo), "baseline_commit": bank.get("baseline_commit"),
              "focus_topics": sorted({t for q in bank["questions"] for t in q.get("topics", [])}),
              "stale_question_ids": sorted(set(stale_question_ids)),
              "focus": focus, "max_depth": max_depth,
              "task_scope": task_scope or "repository onboarding",
              "style": style, "channel": channel, "count": len(bank["questions"]),
              "created_at": now}
    state = {"schema_version": SCHEMA_VERSION, "session_id": sid,
             "state_version": 1, "status": "question_open", "current_index": 0,
             "current_question_id": bank["questions"][0]["id"],
             "total": len(bank["questions"]), "attempts": [],
             "consumed_continue_keys": [],
             "created_at": now, "updated_at": now}
    with store.lock():
        directory.mkdir(parents=True)
        atomic_json(directory / "config.json", config)
        # Sessions read this immutable snapshot, so reinstalling a bank can
        # never rewrite the questions or evidence behind historical attempts.
        atomic_json(directory / "bank.json", bank)
        first_event_id = uuid.uuid4().hex
        atomic_json(directory / "events" / f"000001-{first_event_id}.json", {
            "schema_version": SCHEMA_VERSION, "record_type": "session_event",
            "event_id": first_event_id, "session_id": sid, "learner": store.learner,
            "sequence": 1, "event_type": "session_started", "occurred_at": now,
            "from_version": 0, "to_version": 1,
            "payload": {"style": style, "channel": channel,
                        "baseline_commit": config["baseline_commit"],
                        "focus_topics": config["focus_topics"]}, "state_after": state})
        atomic_json(directory / "state.json", state)
        atomic_json(store.project / "active.json", {"session_id": sid, "updated_at": now})
    return public_session(store, sid)


def validate_evidence_freshness(repo: Path, bank: dict,
                                evidence_ids: set[str] | None = None) -> None:
    """Reject when selected local/commit evidence no longer matches."""
    root = repo.resolve()
    stale = []
    for evidence in bank.get("evidence", []):
        if not isinstance(evidence, dict) or evidence.get("status", "active") != "active":
            continue
        if evidence_ids is not None and evidence.get("id") not in evidence_ids:
            continue
        source = evidence.get("source", {})
        if source.get("type") == "commit":
            commit = source.get("commit")
            relative_value = source.get("path")
            if not isinstance(commit, str):
                raise XSyncError(f"evidence {evidence.get('id')} commit 非法")
            relative = Path(relative_value) if isinstance(relative_value, str) else None
            if relative is not None and (
                relative.is_absolute() or ".." in relative.parts or is_sensitive_path(relative)
            ):
                raise XSyncError(f"evidence {evidence.get('id')} path 非法")
            try:
                raw = commit_snapshot_bytes(repo, commit, relative)
            except XSyncError:
                stale.append(str(evidence.get("id")))
                continue
            expected = str(evidence.get("content_hash", "")).removeprefix("sha256:")
            if expected and not secrets.compare_digest(expected, hashlib.sha256(raw).hexdigest()):
                stale.append(str(evidence.get("id")))
            continue
        if source.get("type") != "file" or not isinstance(source.get("path"), str):
            continue
        relative = Path(source["path"])
        if relative.is_absolute() or ".." in relative.parts or is_sensitive_path(relative):
            raise XSyncError(f"evidence {evidence.get('id')} path 非法")
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError:
            raise XSyncError(f"evidence {evidence.get('id')} path 越界")
        if not path.is_file() or path.is_symlink():
            stale.append(str(evidence.get("id")))
            continue
        start = end = None
        if "start_line" in source or "end_line" in source:
            start, end = source.get("start_line"), source.get("end_line")
            if not isinstance(start, int) or not isinstance(end, int) or start < 1 or end < start:
                raise XSyncError(f"evidence {evidence.get('id')} 行号非法")
        try:
            raw = focused_file_bytes(path, start, end)
        except XSyncError:
            stale.append(str(evidence.get("id")))
            continue
        expected = str(evidence.get("content_hash", "")).removeprefix("sha256:")
        if expected and not secrets.compare_digest(expected, hashlib.sha256(raw).hexdigest()):
            stale.append(str(evidence.get("id")))
    if stale:
        raise EvidenceStaleError(stale)


def validate_question_evidence(repo: Path, bank: dict, question: dict) -> None:
    validate_evidence_freshness(repo, bank, set(question.get("evidence_ids", [])))


def validate_review(value: object, label: str) -> dict:
    """Validate the complete persisted review shared by state and event payload."""
    if not isinstance(value, dict):
        raise XSyncError(f"{label} review 必须为 object")
    required = {
        "status", "correctness", "initial_correctness", "final_correctness",
        "reasoning", "evidence_use", "max_hint_level", "unaided",
        "rubric_results", "confidence", "brier_error", "overconfidence",
        "evidence_ids", "explanation", "grader", "outcome", "feedback",
        "reviewer", "reviewed_at",
    }
    if not required <= set(value):
        raise XSyncError(f"{label} review 字段不完整")
    status = value["status"]
    outcome = value["outcome"]
    if status not in {"graded", "unscored", "disputed", "stale"}:
        raise XSyncError(f"{label} review.status 非法")
    if outcome not in {None, "mastered", "probe", "exhausted", "disputed"}:
        raise XSyncError(f"{label} review.outcome 非法")

    scores = {}
    for field in ("correctness", "initial_correctness", "final_correctness",
                  "reasoning", "evidence_use"):
        score = value[field]
        if (score is not None and (not isinstance(score, (int, float))
                                   or isinstance(score, bool) or not math.isfinite(score)
                                   or not 0 <= score <= 1)):
            raise XSyncError(f"{label} review.{field} 必须在 [0,1] 或为 null")
        scores[field] = score
    if (not isinstance(value["confidence"], (int, float))
            or isinstance(value["confidence"], bool)
            or not math.isfinite(value["confidence"])
            or not 0 <= value["confidence"] <= 1):
        raise XSyncError(f"{label} review.confidence 非法")
    brier, overconfidence = value["brier_error"], value["overconfidence"]
    if (brier is not None and (not isinstance(brier, (int, float))
                               or isinstance(brier, bool) or not math.isfinite(brier)
                               or not 0 <= brier <= 1)):
        raise XSyncError(f"{label} review.brier_error 非法")
    if (overconfidence is not None and (
        not isinstance(overconfidence, (int, float)) or isinstance(overconfidence, bool)
        or not math.isfinite(overconfidence)
        or not -1 <= overconfidence <= 1
    )):
        raise XSyncError(f"{label} review.overconfidence 非法")
    max_hint = value["max_hint_level"]
    if (not isinstance(max_hint, int) or isinstance(max_hint, bool)
            or not 0 <= max_hint <= 4 or not isinstance(value["unaided"], bool)):
        raise XSyncError(f"{label} review hint/unaided 非法")
    correctness = scores["correctness"]
    if status == "graded":
        if correctness is None or outcome not in {"mastered", "probe", "exhausted"}:
            raise XSyncError(f"{label} graded review 缺少有效评分/outcome")
    elif status == "unscored":
        if correctness is not None or outcome not in {None, "probe"}:
            raise XSyncError(f"{label} unscored review 不得携带评分结论")
    elif status == "disputed":
        if correctness is not None or outcome != "disputed":
            raise XSyncError(f"{label} disputed review 语义不一致")
    elif correctness is not None or outcome is not None:
        raise XSyncError(f"{label} stale review 不得携带评分结论")
    if outcome == "mastered" and (correctness is None or correctness < 0.8):
        raise XSyncError(f"{label} mastered review correctness 不足")
    if outcome == "exhausted" and correctness is not None and correctness >= 0.8:
        raise XSyncError(f"{label} exhausted review correctness 矛盾")
    if value["unaided"] and (max_hint != 0 or correctness is None or correctness < 0.8):
        raise XSyncError(f"{label} unaided review 矛盾")
    if (brier is None) != (overconfidence is None):
        raise XSyncError(f"{label} review 校准指标必须同时存在或同时为 null")
    if brier is not None:
        if correctness is None:
            raise XSyncError(f"{label} 未评分 review 不得携带校准指标")
        expected_overconfidence = float(value["confidence"]) - float(correctness)
        expected_brier = expected_overconfidence ** 2
        if (not math.isclose(float(overconfidence), expected_overconfidence, abs_tol=1e-9)
                or not math.isclose(float(brier), expected_brier, abs_tol=1e-9)):
            raise XSyncError(f"{label} review 校准指标与 correctness/confidence 不一致")

    evidence_ids = value["evidence_ids"]
    if (not isinstance(evidence_ids, list)
            or not all(isinstance(item, str) for item in evidence_ids)
            or len(set(evidence_ids)) != len(evidence_ids)):
        raise XSyncError(f"{label} review.evidence_ids 非法")
    results = value["rubric_results"]
    if not isinstance(results, list):
        raise XSyncError(f"{label} review.rubric_results 非法")
    rubric_ids = []
    for result in results:
        if (not isinstance(result, dict) or not isinstance(result.get("rubric_id"), str)
                or not isinstance(result.get("earned"), (int, float))
                or isinstance(result.get("earned"), bool)
                or not math.isfinite(result["earned"]) or not 0 <= result["earned"] <= 1
                or not isinstance(result.get("score"), (int, float))
                or isinstance(result.get("score"), bool)
                or result.get("score") not in {0, 0.5, 1}
                or not isinstance(result.get("evidence_ids"), list)
                or not all(isinstance(item, str) for item in result["evidence_ids"])
                or not set(result["evidence_ids"]) <= set(evidence_ids)
                or not isinstance(result.get("justification"), str)):
            raise XSyncError(f"{label} rubric_result 非法")
        rubric_ids.append(result["rubric_id"])
    if len(set(rubric_ids)) != len(rubric_ids):
        raise XSyncError(f"{label} rubric_result 重复")
    if not isinstance(value["explanation"], str) or not isinstance(value["feedback"], str):
        raise XSyncError(f"{label} review 解释必须为字符串")
    grader = value["grader"]
    if (not isinstance(grader, dict) or not all(
        isinstance(grader.get(field), str) and grader[field]
        for field in ("name", "version", "prompt_version")
    )):
        raise XSyncError(f"{label} review.grader 非法")
    reviewer = value["reviewer"]
    if (not isinstance(reviewer, dict)
            or not any(isinstance(reviewer.get(field), str) and reviewer[field]
                       for field in ("name", "kind"))):
        raise XSyncError(f"{label} review.reviewer 非法")
    parse_timestamp(value["reviewed_at"])
    return value


def validate_materialized_state(value: object, session_id: str, label: str) -> dict:
    if not isinstance(value, dict) or value.get("session_id") != session_id:
        raise XSyncError(f"{label} 会话状态非法")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise XSyncError(f"{label} schema_version 非法")
    status = value.get("status")
    index, total, version = (value.get("current_index"), value.get("total"),
                             value.get("state_version"))
    if status not in {"question_open", "answer_saved", "agent_review_pending",
                      "reviewed", "completed"}:
        raise XSyncError(f"{label} status 非法")
    if (not isinstance(index, int) or isinstance(index, bool) or index < 0
            or not isinstance(total, int) or isinstance(total, bool) or total < 1
            or not isinstance(version, int) or isinstance(version, bool) or version < 1
            or not isinstance(value.get("attempts"), list)):
        raise XSyncError(f"{label} 状态字段非法")
    current = value.get("current_question_id")
    if status == "completed":
        if current is not None or index != total:
            raise XSyncError(f"{label} completed 状态非法")
    elif not isinstance(current, str) or not current or index >= total:
        raise XSyncError(f"{label} 当前题目非法")
    consumed = value.get("consumed_continue_keys")
    if (not isinstance(consumed, list)
            or not all(isinstance(key, str) and key for key in consumed)
            or len(set(consumed)) != len(consumed)):
        raise XSyncError(f"{label} consumed_continue_keys 非法")
    allowed_attempt_fields = {
        "attempt_id", "question_id", "question_version", "response", "reason",
        "confidence", "saved_at", "evidence_check", "auto_result", "review",
    }
    attempt_ids = []
    for attempt in value["attempts"]:
        if (not isinstance(attempt, dict) or set(attempt) - allowed_attempt_fields
                or not isinstance(attempt.get("attempt_id"), str)
                or not isinstance(attempt.get("question_id"), str)
                or not isinstance(attempt.get("question_version"), int)
                or isinstance(attempt.get("question_version"), bool)
                or not isinstance(attempt.get("response"), str)
                or not isinstance(attempt.get("reason"), str)
                or not isinstance(attempt.get("confidence"), (int, float))
                or isinstance(attempt.get("confidence"), bool)
                or not 0 <= attempt["confidence"] <= 1
                or not isinstance(attempt.get("saved_at"), str)
                or not isinstance(attempt.get("evidence_check"), dict)):
            raise XSyncError(f"{label} attempt 字段非法")
        evidence_check = attempt["evidence_check"]
        if (evidence_check.get("status") not in {"fresh", "stale"}
                or not isinstance(evidence_check.get("evidence_ids"), list)
                or not all(isinstance(item, str) for item in evidence_check["evidence_ids"])
                or not isinstance(evidence_check.get("checked_at"), str)):
            raise XSyncError(f"{label} attempt evidence_check 非法")
        parse_timestamp(attempt["saved_at"])
        parse_timestamp(evidence_check["checked_at"])
        auto_result = attempt.get("auto_result")
        if auto_result is not None and (
            not isinstance(auto_result, dict) or set(auto_result) != {"correct", "selected"}
            or not isinstance(auto_result["correct"], bool)
            or auto_result["selected"] != attempt["response"]
        ):
            raise XSyncError(f"{label} attempt.auto_result 非法")
        if "review" in attempt:
            validate_review(attempt["review"], f"{label} attempt {attempt['attempt_id']}")
        attempt_ids.append(attempt["attempt_id"])
    if len(set(attempt_ids)) != len(attempt_ids):
        raise XSyncError(f"{label} attempts 非法")
    return value


def validate_event_payload(event_type: str, payload: object, label: str) -> dict:
    if not isinstance(payload, dict):
        raise XSyncError(f"{label} payload 必须为 object")

    def strings(*names: str) -> bool:
        return all(isinstance(payload.get(name), str) and payload[name] for name in names)

    if event_type == "session_started":
        if (payload.get("style") not in {"regular", "socratic"}
                or payload.get("channel") not in {"terminal", "web"}
                or not GIT_OID_RE.fullmatch(str(payload.get("baseline_commit", "")))
                or not isinstance(payload.get("focus_topics"), list)
                or not all(isinstance(topic, str) for topic in payload["focus_topics"])):
            raise XSyncError(f"{label} session_started payload 非法")
    elif event_type == "question_presented":
        if (not strings("id", "evidence_validated_at", "idempotency_key")
                or not isinstance(payload.get("version"), int)
                or isinstance(payload.get("version"), bool) or payload["version"] < 1):
            raise XSyncError(f"{label} question_presented payload 非法")
        parse_timestamp(payload["evidence_validated_at"])
    elif event_type == "answer_submitted":
        confidence = payload.get("confidence")
        if (not strings("attempt_id", "question_id", "answer", "submitted_at")
                or not isinstance(payload.get("question_version"), int)
                or isinstance(payload.get("question_version"), bool)
                or not isinstance(confidence, (int, float)) or isinstance(confidence, bool)
                or not 0 <= confidence <= 1
                or not isinstance(payload.get("evidence_check"), dict)):
            raise XSyncError(f"{label} answer_submitted payload 非法")
        parse_timestamp(payload["submitted_at"])
    elif event_type == "socratic_turn":
        if (not strings("attempt_id", "speaker", "kind", "text", "idempotency_key")
                or payload.get("speaker") not in {"learner", "tutor"}
                or not isinstance(payload.get("turn"), int)
                or isinstance(payload.get("turn"), bool) or payload["turn"] < 1):
            raise XSyncError(f"{label} socratic_turn payload 非法")
    elif event_type == "grading_started":
        if not strings("attempt_id", "idempotency_key"):
            raise XSyncError(f"{label} grading_started payload 非法")
    elif event_type == "answer_reviewed":
        evaluation = payload.get("evaluation")
        required_evaluation = {
            "status", "correctness", "initial_correctness", "final_correctness",
            "reasoning", "evidence_use", "max_hint_level", "unaided",
            "rubric_results", "confidence", "brier_error", "overconfidence",
            "evidence_ids", "explanation", "grader", "outcome",
        }
        if (not strings("attempt_id") or not isinstance(evaluation, dict)
                or not required_evaluation <= set(evaluation)
                or "next_review_at" not in payload):
            raise XSyncError(f"{label} answer_reviewed payload 非法")
        validate_review(evaluation, label)
        if payload["next_review_at"] is not None:
            parse_timestamp(payload["next_review_at"])
    elif event_type == "session_ended":
        if (not strings("reason", "idempotency_key")
                or not isinstance(payload.get("assessed_scope"), dict)):
            raise XSyncError(f"{label} session_ended payload 非法")
    else:
        raise XSyncError(f"{label} event_type 非法: {event_type!r}")
    return payload


def validate_event_transition(previous: dict | None, event: dict,
                              state_after: dict, label: str) -> None:
    event_type = event["event_type"]
    payload = event["payload"]
    if previous is None:
        if (event_type != "session_started" or state_after["status"] != "question_open"
                or state_after["current_index"] != 0 or state_after["attempts"]
                or state_after["consumed_continue_keys"]):
            raise XSyncError(f"{label} 首事件必须创建 question_open 会话")
        return
    before = previous["state_after"]

    def unchanged_except(*allowed: str) -> bool:
        ignored = {"state_version", "updated_at", *allowed}
        return ({key: value for key, value in before.items() if key not in ignored}
                == {key: value for key, value in state_after.items() if key not in ignored})

    def current_attempt() -> dict | None:
        question_id = before.get("current_question_id")
        return next((attempt for attempt in reversed(before.get("attempts", []))
                     if attempt.get("question_id") == question_id), None)

    def expected_continue_key() -> str | None:
        attempt = current_attempt()
        if attempt is None:
            return None
        return idempotency_key(
            "continue", event["session_id"], attempt.get("attempt_id"),
            attempt.get("question_id"), attempt.get("question_version"),
        )

    def consumed_key_added() -> bool:
        key = payload.get("idempotency_key")
        old = before.get("consumed_continue_keys", [])
        new = state_after.get("consumed_continue_keys", [])
        return (isinstance(key, str) and key == expected_continue_key()
                and new == [*old, key] and key not in old)

    same_position = (
        state_after.get("current_index") == before.get("current_index")
        and state_after.get("current_question_id") == before.get("current_question_id")
    )
    if event_type == "session_started":
        raise XSyncError(f"{label} session_started 只能是首事件")
    if event_type == "answer_submitted":
        new_attempt = state_after["attempts"][-1] if state_after["attempts"] else {}
        auto = new_attempt.get("auto_result")
        auto_valid = (auto is None or (
            isinstance(auto, dict) and isinstance(auto.get("correct"), bool)
            and auto.get("selected") == new_attempt.get("response")
        ))
        valid = (before["status"] == "question_open" and state_after["status"] == "answer_saved"
                 and same_position and unchanged_except("status", "attempts")
                 and state_after["attempts"][:-1] == before["attempts"]
                 and len(state_after["attempts"]) == len(before["attempts"]) + 1
                 and new_attempt.get("attempt_id") == payload["attempt_id"]
                 and new_attempt.get("question_id") == payload["question_id"]
                 and new_attempt.get("question_id") == before["current_question_id"]
                 and new_attempt.get("question_version") == payload["question_version"]
                 and new_attempt.get("response") == payload["answer"]
                 and new_attempt.get("reason") == payload.get("reason", "")
                 and new_attempt.get("confidence") == payload["confidence"]
                 and new_attempt.get("saved_at") == payload["submitted_at"]
                 and new_attempt.get("evidence_check") == payload.get("evidence_check")
                 and "review" not in new_attempt
                 and auto_valid)
    elif event_type == "grading_started":
        pending = next((attempt for attempt in reversed(before["attempts"])
                        if attempt.get("attempt_id") == payload["attempt_id"]), None)
        expected_grading_key = idempotency_key(
            "grading", event["session_id"], pending.get("attempt_id"),
            pending.get("question_id"), pending.get("question_version"),
        ) if isinstance(pending, dict) else None
        valid = (before["status"] == "answer_saved"
                 and state_after["status"] == "agent_review_pending" and same_position
                 and isinstance(pending, dict) and "review" not in pending
                 and pending.get("question_id") == before["current_question_id"]
                 and payload.get("idempotency_key") == expected_grading_key
                 and unchanged_except("status"))
    elif event_type == "answer_reviewed":
        target_index = next((index for index, attempt in enumerate(state_after["attempts"])
                             if attempt.get("attempt_id") == payload["attempt_id"]), None)
        target = state_after["attempts"][target_index] if target_index is not None else None
        old_target = (before["attempts"][target_index]
                      if target_index is not None and target_index < len(before["attempts"])
                      else None)
        target_delta_valid = False
        evaluation_matches = False
        if isinstance(target, dict) and isinstance(old_target, dict):
            target_base = dict(target)
            review = target_base.pop("review", None)
            old_base = dict(old_target)
            if isinstance(review, dict) and review.get("status") == "stale":
                old_base.pop("auto_result", None)
                target_base.pop("auto_result", None)
            target_delta_valid = target_base == old_base and "review" not in old_target
            evaluation = payload.get("evaluation")
            evaluation_matches = (isinstance(evaluation, dict) and review == evaluation
                                  and review.get("confidence") == old_target.get("confidence"))
        valid = (before["status"] in {"answer_saved", "agent_review_pending"}
                 and state_after["status"] == "reviewed" and same_position
                 and unchanged_except("status", "attempts")
                 and len(state_after["attempts"]) == len(before["attempts"])
                 and all(state_after["attempts"][index] == before["attempts"][index]
                         for index in range(len(before["attempts"]))
                         if index != target_index)
                 and target_delta_valid and evaluation_matches)
    elif event_type == "socratic_turn":
        valid = (before["status"] == "reviewed" and state_after["status"] == "question_open"
                 and same_position and consumed_key_added()
                 and unchanged_except("status", "consumed_continue_keys"))
    elif event_type == "question_presented":
        valid = (before["status"] == "reviewed" and state_after["status"] == "question_open"
                 and state_after["current_index"] == before["current_index"] + 1
                 and state_after["current_question_id"] == payload["id"]
                 and consumed_key_added()
                 and unchanged_except("status", "current_index", "current_question_id",
                                      "consumed_continue_keys"))
    elif event_type == "session_ended":
        valid = (before["status"] == "reviewed" and state_after["status"] == "completed"
                 and state_after["current_index"] == before["current_index"] + 1
                 and state_after["current_question_id"] is None and consumed_key_added()
                 and unchanged_except("status", "current_index", "current_question_id",
                                      "completed_at", "consumed_continue_keys"))
    else:
        valid = False
    if not valid:
        raise XSyncError(f"{label} {event_type} 状态转移非法")


def load_event_chain(directory: Path) -> list[dict]:
    """Read and validate the contiguous append-only event chain."""
    events = directory / "events"
    if not events.is_dir():
        return []

    def file_sequence(path: Path) -> int:
        try:
            return int(path.stem.split("-", 1)[0])
        except ValueError as exc:
            raise XSyncError(f"事件文件名序号非法: {path.name}") from exc

    paths = sorted(events.glob("[0-9]*-*.json"), key=file_sequence)
    chain = []
    previous_version = 0
    expected_session = directory.name
    for expected_sequence, event_path in enumerate(paths, 1):
        if file_sequence(event_path) != expected_sequence:
            raise XSyncError(f"事件序列不连续，期望 {expected_sequence}")
        event = load_json(event_path)
        if not isinstance(event, dict):
            raise XSyncError(f"事件不是 object: {event_path.name}")
        sequence = event.get("sequence")
        state_after = event.get("state_after")
        from_version = event.get("from_version")
        to_version = event.get("to_version")
        event_id = event.get("event_id")
        event_type = event.get("event_type")
        filename_event_id = event_path.stem.split("-", 1)[1]
        if (event.get("schema_version") != SCHEMA_VERSION
                or event.get("record_type") != "session_event"
                or not isinstance(event_id, str) or not ID_RE.fullmatch(event_id)
                or filename_event_id != event_id
                or not isinstance(event_type, str)
                or not isinstance(event.get("learner"), str)
                or not LEARNER_RE.fullmatch(event["learner"])
                or sequence != expected_sequence or event.get("session_id") != expected_session
                or not isinstance(state_after, dict)
                or state_after.get("session_id") != expected_session
                or from_version != previous_version or to_version != previous_version + 1
                or state_after.get("state_version") != to_version):
            raise XSyncError(f"事件链断裂或与会话不一致: {event_path.name}")
        parse_timestamp(str(event.get("occurred_at")))
        validate_materialized_state(state_after, expected_session, event_path.name)
        validate_event_payload(event_type, event.get("payload"), event_path.name)
        validate_event_transition(chain[-1] if chain else None, event, state_after,
                                  event_path.name)
        chain.append(event)
        previous_version = to_version
    return chain


def recover_state(directory: Path) -> dict:
    """Use the validated event chain as the authority for materialized state."""
    state = None
    with contextlib.suppress(XSyncError):
        candidate = load_json(directory / "state.json")
        if isinstance(candidate, dict):
            state = candidate
    chain = load_event_chain(directory)
    latest = chain[-1]["state_after"] if chain else None
    if latest is None:
        raise XSyncError("会话缺少可重放的事件链")
    if state is None:
        state = latest
        atomic_json(directory / "state.json", state)
    elif state != latest:
        state_version = state.get("state_version")
        if (isinstance(state_version, int) and not isinstance(state_version, bool)
                and state_version > latest.get("state_version", 0)):
            raise XSyncError("state.json 比事件链更新；拒绝静默回滚")
        state = latest
        atomic_json(directory / "state.json", state)
    if not isinstance(state, dict) or not isinstance(state.get("session_id"), str):
        raise XSyncError("会话状态已损坏")
    return state


def question_rubric_specs(question: dict) -> dict[str, dict]:
    """Return normalized criterion weights and their own grounding evidence."""
    return {
        item["id"]: {
            "weight": float(item.get("weight", item.get("points"))),
            "evidence_ids": set(item.get("evidence_ids", [])),
        }
        for item in question.get("answer", {}).get("rubric", [])
    }


def validate_question_rubric_results(
    question: dict, results: list, label: str, require_complete: bool
) -> float | None:
    """Bind every criterion score to its weight and criterion-local evidence."""
    specs = question_rubric_specs(question)
    seen = set()
    earned_total = 0.0
    for result in results:
        rubric_id = result["rubric_id"]
        spec = specs.get(rubric_id)
        if spec is None:
            raise XSyncError(f"{label} review rubric 不存在")
        result_evidence = result["evidence_ids"]
        expected_earned = float(result["score"]) * spec["weight"]
        if (not result_evidence
                or not set(result_evidence) <= spec["evidence_ids"]
                or not math.isclose(float(result["earned"]), expected_earned, abs_tol=1e-9)):
            raise XSyncError(f"{label} review rubric 分数/criterion 证据不一致")
        seen.add(rubric_id)
        earned_total += float(result["earned"])
    if require_complete and seen != set(specs):
        raise XSyncError(f"{label} 缺少完整 rubric 评分")
    if not require_complete:
        return None
    possible = sum(spec["weight"] for spec in specs.values())
    if possible <= 0:
        raise XSyncError(f"{label} rubric 总权重非法")
    return earned_total / possible


def validate_state_against_bank(state: dict, config: dict, bank: dict) -> None:
    """Bind persisted attempts/reviews to the immutable session bank."""
    questions = bank["questions"]
    if state.get("total") != len(questions):
        raise XSyncError("state.total 与 session bank 不一致")
    index = state["current_index"]
    expected_current = questions[index]["id"] if index < len(questions) else None
    if state.get("current_question_id") != expected_current:
        raise XSyncError("state.current_question_id 与 session bank 不一致")
    by_key = {(question["id"], question.get("version", 1)): question
              for question in questions}
    for attempt in state["attempts"]:
        key = (attempt["question_id"], attempt["question_version"])
        question = by_key.get(key)
        if question is None:
            raise XSyncError(f"attempt {attempt['attempt_id']} 引用未知题目版本")
        cited = set(question.get("evidence_ids", []))
        check_ids = attempt["evidence_check"].get("evidence_ids", [])
        if not set(check_ids) <= cited:
            raise XSyncError(f"attempt {attempt['attempt_id']} evidence_check 越界")
        if attempt["evidence_check"]["status"] == "fresh" and check_ids:
            raise XSyncError(f"attempt {attempt['attempt_id']} fresh evidence_check 不得列出 stale IDs")
        auto = attempt.get("auto_result")
        if auto is not None:
            if (question["type"] != "single_choice" or config.get("style") != "regular"
                    or auto["correct"] != (
                        attempt["response"] == question["answer"]["correct_choice"]
                    )):
                raise XSyncError(f"attempt {attempt['attempt_id']} auto_result 与答案键不一致")
        review = attempt.get("review")
        if not isinstance(review, dict):
            continue
        if not math.isclose(float(review["confidence"]), float(attempt["confidence"]), abs_tol=1e-12):
            raise XSyncError(f"attempt {attempt['attempt_id']} review confidence 不一致")
        if not set(review["evidence_ids"]) <= cited:
            raise XSyncError(f"attempt {attempt['attempt_id']} review evidence 越界")
        if question["type"] == "free_text":
            label = f"attempt {attempt['attempt_id']}"
            derived = validate_question_rubric_results(
                question, review["rubric_results"], label,
                require_complete=review["status"] == "graded",
            )
            if (review["status"] == "graded"
                    and not math.isclose(float(review["correctness"]), float(derived),
                                         abs_tol=1e-9)):
                raise XSyncError(f"{label} correctness 与 rubric 汇总分不一致")
        elif review["rubric_results"]:
            raise XSyncError(f"attempt {attempt['attempt_id']} 客观题不得携带 rubric 评分")
        objective = question["type"] == "single_choice" and review["status"] == "graded"
        calibration_present = review["brier_error"] is not None
        if objective != calibration_present:
            raise XSyncError(f"attempt {attempt['attempt_id']} 校准指标与题型不一致")
        if auto is not None:
            expected = float(auto["correct"])
            expected_outcome = "mastered" if auto["correct"] else "exhausted"
            if (review["correctness"] != expected or review["outcome"] != expected_outcome
                    or review["status"] != "graded"):
                raise XSyncError(f"attempt {attempt['attempt_id']} 自动复核与 auto_result 不一致")


def session_data(store: Store, session: str | None) -> tuple[str, Path, dict, dict, dict]:
    sid, directory = store.resolve_session(session)
    config = load_json(directory / "config.json")
    state = recover_state(directory)
    if not isinstance(config, dict) or not isinstance(state, dict):
        raise XSyncError("会话文件损坏")
    assert store.project
    snapshot = directory / "bank.json"
    bank_path = snapshot if snapshot.is_file() else safe_child(
        store.project, "banks", f"{config['bank_id']}.json"
    )
    bank = validate_bank(load_json(bank_path))
    bank["questions"] = bank["questions"][:int(config.get("count", len(bank["questions"])))]
    validate_state_against_bank(state, config, bank)
    return sid, directory, config, state, bank


def current_question(state: dict, bank: dict) -> dict | None:
    index = int(state["current_index"])
    return bank["questions"][index] if 0 <= index < len(bank["questions"]) else None


def public_question(question: dict | None, state: dict, config: dict) -> dict | None:
    if question is None:
        return None
    allowed = {key: question.get(key) for key in
               ("id", "domain", "depth", "type", "prompt", "choices")
               if key in question}
    allowed["topic"] = question["topics"][0]
    if isinstance(allowed.get("choices"), list):
        allowed["choices"] = [
            {"id": choice["id"], "text": choice["text"]}
            for choice in allowed["choices"]
        ]
    if config["style"] == "socratic":
        socratic = question.get("socratic", {})
        prior = [a for a in state["attempts"] if a["question_id"] == question["id"]]
        probe_index = max(0, len(prior) - 1)
        probes = socratic.get("probes", []) if isinstance(socratic, dict) else []
        if prior and probe_index < len(probes):
            allowed["probe"] = probes[probe_index]
    return allowed


def public_session(store: Store, session: str | None = None) -> dict:
    sid, _, config, state, bank = session_data(store, session)
    question = current_question(state, bank)
    attempts = [{k: a.get(k) for k in ("attempt_id", "question_id", "question_version",
                                        "response", "reason", "confidence", "saved_at",
                                        "auto_result", "review") if k in a}
                for a in state.get("attempts", [])]
    session_view = {**config, "status": state["status"],
                    "current_index": state["current_index"],
                    "current_question_id": state.get("current_question_id"),
                    "state_version": state["state_version"], "total": len(bank["questions"])}
    pending = next((a for a in reversed(attempts)
                    if a["question_id"] == state.get("current_question_id")
                    and "review" not in a), None)
    return {"schema_version": SCHEMA_VERSION, "session": session_view,
            "state": {**state, "attempts": attempts},
            "question": public_question(question, state, config),
            "pending_attempt": pending if state["status"] in {"answer_saved", "agent_review_pending"} else None,
            "progress": {"current": min(state["current_index"] + 1, len(bank["questions"])),
                         "total": len(bank["questions"])}, "session_id": sid}


def public_web_session(store: Store, session: str | None = None) -> dict:
    """Return the least-privilege view used by the browser quiz."""
    value = public_session(store, session)
    pending = value.get("pending_attempt")
    safe_pending = None
    if isinstance(pending, dict):
        safe_pending = {
            key: pending[key] for key in ("attempt_id", "question_id", "saved_at")
            if key in pending
        }
    return {
        "schema_version": value["schema_version"],
        "session_id": value["session_id"],
        "session": value["session"],
        "question": value["question"],
        "pending_attempt": safe_pending,
        "progress": value["progress"],
    }


def learner_status(store: Store) -> dict:
    """Return learner/profile status even before the first session exists."""
    assert store.project and store.learner
    profile_path = safe_child(store.root, "users", store.learner, "profile.json")
    profile = load_json(profile_path) if profile_path.is_file() else None
    mastery_path = store.project / "mastery.json"
    mastery = load_json(mastery_path) if mastery_path.is_file() else None
    summary = {
        "learner": store.learner, "repository": str(store.repo),
        "profile_exists": isinstance(profile, dict), "active_session": None,
        "repository_scan": repository_scan_status(store),
        "bank_ids": sorted(path.stem for path in (store.project / "banks").glob("*.json")),
        "reviewed_questions": len(mastery.get("reviews", [])) if isinstance(mastery, dict) else 0,
        "due_question_ids": mastery.get("due_question_ids", []) if isinstance(mastery, dict) else [],
    }
    active_path = store.project / "active.json"
    if active_path.is_file():
        active = public_session(store)
        summary["active_session"] = active
    return summary


def stale_review(attempt: dict, evidence_ids: list[str]) -> dict:
    return {
        "status": "stale", "correctness": None, "initial_correctness": None,
        "final_correctness": None, "reasoning": None, "evidence_use": None,
        "max_hint_level": 0, "unaided": False, "rubric_results": [],
        "confidence": float(attempt.get("confidence", 0.5)),
        "brier_error": None, "overconfidence": None,
        "evidence_ids": sorted(set(evidence_ids)),
        "explanation": "Repository evidence changed after this question was prepared; answer preserved but not scored.",
        "grader": {"name": "x-sync-runtime", "version": "0.1.0",
                   "prompt_version": "evidence-freshness-v1"},
        "outcome": None, "feedback": "证据已变化，本次答案保留但不计分。",
        "reviewer": {"name": "x-sync-runtime", "version": "0.1.0"},
        "reviewed_at": utc_now(),
    }


def mark_attempt_stale(store: Store, sid: str, directory: Path, question: dict,
                       attempt_id: str, evidence_ids: list[str]) -> dict:
    before = recover_state(directory)
    before_attempt = next((item for item in before.get("attempts", [])
                           if item.get("attempt_id") == attempt_id), None)
    if before_attempt is None:
        raise XSyncError("stale review 找不到 attempt")
    prepared_review = stale_review(before_attempt, evidence_ids)

    def transform(value: dict) -> dict:
        if value.get("current_question_id") != question["id"]:
            raise XSyncError("题目状态已经变化，无法标记 stale")
        target = next((item for item in value.get("attempts", [])
                       if item.get("attempt_id") == attempt_id), None)
        if target is None:
            raise XSyncError("stale review 找不到 attempt")
        if target.get("review", {}).get("status") == "stale":
            return value
        if "review" in target:
            raise XSyncError("attempt 已经完成复核")
        target["review"] = prepared_review
        target.pop("auto_result", None)
        value["status"] = "reviewed"
        return value

    store.mutate(directory, "answer_reviewed", {
        "attempt_id": attempt_id,
        "evaluation": prepared_review,
        "next_review_at": None,
    }, transform)
    rebuild_mastery(store)
    return public_session(store, sid)


def apply_auto_review(store: Store, sid: str, directory: Path, question: dict,
                      attempt_id: str) -> dict:
    state = recover_state(directory)
    attempt = next((item for item in state.get("attempts", [])
                    if item.get("attempt_id") == attempt_id), None)
    if attempt is None:
        raise XSyncError("自动判题找不到客观答案")
    correct = attempt.get("response") == question["answer"]["correct_choice"]
    auto_result = {"correct": correct, "selected": attempt.get("response")}
    correctness = float(correct)
    confidence = float(attempt.get("confidence", 0.5))
    outcome = "mastered" if correctness == 1 else "exhausted"
    review = {
        "status": "graded", "correctness": correctness,
        "initial_correctness": None, "final_correctness": None,
        "reasoning": None, "evidence_use": None, "max_hint_level": 0,
        "unaided": correctness == 1, "rubric_results": [],
        "confidence": confidence, "brier_error": (confidence - correctness) ** 2,
        "overconfidence": confidence - correctness,
        "evidence_ids": list(question.get("evidence_ids", [])),
        "explanation": str(question.get("answer", {}).get("explanation", "")),
        "grader": {"name": "x-sync-runtime", "version": "0.1.0",
                   "prompt_version": "deterministic-mcq-v1"},
        "outcome": outcome, "feedback": "选择题已按题库答案键确定性判定。",
        "reviewer": {"name": "x-sync-runtime", "version": "0.1.0"},
        "reviewed_at": utc_now(),
    }

    def transform(value: dict) -> dict:
        target = next((item for item in value.get("attempts", [])
                       if item.get("attempt_id") == attempt_id), None)
        if target is None:
            raise XSyncError("自动判题找不到 attempt")
        if "review" in target:
            return value
        if value.get("status") != "answer_saved":
            raise XSyncError("自动判题状态已经变化")
        target["auto_result"] = auto_result
        target["review"] = review
        value["status"] = "reviewed"
        return value

    store.mutate(directory, "answer_reviewed", {
        "attempt_id": attempt_id, "evaluation": review, "next_review_at": None,
    }, transform)
    rebuild_mastery(store)
    return public_session(store, sid)


def dispatch_saved_answer(store: Store, sid: str, directory: Path,
                          config: dict, bank: dict) -> dict:
    """Resume the durable post-submit step after a process interruption."""
    state = recover_state(directory)
    if state.get("status") != "answer_saved":
        return public_session(store, sid)
    question = current_question(state, bank)
    if question is None:
        raise XSyncError("answer_saved 状态缺少当前题目")
    attempt = next((item for item in reversed(state.get("attempts", []))
                    if item.get("question_id") == question["id"] and "review" not in item), None)
    if attempt is None:
        raise XSyncError("answer_saved 状态缺少待处理 attempt")
    evidence_check = attempt.get("evidence_check", {})
    if evidence_check.get("status") == "stale":
        stale_ids = evidence_check.get("evidence_ids", [])
        if not isinstance(stale_ids, list) or not stale_ids:
            raise XSyncError("attempt 的 stale evidence 记录已损坏")
        return mark_attempt_stale(
            store, sid, directory, question, attempt["attempt_id"], stale_ids
        )
    try:
        validate_question_evidence(store.repo, bank, question)
    except EvidenceStaleError as exc:
        return mark_attempt_stale(
            store, sid, directory, question, attempt["attempt_id"], exc.evidence_ids
        )
    if question["type"] == "single_choice" and config["style"] == "regular":
        return apply_auto_review(store, sid, directory, question, attempt["attempt_id"])
    return public_session(store, sid)


def submit_answer(store: Store, session: str | None, response: str,
                  attempt_id: str | None = None, expected_version: int | None = None,
                  confidence: float = 0.5, reason: str = "") -> dict:
    sid, directory, config, state, bank = session_data(store, session)
    q = current_question(state, bank)
    attempt_id = attempt_id or uuid.uuid4().hex
    validate_component(attempt_id, " attempt id", ID_RE)
    if not isinstance(response, str) or not response.strip():
        raise XSyncError("答案不能为空")
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool) or not 0 <= confidence <= 1:
        raise XSyncError("confidence 必须是 [0,1] 之间的数字")
    if not isinstance(reason, str):
        raise XSyncError("reason 必须是字符串")
    for previous in state.get("attempts", []):
        if previous["attempt_id"] == attempt_id:
            if (previous.get("response") == response and previous.get("reason", "") == reason
                    and previous.get("confidence") == float(confidence)):
                return dispatch_saved_answer(store, sid, directory, config, bank)
            raise XSyncError("attempt_id 已用于不同答案")
    if expected_version is not None and expected_version != state["state_version"]:
        raise XSyncError(f"状态版本冲突: 当前为 {state['state_version']}")
    if state["status"] != "question_open":
        raise XSyncError(f"当前状态 {state['status']} 不接受答案")
    if q is None:
        raise XSyncError("当前没有题目")
    if q["type"] == "single_choice":
        valid = {choice["id"] for choice in q["choices"]}
        if response not in valid | {"unknown"}:
            raise XSyncError(f"选择必须是: {', '.join(sorted(valid))} 或 unknown")
    stale_ids: list[str] = []
    evidence_checked_at = utc_now()
    submitted_at = utc_now()
    try:
        validate_question_evidence(store.repo, bank, q)
    except EvidenceStaleError as exc:
        stale_ids = exc.evidence_ids

    def transform(value: dict) -> dict:
        duplicate = next((a for a in value["attempts"] if a["attempt_id"] == attempt_id), None)
        if duplicate:
            if (duplicate.get("response") == response and duplicate.get("reason", "") == reason
                    and duplicate.get("confidence") == float(confidence)):
                return value
            raise XSyncError("attempt_id 已用于不同答案")
        if expected_version is not None and expected_version != value["state_version"]:
            raise XSyncError(f"状态版本冲突: 当前为 {value['state_version']}")
        if value["status"] != "question_open" or value["current_question_id"] != q["id"]:
            raise XSyncError("题目状态已经变化，请刷新后重试")
        attempt = {"attempt_id": attempt_id, "question_id": q["id"],
                   "question_version": q.get("version", 1), "response": response,
                   "reason": reason, "confidence": float(confidence), "saved_at": submitted_at,
                   "evidence_check": {
                       "status": "stale" if stale_ids else "fresh",
                       "evidence_ids": list(stale_ids), "checked_at": evidence_checked_at,
                   }}
        if q["type"] == "single_choice" and config["style"] == "regular" and not stale_ids:
            attempt["auto_result"] = {
                "correct": response == q["answer"]["correct_choice"],
                "selected": response,
            }
        value["attempts"].append(attempt)
        value["status"] = "answer_saved"
        return value

    store.mutate(directory, "answer_submitted", {"attempt_id": attempt_id,
                 "question_id": q["id"], "question_version": q.get("version", 1),
                 "answer": response, "reason": reason, "confidence": float(confidence),
                 "submitted_at": submitted_at,
                 "evidence_check": {"status": "stale" if stale_ids else "fresh",
                                    "evidence_ids": list(stale_ids),
                                    "checked_at": evidence_checked_at}}, transform)
    return dispatch_saved_answer(store, sid, directory, config, bank)


def pending_reviews(store: Store, session: str | None) -> dict:
    sid, directory, config, state, bank = session_data(store, session)
    if state["status"] == "answer_saved":
        dispatch_saved_answer(store, sid, directory, config, bank)
        sid, directory, config, state, bank = session_data(store, sid)
    result = []
    for attempt in state.get("attempts", []):
        if "review" in attempt:
            continue
        q = next(q for q in bank["questions"] if q["id"] == attempt["question_id"])
        if q["type"] == "free_text" or config["style"] == "socratic":
            try:
                validate_question_evidence(store.repo, bank, q)
            except EvidenceStaleError as exc:
                mark_attempt_stale(store, sid, directory, q, attempt["attempt_id"],
                                   exc.evidence_ids)
                state = recover_state(directory)
                continue
            result.append({
                "session_id": sid, "attempt_id": attempt["attempt_id"],
                "question_id": attempt["question_id"],
                "question_version": attempt.get("question_version", 1),
                "question": q, "response": attempt["response"],
                "reason": attempt.get("reason", ""),
                "confidence": attempt.get("confidence", 0.5),
                "saved_at": attempt.get("saved_at"), "style": config["style"],
            })
    if result and state["status"] == "answer_saved":
        attempt = result[-1]

        def transform(value: dict) -> dict:
            if value["status"] == "agent_review_pending":
                return value
            if value["status"] != "answer_saved":
                raise XSyncError("答案状态已经变化，请重试")
            value["status"] = "agent_review_pending"
            return value

        store.mutate(directory, "grading_started", {
            "attempt_id": attempt["attempt_id"],
            "idempotency_key": idempotency_key(
                "grading", sid, attempt["attempt_id"], attempt["question"]["id"],
                attempt["question_version"],
            ),
        }, transform)
    return {"pending": result}


def apply_review(store: Store, review_value: object, session: str | None = None) -> dict:
    if not isinstance(review_value, dict):
        raise XSyncError("review 必须为 object")
    sid = str(review_value.get("session_id") or session or "")
    if not sid:
        raise XSyncError("review 缺少 session_id")
    sid, directory, config, state, bank = session_data(store, sid)
    if state["status"] != "agent_review_pending":
        raise XSyncError(f"当前状态 {state['status']} 不等待 review")
    aid = review_value.get("attempt_id")
    qid = review_value.get("question_id")
    outcome = review_value.get("outcome")
    if outcome not in {"mastered", "probe", "exhausted", "disputed"}:
        raise XSyncError("outcome 必须为 mastered、probe、exhausted 或 disputed")
    attempt = next((a for a in state["attempts"] if a["attempt_id"] == aid), None)
    if not attempt or attempt["question_id"] != qid or "review" in attempt:
        raise XSyncError("review 未引用当前待复核 attempt/question")
    if qid != state["current_question_id"]:
        raise XSyncError("只能复核当前题目")
    q = next(q for q in bank["questions"] if q["id"] == qid)
    try:
        validate_question_evidence(store.repo, bank, q)
    except EvidenceStaleError as exc:
        return mark_attempt_stale(store, sid, directory, q, str(aid), exc.evidence_ids)
    rubric = question_rubric_specs(q)
    evaluation = review_value.get("evaluation", {})
    if not isinstance(evaluation, dict):
        raise XSyncError("evaluation 必须为 object")
    results = review_value.get("rubric_results", evaluation.get("rubric_results", []))
    if not isinstance(results, list):
        raise XSyncError("rubric_results 必须为 array")
    seen = set()
    sanitized_results = []
    for result in results:
        if not isinstance(result, dict):
            raise XSyncError("rubric_result 必须为 object")
        rid = result.get("rubric_id", result.get("criterion_id"))
        if rid not in rubric:
            raise XSyncError("rubric_result 引用了未知 rubric")
        weight = rubric[rid]["weight"]
        earned = result.get("earned")
        if earned is None and "score" in result:
            if isinstance(result["score"], bool) or result["score"] not in {0, 0.5, 1}:
                raise XSyncError("rubric_result score 必须为 0、0.5 或 1")
            earned = result["score"] * weight
        if (rid in seen or not isinstance(earned, (int, float))
                or isinstance(earned, bool) or not math.isfinite(earned)
                or not 0 <= earned <= weight):
            raise XSyncError("rubric_result 分数非法或重复")
        ratio = float(earned) / weight
        if not any(abs(ratio - allowed) < 0.000001 for allowed in (0, 0.5, 1)):
            raise XSyncError("rubric_result 只能给 0、半分或满分")
        result_evidence = result.get("evidence_ids", [])
        if (not isinstance(result_evidence, list)
                or not result_evidence
                or not all(isinstance(item, str) for item in result_evidence)
                or not set(result_evidence) <= rubric[rid]["evidence_ids"]):
            raise XSyncError("rubric_result evidence_ids 必须来自对应 criterion")
        justification = str(result.get("justification", result.get("reason", "")))
        if q["type"] == "free_text" and not justification.strip():
            raise XSyncError("主观题 rubric_result 必须说明 justification")
        sanitized_results.append({
            "rubric_id": rid, "earned": float(earned),
            "score": ratio, "evidence_ids": list(result_evidence),
            "justification": justification,
        })
        seen.add(rid)
    if q["type"] == "free_text" and outcome != "disputed" and seen != set(rubric):
        raise XSyncError("主观题复核必须逐项提交全部 rubric 结果")
    derived_correctness = None
    if q["type"] == "free_text":
        derived_correctness = validate_question_rubric_results(
            q, sanitized_results, "主观题复核", require_complete=outcome != "disputed"
        )
    scalar_fields = ("correctness", "initial_correctness", "final_correctness",
                     "reasoning", "evidence_use")
    clean_evaluation = {}
    for field in scalar_fields:
        value = evaluation.get(field)
        if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool)
                                  or not 0 <= value <= 1):
            raise XSyncError(f"evaluation.{field} 必须是 [0,1] 或 null")
        clean_evaluation[field] = value
    max_hint = evaluation.get("max_hint_level", review_value.get("max_hint_level", 0))
    if not isinstance(max_hint, int) or isinstance(max_hint, bool) or not 0 <= max_hint <= 4:
        raise XSyncError("max_hint_level 必须为 0..4")
    unaided = evaluation.get("unaided", review_value.get("unaided", False))
    if not isinstance(unaided, bool):
        raise XSyncError("unaided 必须为 boolean")
    evidence_ids = evaluation.get("evidence_ids", review_value.get("evidence_ids", []))
    if not isinstance(evidence_ids, list) or not all(isinstance(item, str) for item in evidence_ids):
        raise XSyncError("review evidence_ids 必须为字符串数组")
    if not evidence_ids:
        evidence_ids = sorted({evidence_id for item in sanitized_results
                               for evidence_id in item["evidence_ids"]})
    if not set(evidence_ids) <= set(q.get("evidence_ids", [])):
        raise XSyncError("review evidence_ids 超出题目证据范围")
    grader = evaluation.get("grader", review_value.get("reviewer", {"name": "host_agent"}))
    if not isinstance(grader, dict):
        raise XSyncError("evaluation.grader 必须为 object")
    grader = {"name": str(grader.get("name", grader.get("kind", "host_agent"))),
              "version": str(grader.get("version", "unknown")),
              "prompt_version": str(grader.get("prompt_version", "x-sync-review-v1"))}
    correctness = clean_evaluation.get("correctness")
    if q["type"] == "free_text" and outcome != "disputed":
        if (correctness is not None
                and not math.isclose(float(correctness), float(derived_correctness),
                                     abs_tol=1e-9)):
            raise XSyncError("evaluation.correctness 必须等于 rubric 汇总分")
        correctness = derived_correctness
    elif correctness is None and outcome in {"mastered", "exhausted"}:
        correctness = 1.0 if outcome == "mastered" else 0.0
    if outcome == "disputed":
        correctness = None
    confidence = float(attempt.get("confidence", 0.5))
    objective = q["type"] == "single_choice" and correctness is not None
    clean_evaluation.update({
        "status": "disputed" if outcome == "disputed" else (
            "graded" if correctness is not None else "unscored"
        ),
        "correctness": correctness, "max_hint_level": max_hint, "unaided": unaided,
        "rubric_results": sanitized_results, "confidence": confidence,
        "brier_error": (confidence - correctness) ** 2 if objective else None,
        "overconfidence": confidence - correctness if objective else None,
        "evidence_ids": evidence_ids,
        "explanation": str(evaluation.get("explanation",
                                           review_value.get("feedback", ""))),
        "grader": grader, "outcome": outcome,
    })
    if outcome == "mastered" and (correctness is None or correctness < 0.8):
        raise XSyncError("outcome=mastered 要求 correctness >= 0.8")
    if outcome == "exhausted" and correctness is not None and correctness >= 0.8:
        raise XSyncError("高正确度答案不能标记为 exhausted")
    if unaided and (max_hint != 0 or correctness is None or correctness < 0.8):
        raise XSyncError("unaided=true 只适用于 H0 且已掌握的答案")
    prior_attempts = [item for item in state["attempts"] if item["question_id"] == qid]
    maximum = int(q.get("socratic", {}).get("max_attempts", 1))
    if outcome == "probe" and config["style"] != "socratic":
        raise XSyncError("regular 模式不能使用 outcome=probe")
    if outcome == "probe" and len(prior_attempts) >= maximum:
        raise XSyncError("已达到 Socratic max_attempts，不能继续 probe")
    probes = q.get("socratic", {}).get("probes", [])
    if outcome == "probe" and len(prior_attempts) - 1 >= len(probes):
        raise XSyncError("没有下一条已准备的 Socratic probe")
    if config["style"] == "socratic" and q["type"] == "free_text" and correctness is not None:
        initial = clean_evaluation.get("initial_correctness")
        final = clean_evaluation.get("final_correctness")
        if initial is None or final is None:
            raise XSyncError("Socratic 复核必须分别记录 initial/final correctness")
    sanitized = {**clean_evaluation,
                 "feedback": str(review_value.get("feedback", "")),
                 "reviewer": review_value.get("reviewer", {"kind": "host_agent"}),
                 "reviewed_at": utc_now()}

    def transform(value: dict) -> dict:
        if value["status"] != "agent_review_pending" or value["current_question_id"] != qid:
            raise XSyncError("复核状态已经变化，请重新读取 pending")
        target = next(a for a in value["attempts"] if a["attempt_id"] == aid)
        if "review" in target:
            return value
        target["review"] = sanitized
        value["status"] = "reviewed"
        return value

    store.mutate(directory, "answer_reviewed", {"attempt_id": aid, "outcome": outcome,
                 "evaluation": sanitized, "next_review_at": None}, transform)
    rebuild_mastery(store)
    return public_session(store, sid)


def continue_session(store: Store, session: str | None) -> dict:
    sid, directory, config, state, bank = session_data(store, session)
    if state["status"] == "answer_saved":
        dispatch_saved_answer(store, sid, directory, config, bank)
        sid, directory, config, state, bank = session_data(store, sid)
    if state["status"] == "agent_review_pending":
        raise XSyncError("仍有答案等待 host agent 复核；先运行 pending 和 review apply")
    if state["status"] == "completed":
        return public_session(store, sid)
    if state["status"] == "question_open":
        chain = load_event_chain(directory)
        last_type = chain[-1].get("event_type") if chain else None
        if last_type in {"question_presented", "socratic_turn"}:
            return public_session(store, sid)
        raise XSyncError("当前题目尚未作答，不能继续")
    if state["status"] == "answer_saved":
        raise XSyncError("语义答案已保存；请先运行 pending 和 review apply")
    if state["status"] != "reviewed":
        raise XSyncError(f"当前状态 {state['status']} 不能继续")
    latest = next(a for a in reversed(state["attempts"])
                  if a["question_id"] == state["current_question_id"])
    review = latest.get("review")
    probe = config["style"] == "socratic" and review and review["outcome"] == "probe"
    q = current_question(state, bank)
    if q is None:
        raise XSyncError("reviewed 状态缺少当前题目")
    continue_key = idempotency_key(
        "continue", sid, latest["attempt_id"], q["id"], q.get("version", 1)
    )
    probe_text = None
    turn = None
    if probe:
        socratic = q.get("socratic", {}) if q else {}
        count = sum(a["question_id"] == q["id"] for a in state["attempts"])
        maximum = int(socratic.get("max_attempts", 3)) if isinstance(socratic, dict) else 3
        probes = socratic.get("probes", []) if isinstance(socratic, dict) else []
        probe_index = max(0, count - 1)
        if count >= maximum or probe_index >= len(probes):
            probe = False
        else:
            probe_text = probes[probe_index]
            turn = count

    next_question = None
    if not probe and state["current_index"] + 1 < len(bank["questions"]):
        next_question = bank["questions"][state["current_index"] + 1]
        validate_question_evidence(store.repo, bank, next_question)

    def transform(value: dict) -> dict:
        consumed = value.get("consumed_continue_keys", [])
        if continue_key in consumed:
            return value
        if (value["status"] != "reviewed"
                or value.get("current_question_id") != q["id"]):
            raise XSyncError("会话状态已经变化，请刷新")
        current_attempt = next((item for item in reversed(value.get("attempts", []))
                                if item.get("question_id") == q["id"]), None)
        if current_attempt is None or current_attempt.get("attempt_id") != latest["attempt_id"]:
            raise XSyncError("会话 attempt 已经变化，请刷新")
        value["consumed_continue_keys"] = [*consumed, continue_key]
        if probe:
            value["status"] = "question_open"
            return value
        index = value["current_index"] + 1
        value["current_index"] = index
        if index >= len(bank["questions"]):
            value["current_question_id"] = None
            value["status"] = "completed"
            value["completed_at"] = utc_now()
        else:
            value["current_question_id"] = bank["questions"][index]["id"]
            value["status"] = "question_open"
        return value

    if probe:
        event_type = "socratic_turn"
        payload = {"attempt_id": latest["attempt_id"], "turn": turn,
                   "speaker": "tutor", "kind": "probe", "text": probe_text,
                   "idempotency_key": continue_key}
    elif next_question is None:
        event_type = "session_ended"
        payload = {
            "reason": "all_selected_questions_reviewed",
            "assessed_scope": {
                "task_scope": config.get("task_scope"), "focus": config.get("focus"),
                "max_depth": config.get("max_depth"),
                "topics": config.get("focus_topics", []), "question_count": len(bank["questions"]),
            },
            "idempotency_key": continue_key,
        }
    else:
        event_type = "question_presented"
        payload = {"id": next_question["id"],
                   "version": next_question.get("version", 1),
                   "evidence_validated_at": utc_now(),
                   "idempotency_key": continue_key}
    store.mutate(directory, event_type, payload, transform)
    return public_session(store, sid)


def parse_timestamp(value: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise XSyncError(f"时间格式非法: {value!r}") from exc
    if parsed.tzinfo is None:
        raise XSyncError(f"时间必须包含时区: {value!r}")
    return parsed


def question_dimension(question: dict) -> str:
    if question["domain"] == "business":
        return "business"
    words = " ".join(str(topic).lower() for topic in question.get("topics", []))
    if any(word in words for word in ("architecture", "data-flow", "dataflow", "架构", "数据流")):
        return "architecture_data_flow"
    if any(word in words for word in ("bug", "incident", "decision", "commit", "history", "故障", "决策")):
        return "decisions_bugs"
    if any(word in words for word in (
        "performance", "transaction", "consistency", "availability", "security",
        "observability", "network", "os", "docker", "性能", "事务", "一致性", "安全",
    )):
        return "non_functional"
    return "technical_mechanisms"


def attempt_grade(question: dict, config: dict, attempt: dict) -> tuple[float | None, bool, str]:
    review = attempt.get("review")
    if isinstance(review, dict):
        outcome = review.get("outcome")
        status = review.get("status")
        if status == "unscored":
            return None, False, "unscored"
        if status in {"disputed", "stale"}:
            return None, False, str(outcome or status)
        evaluation = review.get("evaluation", review)
        correctness = evaluation.get("correctness") if isinstance(evaluation, dict) else None
        if not isinstance(correctness, (int, float)):
            if outcome == "mastered":
                correctness = 1.0
            else:
                results = review.get("rubric_results", [])
                possible = sum(float(item.get("points", item.get("weight", 0)))
                               for item in question.get("answer", {}).get("rubric", []))
                earned = sum(float(item.get("earned", 0)) for item in results)
                correctness = earned / possible if possible else 0.0
        max_hint = review.get("max_hint_level", evaluation.get("max_hint_level", 0)
                              if isinstance(evaluation, dict) else 0)
        unaided = bool(review.get("unaided", evaluation.get("unaided", False)
                                 if isinstance(evaluation, dict) else False))
        if config.get("style") == "regular" and outcome == "mastered" and max_hint == 0:
            unaided = True
        return float(correctness), unaided, str(outcome)
    auto = attempt.get("auto_result")
    if isinstance(auto, dict) and isinstance(auto.get("correct"), bool):
        return float(auto["correct"]), True, "mastered" if auto["correct"] else "exhausted"
    return None, False, "unreviewed"


def rebuild_mastery_unlocked(store: Store) -> dict:
    """Materialize cross-session review state while the project lock is held."""
    assert store.project and store.learner
    observations = []
    sessions_dir = safe_child(store.project, "sessions")
    for directory in sorted(sessions_dir.glob("*")) if sessions_dir.is_dir() else []:
        if not directory.is_dir() or directory.is_symlink():
            continue
        try:
            config = load_json(directory / "config.json")
            state = recover_state(directory)
            bank = validate_bank(load_json(directory / "bank.json"))
        except XSyncError:
            continue
        if not isinstance(config, dict) or not isinstance(state, dict):
            continue
        questions = {(item["id"], item.get("version", 1)): item for item in bank["questions"]}
        for attempt in state.get("attempts", []):
            if not isinstance(attempt, dict):
                continue
            key = (attempt.get("question_id"), attempt.get("question_version", 1))
            question = questions.get(key)
            if question is None:
                continue
            correctness, unaided, outcome = attempt_grade(question, config, attempt)
            if correctness is None and outcome in {"unreviewed", "unscored"}:
                continue
            observations.append((attempt.get("saved_at", config.get("created_at", utc_now())),
                                 directory.name, config, question, attempt,
                                 correctness, unaided, outcome))
    observations.sort(key=lambda item: (str(item[0]), item[1], str(item[4].get("attempt_id"))))
    current: dict[tuple[str, int], dict] = {}
    history = []
    intervals = [1, 3, 7, 14, 30]
    for timestamp, session_id, config, question, attempt, correctness, unaided, outcome in observations:
        moment = parse_timestamp(str(timestamp))
        logical = (question["id"], int(question.get("version", 1)))
        previous = current.get(logical)
        delayed_successes = int(previous.get("successful_delayed_retrievals", 0)) if previous else 0
        prior_due = parse_timestamp(previous["due_at"]) if previous and previous.get("due_at") else None
        delayed = bool(prior_due and moment >= prior_due)
        confidence = float(attempt.get("confidence", 0.5))
        priority = ["task_relevant"]
        if outcome == "disputed":
            stage, result, interval, due = "disputed", "disputed", 0, None
        elif outcome == "stale":
            stage, result, interval, due = "stale", "stale", 0, None
        elif correctness is not None and correctness >= 0.8:
            if unaided and delayed:
                delayed_successes += 1
            result = "unaided_correct" if unaided else "aided_correct"
            interval = intervals[min(delayed_successes, len(intervals) - 1)]
            due = (moment + dt.timedelta(days=interval)).isoformat()
            stage = "stable" if delayed_successes >= 3 else "reviewing"
            if confidence < 0.5:
                priority.append("low_confidence_correct")
        else:
            stage, result, interval = "learning", "incorrect", 1
            due = (moment + dt.timedelta(days=interval)).isoformat()
            if correctness is not None and confidence - correctness >= 0.5:
                priority.append("high_confidence_error")
        review_id = "review." + hashlib.sha256(
            f"{session_id}:{attempt.get('attempt_id')}".encode()
        ).hexdigest()[:20]
        record = {
            "schema_version": 1, "record_type": "review", "id": review_id,
            "learner": store.learner, "session_id": session_id,
            "question_id": question["id"], "question_version": int(question.get("version", 1)),
            "topic_ids": list(question.get("topics", [question.get("topic", "未分类")])),
            "domain": question["domain"], "dimension": question_dimension(question),
            "depth": question["depth"], "stage": stage,
            "last_attempt_id": attempt.get("attempt_id"), "last_result": result,
            "correctness": correctness, "confidence": confidence, "unaided": unaided,
            "brier_error": (confidence - correctness) ** 2 if correctness is not None else None,
            "successful_delayed_retrievals": delayed_successes,
            "interval_days": interval, "due_at": due,
            "priority_reasons": priority, "updated_at": moment.isoformat(),
        }
        if previous:
            record["supersedes_id"] = previous["id"]
        current[logical] = record
        history.append(record)
    now = dt.datetime.now(dt.timezone.utc)
    reviews = sorted(current.values(), key=lambda item: (item["question_id"], item["question_version"]))
    mastery = {
        "schema_version": 1, "learner": store.learner, "repo_id": repo_id(store.repo),
        "updated_at": now.replace(microsecond=0).isoformat(), "reviews": reviews,
        "history": history,
        "due_question_ids": [item["question_id"] for item in reviews
                             if item.get("due_at") and parse_timestamp(item["due_at"]) <= now],
    }
    atomic_json(store.project / "mastery.json", mastery)
    profile_path = safe_child(store.root, "users", store.learner, "profile.json")
    profile = load_json(profile_path) if profile_path.is_file() else {
        "schema_version": 1, "learner": store.learner, "created_at": utc_now()
    }
    if not isinstance(profile, dict):
        raise XSyncError("profile.json 已损坏")
    projects = profile.setdefault("projects", {})
    projects[repo_id(store.repo)] = {
        "repo": str(store.repo), "mastery_path": str(store.project / "mastery.json"),
        "reviewed_questions": len(reviews), "due_reviews": len(mastery["due_question_ids"]),
        "updated_at": mastery["updated_at"],
    }
    atomic_json(profile_path, profile)
    return mastery


def rebuild_mastery(store: Store) -> dict:
    with store.lock():
        return rebuild_mastery_unlocked(store)


def build_report_data(store: Store, session: str | None) -> dict:
    sid, directory, config, state, bank = session_data(store, session)
    mastery = rebuild_mastery(store)
    dimension_names = ("business", "architecture_data_flow", "technical_mechanisms",
                       "decisions_bugs", "non_functional")
    dimensions: dict[str, dict] = {
        name: {"attempts": 0, "scored": 0, "correctness_sum": 0.0,
               "unaided_successes": 0, "hinted_successes": 0, "topics": []}
        for name in dimension_names
    }
    brier = []
    for attempt in state["attempts"]:
        q = next(q for q in bank["questions"] if q["id"] == attempt["question_id"])
        key = question_dimension(q)
        item = dimensions[key]
        item["attempts"] += 1
        item["topics"] = sorted(set(item["topics"]) | set(q.get("topics", [q.get("topic", "未分类")])))
        correctness, unaided, _ = attempt_grade(q, config, attempt)
        if correctness is not None:
            item["scored"] += 1
            item["correctness_sum"] += correctness
            item["unaided_successes"] += int(unaided and correctness >= 0.8)
            item["hinted_successes"] += int(not unaided and correctness >= 0.8)
            if "auto_result" in attempt:
                brier.append((float(attempt.get("confidence", 0.5)) - correctness) ** 2)
    for item in dimensions.values():
        item["mean_correctness"] = (
            item["correctness_sum"] / item["scored"] if item["scored"] else None
        )
        del item["correctness_sum"]
    evidence_kinds: dict[str, int] = {}
    stale_evidence = []
    for evidence in bank.get("evidence", []):
        kind = str(evidence.get("kind", "unknown"))
        evidence_kinds[kind] = evidence_kinds.get(kind, 0) + 1
        try:
            validate_evidence_freshness(store.repo, bank, {str(evidence.get("id"))})
        except EvidenceStaleError as exc:
            stale_evidence.extend(exc.evidence_ids)
    validations = [question.get("validation", {}) for question in bank["questions"]]
    repository_evidence_kinds = {"spec", "adr", "commit", "bug", "test", "code", "config"}
    delayed_retrievals = sum(
        int(item.get("successful_delayed_retrievals", 0)) for item in mastery["reviews"]
    )
    return {
        "schema_version": 1,
        "scope": {"learner": config["learner"], "repository": str(store.repo),
                  "repo_id": config["repo_id"], "baseline_commit": config.get("baseline_commit"),
                  "bank_id": config["bank_id"], "session_id": sid,
                  "style": config["style"], "task_scope": config.get("task_scope"),
                  "focus": config.get("focus", "mixed"),
                  "max_depth": config.get("max_depth"),
                  "topics": config.get("focus_topics", [])},
        "session": {"status": state["status"], "questions_sampled": len(bank["questions"]),
                    "attempts": len(state["attempts"])},
        "human_repository_profile": dimensions,
        "retention": {"reviewed_questions": len(mastery["reviews"]),
                      "due_question_ids": mastery["due_question_ids"],
                      "stable_questions": sum(item["stage"] == "stable"
                                              for item in mastery["reviews"]),
                      "successful_delayed_retrievals": delayed_retrievals},
        "confidence_calibration": {"objective_attempts": len(brier),
                                   "mean_brier_error": sum(brier) / len(brier) if brier else None},
        "model_repository_profile": {
            "questions_with_evidence": sum(bool(q.get("evidence_ids")) for q in bank["questions"]),
            "questions_sampled": len(bank["questions"]), "evidence_items": len(bank.get("evidence", [])),
            "evidence_kinds": evidence_kinds,
            "grounded_questions": sum(item.get("grounded") is True for item in validations),
            "low_ambiguity_questions": sum(item.get("ambiguity") == "low" for item in validations),
            "stale_evidence_ids": sorted(set(stale_evidence)),
            "conflict_evidence_items": evidence_kinds.get("conflict", 0),
            "inference_evidence_items": evidence_kinds.get("inference", 0),
        },
        "repository_knowledge_profile": {
            "business_questions": sum(q["domain"] == "business" for q in bank["questions"]),
            "technical_questions": sum(q["domain"] == "technical" for q in bank["questions"]),
            "dirty_worktree_at_generation": bool(bank.get("working_tree", {}).get("dirty", False)),
            "repository_evidence_items": sum(
                count for kind, count in evidence_kinds.items() if kind in repository_evidence_kinds
            ),
            "source_coverage": {kind: evidence_kinds.get(kind, 0)
                                for kind in sorted(repository_evidence_kinds)},
            "document_implementation_conflicts": evidence_kinds.get("conflict", 0),
        },
        "limitations": [
            "This profile applies only to the sampled questions, repository revision, and task scope.",
            "It is not an employee ranking or a universal repository-understanding score.",
        ],
    }


def markdown_inline(value: object) -> str:
    """Render untrusted learner/bank text without active Markdown or HTML."""
    visible = []
    for character in str(value):
        if character == "\t":
            visible.append(" ")
        elif unicodedata.category(character) in {"Cc", "Cf", "Cs"}:
            visible.append(f"\\u{ord(character):04X}")
        else:
            visible.append(character)
    normalized = " ".join("".join(visible).replace("\r", "\n").splitlines())
    return re.sub(r"([\\`*_{}\[\]()#+.!|<>~\-])", r"\\\1", normalized)


def make_report(store: Store, session: str | None, write: bool = True) -> str:
    sid, directory, config, state, bank = session_data(store, session)
    report = build_report_data(store, sid)
    labels = {
        "business": "业务理解", "architecture_data_flow": "架构与数据流",
        "technical_mechanisms": "技术机制", "decisions_bugs": "决策、事故与 Bug",
        "non_functional": "非功能性要求",
    }
    lines = [f"# x-sync 学习报告：{markdown_inline(sid)}", "",
             f"- 学习者：{markdown_inline(config['learner'])}",
             f"- 仓库版本：{markdown_inline(config.get('baseline_commit') or 'unknown')}",
             f"- 任务范围：{markdown_inline(config.get('task_scope') or '未指定')}",
             f"- 模式：{markdown_inline(config['style'])}",
             f"- 状态：{markdown_inline(state['status'])}", "",
             "## 人与仓库的分维度画像", ""]
    dimensions = report["human_repository_profile"]
    if not dimensions:
        lines.append("尚无作答记录。")
    for key, item in sorted(dimensions.items()):
        lines.extend([f"### {labels.get(key, key)}", "", f"- 作答次数：{item['attempts']}",
                      f"- 已评分：{item['scored']}",
                      f"- 平均正确度：{item['mean_correctness']:.0%}"
                      if item["mean_correctness"] is not None else "- 平均正确度：证据不足",
                      f"- 无提示成功：{item['unaided_successes']}",
                      f"- 提示后成功：{item['hinted_successes']}",
                      f"- 主题：{', '.join(markdown_inline(topic) for topic in item['topics'])}"])
        lines.append("")
    calibration = report["confidence_calibration"]
    lines.extend(["## 保持与信心校准", "",
                  f"- 已建立复习状态的知识点：{report['retention']['reviewed_questions']}",
                  f"- 当前到期复习：{len(report['retention']['due_question_ids'])}",
                  f"- 延迟提取成功：{report['retention']['successful_delayed_retrievals']}",
                  f"- 客观题 Brier error：{calibration['mean_brier_error']:.3f}"
                  if calibration["mean_brier_error"] is not None else "- 客观题 Brier error：样本不足", "",
                  "## Agent 与仓库证据", "",
                  f"- 有证据的问题：{report['model_repository_profile']['questions_with_evidence']}/"
                  f"{report['model_repository_profile']['questions_sampled']}",
                  f"- 低歧义题目：{report['model_repository_profile']['low_ambiguity_questions']}/"
                  f"{report['model_repository_profile']['questions_sampled']}",
                  f"- 证据条目：{report['model_repository_profile']['evidence_items']}",
                  f"- 当前过期证据：{len(report['model_repository_profile']['stale_evidence_ids'])}",
                  f"- 已记录证据冲突：{report['model_repository_profile']['conflict_evidence_items']}", ""])
    lines.extend(["## 逐题记录", ""])
    for attempt in state["attempts"]:
        q = next(q for q in bank["questions"] if q["id"] == attempt["question_id"])
        lines.append(
            f"- **{markdown_inline(q['id'])}**"
            f"（{markdown_inline(q['domain'])}，深度 {q['depth']}）："
            f"{markdown_inline(attempt['response'])}"
        )
        if "auto_result" in attempt:
            lines.append(f"  - 选择题结果：{'正确' if attempt['auto_result']['correct'] else '错误'}")
        if "review" in attempt:
            lines.append(
                f"  - Host 复核：{markdown_inline(attempt['review']['outcome'])}；"
                f"{markdown_inline(attempt['review']['feedback'])}"
            )
    lines.extend(["", "## 解释边界", "",
                  "这份画像只适用于本次抽样题目、仓库版本与任务范围；它不是员工排名，也不是对整个仓库理解程度的单一总分。"])
    text = "\n".join(lines) + "\n"
    if write:
        atomic_write(directory / "reports" / "report.md", text.encode("utf-8"))
    return text


class QuizHandler(http.server.BaseHTTPRequestHandler):
    store: Store
    session_id: str
    token: str
    html: bytes
    max_body = 64 * 1024

    def log_message(self, fmt: str, *args) -> None:
        return

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Content-Security-Policy", "default-src 'self' 'unsafe-inline'; connect-src 'self'")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, value: object) -> None:
        self._send(status, json.dumps(value, ensure_ascii=False).encode(),
                   "application/json; charset=utf-8")

    def _problem(self, status: int, message: str) -> None:
        self._json(status, {"error": message, "message": message})

    def _authorized(self) -> bool:
        supplied = self.headers.get("Authorization", "")
        return secrets.compare_digest(supplied, "Bearer " + self.token)

    def _host_ok(self) -> bool:
        raw = self.headers.get("Host", "").lower()
        if raw.startswith("["):
            host = raw[1:].split("]", 1)[0]
        else:
            host = raw.rsplit(":", 1)[0] if raw.count(":") == 1 else raw
        return host in {"127.0.0.1", "localhost", "::1"}

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if not self._host_ok():
            self._problem(403, "invalid Host")
        elif path == "/":
            self._send(200, self.html, "text/html; charset=utf-8")
        elif path in {"/api/state", "/api/v1/state"}:
            if not self._authorized():
                self._problem(401, "unauthorized")
            else:
                try:
                    self._json(200, public_web_session(self.store, self.session_id))
                except XSyncError as exc:
                    self._problem(409, str(exc))
        else:
            self._problem(404, "not found")

    def do_POST(self) -> None:
        if not self._host_ok() or not self._authorized():
            self._problem(401, "unauthorized")
            return
        if self.headers.get("Content-Type", "").split(";", 1)[0] != "application/json":
            self._problem(415, "Content-Type must be application/json")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length < 0 or length > self.max_body:
            self._problem(413, "request too large")
            return
        try:
            data = json.loads(self.rfile.read(length))
            if self.path in {"/api/answer", "/api/v1/answer"}:
                submit_answer(self.store, self.session_id,
                              data.get("answer", data.get("response")),
                              data.get("attempt_id"), data.get("state_version"),
                              data.get("confidence", 0.5), data.get("reason", ""))
                result = public_web_session(self.store, self.session_id)
            else:
                self._problem(404, "not found")
                return
            self._json(200, result)
        except (XSyncError, json.JSONDecodeError, AttributeError) as exc:
            self._problem(409, str(exc))


def serve(store: Store, session: str | None, host: str, port: int, open_page: bool) -> None:
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise XSyncError("serve 仅允许绑定 loopback 地址")
    sid, _, config, _, _ = session_data(store, session)
    if config.get("channel") != "web":
        raise XSyncError("该会话配置为 terminal；请在 Agent 终端继续或明确开始新的 web 会话")
    html_path = Path(__file__).resolve().parent.parent / "assets" / "quiz.html"
    if not html_path.is_file():
        raise XSyncError(f"HTML 资源缺失: {html_path}；请重新安装完整 x-sync skill")
    handler = type("BoundQuizHandler", (QuizHandler,), {
        "store": store, "session_id": sid, "token": secrets.token_urlsafe(32),
        "html": html_path.read_bytes(),
    })
    server_class = http.server.ThreadingHTTPServer
    if host == "::1":
        server_class = type("IPv6ThreadingHTTPServer", (http.server.ThreadingHTTPServer,),
                            {"address_family": socket.AF_INET6})
    server = server_class((host, port), handler)
    actual_port = server.server_address[1]
    shown_host = "127.0.0.1" if host == "localhost" else (f"[{host}]" if ":" in host else host)
    url = f"http://{shown_host}:{actual_port}/#token={handler.token}"
    print(f"x-sync 页面: {url}", flush=True)
    print("答案会先落盘；回到 Agent 终端输入“继续”进行复核。", flush=True)
    if open_page:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def print_question(value: dict) -> None:
    question = value["question"]
    if question is None:
        print("会话已无当前题目。")
        return
    print(f"[{value['progress']['current']}/{value['progress']['total']}] "
          f"{question['domain']} · 深度 {question['depth']}")
    if question.get("probe"):
        print(f"追问：{question['probe']}")
    print(question["prompt"])
    for choice in question.get("choices", []):
        print(f"  {choice['id']}. {choice['text']}")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="xsync", description="x-sync offline runtime")
    root.add_argument("--version", action="version", version="x-sync runtime 0.1.0")
    sub = root.add_subparsers(dest="command", required=True)

    def repo_arg(p):
        p.add_argument("--repo", default=".")

    def json_arg(p):
        p.add_argument("--json", action="store_true", help=argparse.SUPPRESS)

    def learner_args(p, session=True):
        repo_arg(p)
        p.add_argument("--learner", required=True)
        if session:
            p.add_argument("--session")

    doctor = sub.add_parser("doctor"); repo_arg(doctor); json_arg(doctor)
    init = sub.add_parser("init"); learner_args(init, False); json_arg(init)
    scan = sub.add_parser("scan"); repo_arg(scan); json_arg(scan)
    evidence = sub.add_parser("evidence"); evsub = evidence.add_subparsers(dest="evidence_command", required=True)
    snapshot = evsub.add_parser("snapshot"); repo_arg(snapshot); snapshot.add_argument("--paths", nargs="*")
    snapshot.add_argument("--kind"); snapshot.add_argument("--path"); snapshot.add_argument("--lines")
    snapshot.add_argument("--summary"); snapshot.add_argument("--commit")
    snapshot.add_argument("--claim-type"); json_arg(snapshot)
    bank = sub.add_parser("bank"); bsub = bank.add_subparsers(dest="bank_command", required=True)
    validate = bsub.add_parser("validate"); repo_arg(validate); validate.add_argument("--file", required=True); json_arg(validate)
    install = bsub.add_parser("install"); learner_args(install, False); install.add_argument("--file", required=True); json_arg(install)
    start = sub.add_parser("start"); learner_args(start, False); start.add_argument("--bank", required=True)
    start.add_argument("--style", choices=["regular", "socratic"], default=DEFAULT_STYLE)
    start.add_argument("--channel", choices=["terminal", "web"], default=DEFAULT_CHANNEL)
    start.add_argument("--focus", choices=["business", "technical", "mixed"], default=DEFAULT_FOCUS)
    start.add_argument("--max-depth", type=int); start.add_argument("--task")
    start.add_argument("--count", type=int, default=DEFAULT_QUESTION_COUNT); json_arg(start)
    question = sub.add_parser("question"); learner_args(question); json_arg(question)
    answer = sub.add_parser("answer"); learner_args(answer); answer.add_argument("--choice"); answer.add_argument("--text")
    answer.add_argument("--attempt-id"); answer.add_argument("--state-version", type=int)
    answer.add_argument("--confidence", type=float, required=True); answer.add_argument("--reason", default=""); json_arg(answer)
    pending = sub.add_parser("pending"); learner_args(pending); json_arg(pending)
    review = sub.add_parser("review"); rsub = review.add_subparsers(dest="review_command", required=True)
    apply = rsub.add_parser("apply"); learner_args(apply); apply.add_argument("--file", required=True); json_arg(apply)
    cont = sub.add_parser("continue"); learner_args(cont); json_arg(cont)
    status = sub.add_parser("status"); learner_args(status); json_arg(status)
    report = sub.add_parser("report"); learner_args(report); report.add_argument("--output")
    report.add_argument("--format", choices=["md", "json"], default="md"); json_arg(report)
    server = sub.add_parser("serve"); learner_args(server); server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=0); server.add_argument("--open", action="store_true"); json_arg(server)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "bank" and args.bank_command == "validate":
            repo = find_repo(args.repo)
            bank = validate_bank(load_json(Path(args.file)))
            validate_bank_for_repo(repo, bank)
            validate_evidence_freshness(repo, bank)
            print(json.dumps({"valid": True, "bank_id": bank["bank_id"],
                              "questions": len(bank["questions"])}, ensure_ascii=False))
            return 0
        repo = find_repo(args.repo)
        learner = getattr(args, "learner", None)
        store = Store(repo, learner)
        if args.command == "doctor":
            result = {"ok": True, "python": sys.version.split()[0], "repo": str(repo),
                      "repo_id": repo_id(repo),
                      "default_learner": default_learner(),
                      "head_commit": git(repo, "rev-parse", "HEAD", check=False) or None,
                      "working_tree": working_tree_state(repo),
                      "repository_scan": repository_scan_status(store),
                      "git": bool(shutil.which("git")), "stdlib_only": True}
        elif args.command == "init":
            result = init_profile(store)
        elif args.command == "scan":
            result = scan_repository(store)
        elif args.command == "evidence":
            result = snapshot_evidence(repo, store, args.paths, args.kind, args.path,
                                       args.lines, args.summary, args.commit, args.claim_type)
        elif args.command == "bank":
            result = install_bank(store, Path(args.file))
        elif args.command == "start":
            init_profile(store)
            result = start_session(store, args.bank, args.style, args.channel, args.count,
                                   args.focus, args.max_depth, args.task)
        elif args.command == "question":
            result = public_session(store, args.session)
            if args.json:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                print_question(result)
            return 0
        elif args.command == "answer":
            if bool(args.choice) == bool(args.text):
                raise XSyncError("answer 必须且只能指定 --choice 或 --text")
            result = submit_answer(store, args.session, args.choice or args.text,
                                   args.attempt_id, args.state_version,
                                   args.confidence, args.reason)
        elif args.command == "pending":
            result = pending_reviews(store, args.session)
        elif args.command == "review":
            result = apply_review(store, load_json(Path(args.file)), args.session)
        elif args.command == "continue":
            result = continue_session(store, args.session)
        elif args.command == "status":
            result = public_session(store, args.session) if args.session else learner_status(store)
        elif args.command == "report":
            if args.format == "json" or args.json:
                serialized = json.dumps(build_report_data(store, args.session),
                                        ensure_ascii=False, indent=2) + "\n"
                if args.output:
                    atomic_write(Path(args.output), serialized.encode("utf-8"))
                print(serialized, end="")
            else:
                text = make_report(store, args.session)
                if args.output:
                    atomic_write(Path(args.output), text.encode("utf-8"))
                print(text, end="")
            return 0
        elif args.command == "serve":
            serve(store, args.session, args.host, args.port, args.open)
            return 0
        else:
            raise XSyncError("未知命令")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except XSyncError as exc:
        print(f"x-sync: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
