"""Start a self-contained open chat using a locally authenticated host CLI."""

from __future__ import annotations

import argparse
import getpass
import json
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import threading
import webbrowser
from collections.abc import Sequence

from .host import HostError, HostRunner
from .server import ChatRuntime, ChatServer
from .store import ChatError, ChatStore, state_directory


def resolve_repository(value: str) -> Path:
    target = Path(value).expanduser().resolve()
    if not target.is_dir():
        raise ChatError("目标项目目录不存在。")
    try:
        result = subprocess.run(
            ["git", "-C", str(target), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ChatError("请选择一个有效的 Git 工作目录。") from exc
    return Path(result.stdout.strip()).resolve()


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="xsync chat", description="开放式仓库问答")
    commands = root.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="启动自由聊天页面，自动回复网页问题")
    serve.add_argument("--repo", "-d", default=".")
    serve.add_argument("--host", choices=("codex", "claude"), default="codex")
    serve.add_argument("--learner", default=getpass.getuser())
    serve.add_argument("--port", type=int, default=0)
    serve.add_argument("--timeout", type=int, default=600)
    serve.add_argument("--open", action="store_true")
    serve.add_argument("--stream-json", action="store_true")
    return root


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    if not 0 <= arguments.port <= 65535 or not 1 <= arguments.timeout <= 3600:
        print("x-sync: 端口或超时设置无效。", file=sys.stderr)
        return 2
    runtime = None
    server = None
    previous: dict[int, object] = {}
    stopped = threading.Event()
    try:
        repository = resolve_repository(arguments.repo)
        runner = HostRunner(arguments.host, repository, timeout=arguments.timeout)
        directory = state_directory(repository, arguments.learner)
        runtime = ChatRuntime(ChatStore(directory, repository), runner)
        server = ChatServer(runtime, "chat." + secrets.token_hex(32), arguments.port)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, lambda *_: stopped.set())
        envelope = {
            "type": "ready",
            "mode": "chat",
            "host": arguments.host,
            "repository": str(repository),
            "session_id": runtime.store.state["session_id"],
            "browser_url": server.launch_url,
        }
        print(
            json.dumps(envelope, ensure_ascii=False)
            if arguments.stream_json
            else f"X-Sync 已启动：{server.launch_url}",
            flush=True,
        )
        if arguments.open:
            webbrowser.open(server.launch_url)
        stopped.wait()
        return 0
    except (ChatError, HostError) as exc:
        print(f"x-sync: {exc}", file=sys.stderr)
        return 2
    except Exception:
        print("x-sync: 无法启动聊天，请检查目录权限和端口。", file=sys.stderr)
        return 2
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if runtime is not None:
            runtime.close()
        for saved_signum, handler in previous.items():
            signal.signal(saved_signum, handler)  # type: ignore[arg-type]
