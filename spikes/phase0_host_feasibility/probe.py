#!/usr/bin/env python3
"""Disposable Phase 0 harness for testing long-lived Host tool channels.

This module is intentionally independent from both the v1 runtime and the v2
kernel.  It proves transport feasibility only; none of its journal records are
production dialogue events.
"""

from __future__ import annotations

import argparse
import copy
import errno
import fcntl
import hashlib
import hmac
from http import HTTPStatus
import http.server
import json
import os
from pathlib import Path
import queue
import secrets
import sys
import tempfile
import threading
import time
from typing import Any
import urllib.error
import urllib.request
import uuid


SCHEMA_VERSION = 1
MAX_BODY_BYTES = 64 * 1024
MAX_TEXT_CHARS = 16 * 1024
DEFAULT_TOKEN_ENV = "XSYNC_PHASE0_PROBE_TOKEN"


class ProbeError(RuntimeError):
    """Stable error returned by the disposable probe protocol."""

    def __init__(self, code: str, message: str | None = None, status: int = 409):
        super().__init__(message or code)
        self.code = code
        self.status = status


class SystemClock:
    def now(self) -> float:
        return time.time()


def _prepare_state_dir(state_dir: Path) -> Path:
    unresolved_state_dir = Path(state_dir)
    if unresolved_state_dir.is_symlink():
        raise ProbeError("UNSAFE_STATE_DIR", status=400)
    resolved_state_dir = unresolved_state_dir.resolve()
    resolved_state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    return resolved_state_dir


class StateDirectoryOwner:
    """Hold a cooperative process lock for one running probe daemon."""

    def __init__(self, state_dir: Path):
        self.state_dir = _prepare_state_dir(state_dir)
        self.path = self.state_dir / ".probe-owner.lock"
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                    raise
                raise ProbeError("STATE_DIR_IN_USE", status=409) from exc
            self._stream = os.fdopen(descriptor, "r+", encoding="utf-8")
            descriptor = -1
            self._stream.seek(0)
            self._stream.truncate()
            json.dump(
                {"owner_pid": os.getpid(), "acquired_at": time.time()},
                self._stream,
                sort_keys=True,
            )
            self._stream.write("\n")
            self._stream.flush()
            os.fsync(self._stream.fileno())
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            else:
                self._stream.close()
            raise

    def __enter__(self) -> StateDirectoryOwner:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        stream = getattr(self, "_stream", None)
        if stream is None:
            return
        self._stream = None
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()


def _require_text(value: object, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ProbeError("VALIDATION_FAILED", f"invalid {name}", 400)
    if any(ord(character) < 32 for character in value):
        raise ProbeError("VALIDATION_FAILED", f"invalid {name}", 400)
    return value


def _require_positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ProbeError("VALIDATION_FAILED", f"invalid {name}", 400)
    return value


def _canonical_hash(value: object) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProbeError("VALIDATION_FAILED", "result must be JSON", 400) from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class Journal:
    """Single-writer, atomically materialized JSON journal for the probe."""

    def __init__(
        self,
        state_dir: Path,
        *,
        clock: object | None = None,
        runtime_epoch: str | None = None,
    ):
        self.state_dir = _prepare_state_dir(state_dir)
        self.path = self.state_dir / "journal.json"
        self.clock = clock or SystemClock()
        self._condition = threading.Condition(threading.RLock())
        with self._condition:
            if self.path.exists():
                self._state = json.loads(self.path.read_text(encoding="utf-8"))
                if self._state.get("schema_version") != SCHEMA_VERSION:
                    raise ProbeError("UNSUPPORTED_JOURNAL", status=400)
                self._state.setdefault("questions", [])
                self._state.setdefault("current_question", None)
            else:
                self._state = {
                    "schema_version": SCHEMA_VERSION,
                    "runtime_epoch": "",
                    "control_state": "active",
                    "next_work_sequence": 1,
                    "next_event_sequence": 1,
                    "works": [],
                    "questions": [],
                    "current_question": None,
                    "events": [],
                    "receipts": {},
                }
            previous_epoch = self._state.get("runtime_epoch")
            self._state["runtime_epoch"] = runtime_epoch or uuid.uuid4().hex
            if previous_epoch and previous_epoch != self._state["runtime_epoch"]:
                for work in self._state["works"]:
                    if work["state"] == "leased":
                        work["state"] = "queued"
                        work["lease"] = None
                        self._append_event_unlocked(
                            "status", "requeued", work["work_id"], True
                        )
            self._save_unlocked()

    def _now(self) -> float:
        return float(self.clock.now())

    def _append_event_unlocked(
        self,
        kind: str,
        state: str,
        work_id: str | None,
        deliver: bool,
        **fields: object,
    ) -> int:
        sequence = int(self._state["next_event_sequence"])
        self._state["next_event_sequence"] = sequence + 1
        event = {
            "sequence": sequence,
            "kind": kind,
            "state": state,
            "work_id": work_id,
            "deliver": deliver,
            "at": self._now(),
            **fields,
        }
        self._state["events"].append(event)
        return sequence

    def _save_unlocked(self) -> None:
        descriptor, temporary = tempfile.mkstemp(
            prefix=".journal-",
            suffix=".tmp",
            dir=self.state_dir,
        )
        temporary_path = Path(temporary)
        try:
            os.chmod(temporary_path, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(
                    self._state,
                    stream,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                )
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, self.path)
        finally:
            if temporary_path.exists():
                temporary_path.unlink()

    def _notify_unlocked(self) -> None:
        self._save_unlocked()
        self._condition.notify_all()

    def _public_work(self, work: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(work)
        result.pop("submission_handle", None)
        result.pop("lease_generation", None)
        return result

    def _find_by_handle_unlocked(self, handle: str) -> dict[str, Any]:
        for work in self._state["works"]:
            candidate = work.get("submission_handle")
            if isinstance(candidate, str) and hmac.compare_digest(candidate, handle):
                return work
        raise ProbeError("INVALID_SUBMISSION_HANDLE", status=404)

    def _sweep_expired_unlocked(self) -> bool:
        changed = False
        now = self._now()
        for work in self._state["works"]:
            lease = work.get("lease")
            if work["state"] == "leased" and lease["expires_at"] <= now:
                work["state"] = "queued"
                work["lease"] = None
                self._append_event_unlocked(
                    "status", "lease_expired", work["work_id"], True
                )
                changed = True
        if changed:
            self._notify_unlocked()
        return changed

    def _enqueue_unlocked(
        self,
        text_value: str,
        request_value: str,
        round_id: str | None,
    ) -> tuple[dict[str, Any], bool]:
        for work in self._state["works"]:
            if work["request_id"] == request_value:
                if work["payload"] != {"round_id": round_id, "text": text_value}:
                    raise ProbeError("IDEMPOTENCY_CONFLICT")
                return work, False
        if self._state["control_state"] != "active":
            raise ProbeError("CONTROL_NOT_ACTIVE")
        number = int(self._state["next_work_sequence"])
        self._state["next_work_sequence"] = number + 1
        work = {
            "work_id": f"work-{number}",
            "request_id": request_value,
            "payload": {"round_id": round_id, "text": text_value},
            "state": "queued",
            "submission_handle": None,
            "lease_generation": 0,
            "lease": None,
            "renewal_count": 0,
            "result": None,
            "created_at": self._now(),
        }
        self._state["works"].append(work)
        self._append_event_unlocked("work", "queued", work["work_id"], False)
        return work, True

    def enqueue(
        self,
        text: object,
        request_id: object,
        round_id: object | None = None,
    ) -> dict[str, Any]:
        text_value = _require_text(text, "text", MAX_TEXT_CHARS)
        request_value = _require_text(request_id, "request_id")
        round_value = (
            None if round_id is None else _require_text(round_id, "round_id")
        )
        with self._condition:
            work, created = self._enqueue_unlocked(
                text_value, request_value, round_value
            )
            if created:
                self._notify_unlocked()
            return self._public_work(work)

    def publish_question(
        self,
        round_id: object,
        question: object,
        request_id: object,
    ) -> dict[str, Any]:
        round_value = _require_text(round_id, "round_id")
        question_value = _require_text(question, "question", MAX_TEXT_CHARS)
        request_value = _require_text(request_id, "request_id")
        with self._condition:
            for recorded in self._state["questions"]:
                if recorded["request_id"] == request_value:
                    if (
                        recorded["round_id"] != round_value
                        or recorded["question"] != question_value
                    ):
                        raise ProbeError("IDEMPOTENCY_CONFLICT")
                    return copy.deepcopy(recorded)
                if recorded["round_id"] == round_value:
                    raise ProbeError("STALE_ROUND")
            current = self._state["current_question"]
            if current is not None and current["state"] == "awaiting_answer":
                raise ProbeError("QUESTION_ALREADY_OPEN")
            recorded = {
                "round_id": round_value,
                "question": question_value,
                "request_id": request_value,
                "state": "awaiting_answer",
                "answer_request_id": None,
                "answer_text": None,
                "work_id": None,
                "control": None,
                "published_at": self._now(),
            }
            self._state["questions"].append(recorded)
            self._state["current_question"] = recorded
            self._append_event_unlocked(
                "question", "awaiting_answer", None, False, round_id=round_value
            )
            self._notify_unlocked()
            return copy.deepcopy(recorded)

    def answer_question(
        self,
        round_id: object,
        text: object,
        request_id: object,
    ) -> dict[str, Any]:
        round_value = _require_text(round_id, "round_id")
        text_value = _require_text(text, "text", MAX_TEXT_CHARS)
        request_value = _require_text(request_id, "request_id")
        with self._condition:
            for recorded in self._state["questions"]:
                if recorded.get("answer_request_id") == request_value:
                    if (
                        recorded["round_id"] != round_value
                        or recorded["answer_text"] != text_value
                    ):
                        raise ProbeError("IDEMPOTENCY_CONFLICT")
                    work = next(
                        item
                        for item in self._state["works"]
                        if item["work_id"] == recorded["work_id"]
                    )
                    return {
                        "question": copy.deepcopy(recorded),
                        "work": self._public_work(work),
                    }
            current = self._state["current_question"]
            if (
                current is None
                or current["round_id"] != round_value
                or current["state"] != "awaiting_answer"
            ):
                raise ProbeError("STALE_ROUND")
            work, _ = self._enqueue_unlocked(
                text_value, request_value, round_value
            )
            current["state"] = "answered"
            current["answer_request_id"] = request_value
            current["answer_text"] = text_value
            current["work_id"] = work["work_id"]
            current["answered_at"] = self._now()
            self._append_event_unlocked(
                "question",
                "answered",
                work["work_id"],
                False,
                round_id=round_value,
            )
            self._notify_unlocked()
            return {
                "question": copy.deepcopy(current),
                "work": self._public_work(work),
            }

    def answer_for(self, round_id: object) -> dict[str, Any] | None:
        """Return a round's durable browser answer without claiming its work."""
        round_value = _require_text(round_id, "round_id")
        with self._condition:
            for recorded in self._state["questions"]:
                if recorded["round_id"] != round_value:
                    continue
                if recorded["state"] != "answered":
                    return None
                return {
                    "round_id": round_value,
                    "text": recorded["answer_text"],
                    "request_id": recorded["answer_request_id"],
                    "answered_at": recorded["answered_at"],
                }
            return None

    def open_supervisor(self, owner_id: object) -> dict[str, Any]:
        owner = _require_text(owner_id, "owner_id")
        with self._condition:
            return {
                "owner_id": owner,
                "runtime_epoch": self._state["runtime_epoch"],
                "journal_sequence": int(self._state["next_event_sequence"]) - 1,
                "control_state": self._state["control_state"],
            }

    def _claim_unlocked(
        self, owner_id: str, lease_seconds: float
    ) -> dict[str, Any] | None:
        if lease_seconds <= 0:
            raise ProbeError("VALIDATION_FAILED", "invalid lease", 400)
        if self._state["control_state"] != "active":
            return None
        work = next(
            (item for item in self._state["works"] if item["state"] == "queued"),
            None,
        )
        if work is None:
            return None
        generation = int(work.get("lease_generation", 0)) + 1
        work["lease_generation"] = generation
        work["submission_handle"] = secrets.token_urlsafe(24)
        work["state"] = "leased"
        work["lease"] = {
            "claim_id": uuid.uuid4().hex,
            "owner_id": owner_id,
            "lease_version": 1,
            "generation": generation,
            "runtime_epoch": self._state["runtime_epoch"],
            "expires_at": self._now() + float(lease_seconds),
        }
        sequence = self._append_event_unlocked(
            "work", "leased", work["work_id"], False, owner_id=owner_id
        )
        self._notify_unlocked()
        return {
            "journal_sequence": sequence,
            "work_id": work["work_id"],
            "payload": copy.deepcopy(work["payload"]),
            "submission_handle": work["submission_handle"],
            "runtime_epoch": self._state["runtime_epoch"],
            **copy.deepcopy(work["lease"]),
        }

    def claim_next(
        self, owner_id: object, lease_seconds: float
    ) -> dict[str, Any] | None:
        owner = _require_text(owner_id, "owner_id")
        with self._condition:
            self._sweep_expired_unlocked()
            return self._claim_unlocked(owner, lease_seconds)

    def next_for_supervisor(
        self,
        owner_id: object,
        after_sequence: int,
        lease_seconds: float,
        timeout: float = 0,
    ) -> dict[str, Any] | None:
        owner = _require_text(owner_id, "owner_id")
        if not isinstance(after_sequence, int) or after_sequence < 0:
            raise ProbeError("VALIDATION_FAILED", status=400)
        deadline = time.monotonic() + max(0.0, min(float(timeout), 5.0))
        with self._condition:
            while True:
                self._sweep_expired_unlocked()
                event = next(
                    (
                        item
                        for item in self._state["events"]
                        if item["deliver"] and item["sequence"] > after_sequence
                    ),
                    None,
                )
                if event is not None:
                    return {
                        "kind": "status",
                        "journal_sequence": event["sequence"],
                        "work_id": event["work_id"],
                        "state": event["state"],
                    }
                claimed = self._claim_unlocked(owner, lease_seconds)
                if claimed is not None:
                    return {"kind": "work", **claimed}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._condition.wait(remaining)

    def renew(
        self,
        owner_id: object,
        claim_id: object,
        lease_version: int,
        lease_seconds: float,
    ) -> dict[str, Any]:
        owner = _require_text(owner_id, "owner_id")
        claim = _require_text(claim_id, "claim_id")
        with self._condition:
            self._sweep_expired_unlocked()
            work = next(
                (
                    item
                    for item in self._state["works"]
                    if item["state"] == "leased"
                    and item["lease"]["claim_id"] == claim
                ),
                None,
            )
            if work is None:
                raise ProbeError("LEASE_EXPIRED")
            lease = work["lease"]
            if (
                lease["owner_id"] != owner
                or lease["runtime_epoch"] != self._state["runtime_epoch"]
                or lease["lease_version"] != lease_version
            ):
                raise ProbeError("LEASE_FENCED")
            lease["lease_version"] += 1
            lease["expires_at"] = self._now() + float(lease_seconds)
            work["renewal_count"] += 1
            sequence = self._append_event_unlocked(
                "lease", "renewed", work["work_id"], False
            )
            self._notify_unlocked()
            return {
                "journal_sequence": sequence,
                "work_id": work["work_id"],
                **copy.deepcopy(lease),
            }

    def submit(
        self,
        owner_id: object,
        submission_handle: object,
        claim_id: object,
        lease_version: object,
        runtime_epoch: object,
        generation: object,
        idempotency_key: object,
        result: object,
    ) -> dict[str, Any]:
        owner = _require_text(owner_id, "owner_id")
        handle = _require_text(submission_handle, "submission_handle")
        claim = _require_text(claim_id, "claim_id")
        version = _require_positive_int(lease_version, "lease_version")
        epoch = _require_text(runtime_epoch, "runtime_epoch")
        lease_generation = _require_positive_int(generation, "generation")
        key = _require_text(idempotency_key, "idempotency_key")
        result_hash = _canonical_hash(result)
        fence = {
            "owner_id": owner,
            "claim_id": claim,
            "lease_version": version,
            "runtime_epoch": epoch,
            "generation": lease_generation,
        }
        with self._condition:
            receipt = self._state["receipts"].get(key)
            if receipt is not None:
                if (
                    receipt["submission_handle"] != handle
                    or receipt.get("fence") != fence
                    or receipt["result_hash"] != result_hash
                ):
                    raise ProbeError("IDEMPOTENCY_CONFLICT")
                return copy.deepcopy(receipt["response"])
            work = self._find_by_handle_unlocked(handle)
            if work["state"] == "superseded":
                raise ProbeError("WORK_SUPERSEDED")
            self._sweep_expired_unlocked()
            if work["state"] != "leased":
                raise ProbeError("LEASE_EXPIRED")
            lease = work["lease"]
            if (
                lease["owner_id"] != owner
                or lease["claim_id"] != claim
                or lease["lease_version"] != version
                or lease["runtime_epoch"] != epoch
                or lease["generation"] != lease_generation
                or epoch != self._state["runtime_epoch"]
            ):
                raise ProbeError("LEASE_FENCED")
            work["state"] = "completed"
            work["result"] = copy.deepcopy(result)
            work["lease"] = None
            sequence = self._append_event_unlocked(
                "status", "completed", work["work_id"], False
            )
            response = {
                "journal_sequence": sequence,
                "receipt_id": f"receipt-{work['work_id']}-{key}",
                "work_id": work["work_id"],
                "state": "completed",
                "result_hash": result_hash,
            }
            self._state["receipts"][key] = {
                "submission_handle": handle,
                "fence": fence,
                "result_hash": result_hash,
                "response": response,
            }
            self._notify_unlocked()
            return copy.deepcopy(response)

    def control(self, action: object) -> dict[str, Any]:
        if action not in {"pause", "switch"}:
            raise ProbeError("VALIDATION_FAILED", "invalid control", 400)
        with self._condition:
            self._state["control_state"] = action
            current = self._state["current_question"]
            if current is not None and current["state"] == "awaiting_answer":
                current["state"] = "cancelled"
                current["control"] = action
                current["cancelled_at"] = self._now()
            superseded = []
            for work in self._state["works"]:
                if work["state"] in {"queued", "leased"}:
                    work["state"] = "superseded"
                    work["lease"] = None
                    superseded.append(work["work_id"])
                    self._append_event_unlocked(
                        "status", "superseded", work["work_id"], True
                    )
            sequence = self._append_event_unlocked(
                "status", action, None, True
            )
            self._notify_unlocked()
            return {
                "journal_sequence": sequence,
                "control_state": action,
                "superseded_work_ids": superseded,
            }

    def release_owner(self, owner_id: object, reason: str = "disconnected") -> int:
        owner = _require_text(owner_id, "owner_id")
        released = 0
        with self._condition:
            for work in self._state["works"]:
                lease = work.get("lease")
                if (
                    work["state"] == "leased"
                    and lease is not None
                    and lease["owner_id"] == owner
                ):
                    work["state"] = "queued"
                    work["lease"] = None
                    released += 1
                    self._append_event_unlocked(
                        "status", reason, work["work_id"], True
                    )
            if released:
                self._notify_unlocked()
        return released

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            self._sweep_expired_unlocked()
            return {
                "schema_version": SCHEMA_VERSION,
                "runtime_epoch": self._state["runtime_epoch"],
                "control_state": self._state["control_state"],
                "current_question": copy.deepcopy(
                    self._state["current_question"]
                ),
                "works": [self._public_work(item) for item in self._state["works"]],
            }

    def status(self) -> dict[str, Any]:
        with self._condition:
            snapshot = self.snapshot()
            counts: dict[str, int] = {}
            renewals = 0
            for work in snapshot["works"]:
                counts[work["state"]] = counts.get(work["state"], 0) + 1
                renewals += int(work["renewal_count"])
            return {
                "schema_version": SCHEMA_VERSION,
                "runtime_epoch": snapshot["runtime_epoch"],
                "control_state": snapshot["control_state"],
                "counts": counts,
                "lease_renewals": renewals,
                "journal_sequence": int(self._state["next_event_sequence"]) - 1,
            }


class LocalBroker:
    """Direct broker used by deterministic tests without network timing."""

    def __init__(self, journal: Journal):
        self.journal = journal

    def open_supervisor(self, owner_id: str) -> dict[str, Any]:
        return self.journal.open_supervisor(owner_id)

    def next_for_supervisor(
        self, owner_id: str, after: int, lease_seconds: float, timeout: float
    ) -> dict[str, Any] | None:
        return self.journal.next_for_supervisor(
            owner_id, after, lease_seconds, timeout
        )

    def renew(
        self, owner_id: str, claim_id: str, version: int, lease_seconds: float
    ) -> dict[str, Any]:
        return self.journal.renew(owner_id, claim_id, version, lease_seconds)

    def submit(
        self,
        owner_id: str,
        handle: str,
        claim_id: str,
        version: int,
        runtime_epoch: str,
        generation: int,
        key: str,
        result: object,
    ) -> dict[str, Any]:
        return self.journal.submit(
            owner_id,
            handle,
            claim_id,
            version,
            runtime_epoch,
            generation,
            key,
            result,
        )

    def release(self, owner_id: str, reason: str) -> int:
        return self.journal.release_owner(owner_id, reason)


class SupervisorSession:
    """Purely driven Supervisor state; the CLI supplies blocking I/O."""

    def __init__(
        self,
        broker: object,
        *,
        owner_id: str,
        lease_seconds: float,
        clock: object | None = None,
    ):
        self.broker = broker
        self.owner_id = _require_text(owner_id, "owner_id")
        self.lease_seconds = float(lease_seconds)
        if self.lease_seconds <= 0:
            raise ProbeError("VALIDATION_FAILED", "invalid lease", 400)
        self.clock = clock or SystemClock()
        self.cursor = 0
        self.stream_sequence = 0
        self.leases: dict[str, dict[str, Any]] = {}
        self.opened = False

    def _emit(self, kind: str, **fields: object) -> dict[str, Any]:
        self.stream_sequence += 1
        return {
            "protocol": "x-sync-host-phase0/1",
            "stream_sequence": self.stream_sequence,
            "type": kind,
            **fields,
        }

    def open(self) -> dict[str, Any]:
        opened = self.broker.open_supervisor(self.owner_id)
        self.cursor = int(opened["journal_sequence"])
        self.opened = True
        return self._emit(
            "ready",
            owner_id=self.owner_id,
            runtime_epoch=opened["runtime_epoch"],
            control_state=opened["control_state"],
            owner_pid=os.getpid(),
        )

    def poll_once(self, timeout: float = 0) -> dict[str, Any] | None:
        if not self.opened:
            raise ProbeError("SUPERVISOR_NOT_OPEN")
        item = self.broker.next_for_supervisor(
            self.owner_id,
            self.cursor,
            self.lease_seconds,
            timeout,
        )
        if item is None:
            return None
        self.cursor = max(self.cursor, int(item["journal_sequence"]))
        if item["kind"] == "work":
            self.leases[item["claim_id"]] = {
                **item,
                "next_renew_at": float(self.clock.now())
                + self.lease_seconds / 3,
            }
            return self._emit(
                "work",
                work_id=item["work_id"],
                payload=item["payload"],
                submission_handle=item["submission_handle"],
                claim_id=item["claim_id"],
                lease_version=item["lease_version"],
                generation=item["generation"],
                runtime_epoch=item["runtime_epoch"],
                expires_at=item["expires_at"],
            )
        if item.get("work_id") is not None:
            self.leases = {
                claim: lease
                for claim, lease in self.leases.items()
                if lease["work_id"] != item["work_id"]
            }
        return self._emit(
            "status",
            state=item["state"],
            work_id=item.get("work_id"),
        )

    def renew_due(self) -> int:
        now = float(self.clock.now())
        renewed = 0
        for claim_id, lease in tuple(self.leases.items()):
            if now < lease["next_renew_at"]:
                continue
            updated = self.broker.renew(
                self.owner_id,
                claim_id,
                int(lease["lease_version"]),
                self.lease_seconds,
            )
            lease.update(updated)
            lease["next_renew_at"] = now + self.lease_seconds / 3
            self.cursor = max(self.cursor, int(updated["journal_sequence"]))
            renewed += 1
        return renewed

    def handle_stdin_line(self, line: str) -> dict[str, Any]:
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProbeError("INVALID_STDIN_JSON", status=400) from exc
        if not isinstance(message, dict):
            raise ProbeError("INVALID_STDIN_JSON", status=400)
        if message.get("type") == "close":
            return self.close(str(message.get("reason", "requested")))
        if message.get("type") != "submit":
            raise ProbeError("INVALID_STDIN_MESSAGE", status=400)
        supplied_handle = _require_text(
            message.get("submission_handle"), "submission_handle"
        )
        lease = next(
            (
                value
                for value in self.leases.values()
                if hmac.compare_digest(value["submission_handle"], supplied_handle)
            ),
            None,
        )
        if lease is None:
            raise ProbeError("LEASE_FENCED")
        response = self.broker.submit(
            self.owner_id,
            supplied_handle,
            lease["claim_id"],
            int(lease["lease_version"]),
            lease["runtime_epoch"],
            int(lease["generation"]),
            message.get("idempotency_key"),
            message.get("result"),
        )
        self.cursor = max(self.cursor, int(response["journal_sequence"]))
        self.leases = {
            claim: lease
            for claim, lease in self.leases.items()
            if lease["work_id"] != response["work_id"]
        }
        return self._emit(
            "status",
            state="completed",
            work_id=response["work_id"],
            receipt_id=response["receipt_id"],
            result_hash=response["result_hash"],
        )

    def close(self, reason: str = "closed") -> dict[str, Any]:
        if self.opened:
            self.broker.release(self.owner_id, "disconnected")
            self.opened = False
        return self._emit("closed", reason=reason)


class HttpBroker:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _request(self, path: str, body: dict[str, Any]) -> Any:
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                try:
                    payload = json.load(exc)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    payload = {"error": "HTTP_ERROR"}
            finally:
                exc.close()
            raise ProbeError(
                str(payload.get("error", "HTTP_ERROR")),
                str(payload.get("message", "HTTP error")),
                exc.code,
            ) from exc

    def open_supervisor(self, owner_id: str) -> dict[str, Any]:
        return self._request("/host/open", {"owner_id": owner_id})

    def next_for_supervisor(
        self, owner_id: str, after: int, lease_seconds: float, timeout: float
    ) -> dict[str, Any] | None:
        return self._request(
            "/host/next",
            {
                "owner_id": owner_id,
                "after_sequence": after,
                "lease_seconds": lease_seconds,
                "timeout": timeout,
            },
        ).get("item")

    def renew(
        self, owner_id: str, claim_id: str, version: int, lease_seconds: float
    ) -> dict[str, Any]:
        return self._request(
            "/host/renew",
            {
                "owner_id": owner_id,
                "claim_id": claim_id,
                "lease_version": version,
                "lease_seconds": lease_seconds,
            },
        )

    def submit(
        self,
        owner_id: str,
        handle: str,
        claim_id: str,
        version: int,
        runtime_epoch: str,
        generation: int,
        key: str,
        result: object,
    ) -> dict[str, Any]:
        return self._request(
            "/host/submit",
            {
                "owner_id": owner_id,
                "submission_handle": handle,
                "claim_id": claim_id,
                "lease_version": version,
                "runtime_epoch": runtime_epoch,
                "generation": generation,
                "idempotency_key": key,
                "result": result,
            },
        )

    def release(self, owner_id: str, reason: str) -> int:
        response = self._request(
            "/host/release", {"owner_id": owner_id, "reason": reason}
        )
        return int(response["released"])


class ProbeHandler(http.server.BaseHTTPRequestHandler):
    server_version = "XSyncPhase0/1"

    @property
    def probe_server(self) -> Any:
        return self.server

    def log_message(self, format_string: str, *args: object) -> None:
        print(format_string % args, file=sys.stderr)

    def _send(self, status: int, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        host = self.headers.get("Host", "")
        hostname = host.rsplit(":", 1)[0].strip("[]").lower()
        if hostname not in {"127.0.0.1", "localhost", "::1"}:
            self._send(HTTPStatus.BAD_REQUEST, {"error": "INVALID_HOST"})
            return False
        supplied = self.headers.get("Authorization", "")
        expected = f"Bearer {self.probe_server.token}"
        if not hmac.compare_digest(supplied, expected):
            self._send(HTTPStatus.UNAUTHORIZED, {"error": "UNAUTHORIZED"})
            return False
        if self.command == "POST":
            origin = self.headers.get("Origin")
            expected_origin = f"http://{host}"
            if origin is not None and origin != expected_origin:
                self._send(HTTPStatus.FORBIDDEN, {"error": "INVALID_ORIGIN"})
                return False
        return True

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ProbeError("INVALID_BODY", status=400) from exc
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ProbeError("INVALID_BODY", status=400)
        if self.headers.get_content_type() != "application/json":
            raise ProbeError("INVALID_CONTENT_TYPE", status=415)
        try:
            payload = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ProbeError("INVALID_BODY", status=400) from exc
        if not isinstance(payload, dict):
            raise ProbeError("INVALID_BODY", status=400)
        return payload

    def do_GET(self) -> None:
        if self.path in {"/", "/probe.html"}:
            body = self.probe_server.html_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src http://127.0.0.1:* http://localhost:*")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)
            return
        if not self._authorized():
            return
        if self.path == "/state":
            self._send(HTTPStatus.OK, self.probe_server.journal.snapshot())
        elif self.path == "/status":
            self._send(HTTPStatus.OK, self.probe_server.journal.status())
        else:
            self._send(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})

    def do_POST(self) -> None:
        if not self._authorized():
            return
        try:
            payload = self._read_json()
            journal = self.probe_server.journal
            if self.path == "/enqueue":
                response = journal.enqueue(
                    payload.get("text"),
                    payload.get("request_id"),
                    payload.get("round_id"),
                )
            elif self.path == "/question/publish":
                response = journal.publish_question(
                    payload.get("round_id"),
                    payload.get("question"),
                    payload.get("request_id"),
                )
            elif self.path == "/question/answer":
                response = journal.answer_question(
                    payload.get("round_id"),
                    payload.get("text"),
                    payload.get("request_id"),
                )
            elif self.path == "/question/answer-for":
                response = {"answer": journal.answer_for(payload.get("round_id"))}
            elif self.path == "/control":
                response = journal.control(payload.get("action"))
            elif self.path == "/host/open":
                response = journal.open_supervisor(payload.get("owner_id"))
            elif self.path == "/host/next":
                response = {
                    "item": journal.next_for_supervisor(
                        payload.get("owner_id"),
                        payload.get("after_sequence"),
                        payload.get("lease_seconds"),
                        payload.get("timeout", 0),
                    )
                }
            elif self.path == "/host/renew":
                response = journal.renew(
                    payload.get("owner_id"),
                    payload.get("claim_id"),
                    payload.get("lease_version"),
                    payload.get("lease_seconds"),
                )
            elif self.path == "/host/submit":
                response = journal.submit(
                    payload.get("owner_id"),
                    payload.get("submission_handle"),
                    payload.get("claim_id"),
                    payload.get("lease_version"),
                    payload.get("runtime_epoch"),
                    payload.get("generation"),
                    payload.get("idempotency_key"),
                    payload.get("result"),
                )
            elif self.path == "/host/release":
                response = {
                    "released": journal.release_owner(
                        payload.get("owner_id"),
                        str(payload.get("reason", "disconnected")),
                    )
                }
            else:
                raise ProbeError("NOT_FOUND", status=404)
            self._send(HTTPStatus.OK, response)
        except ProbeError as exc:
            self._send(exc.status, {"error": exc.code, "message": str(exc)})
        except (TypeError, ValueError):
            self._send(HTTPStatus.BAD_REQUEST, {"error": "VALIDATION_FAILED"})


class ProbeHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False


def make_server(
    journal: Journal,
    *,
    token: str | None = None,
    port: int = 0,
) -> ProbeHTTPServer:
    server = ProbeHTTPServer(("127.0.0.1", int(port)), ProbeHandler)
    server.journal = journal
    server.token = token or secrets.token_urlsafe(32)
    server.html_path = Path(__file__).with_name("probe.html")
    return server


def _http_json(
    base_url: str,
    token: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> Any:
    headers = {"Authorization": f"Bearer {token}"}
    data = None
    method = "GET"
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
        method = "POST"
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def _print_json(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)


def _remote_token(arguments: argparse.Namespace) -> str:
    if arguments.token_env is not None:
        environment_name = _require_text(arguments.token_env, "token_env")
        token = os.environ.get(environment_name)
    elif arguments.token is not None:
        token = arguments.token
        environment_name = None
    else:
        environment_name = DEFAULT_TOKEN_ENV
        token = os.environ.get(environment_name)
    if not token:
        message = (
            f"set {environment_name} to the bearer token"
            if environment_name is not None
            else "bearer token must not be empty"
        )
        raise ProbeError("MISSING_TOKEN", message, 400)
    return _require_text(token, "token", 4096)


def run_supervisor(arguments: argparse.Namespace, token: str) -> int:
    broker = HttpBroker(arguments.url, token)
    session = SupervisorSession(
        broker,
        owner_id=arguments.owner,
        lease_seconds=arguments.lease_seconds,
    )
    _print_json(session.open())
    inbox: queue.Queue[object] = queue.Queue()
    stdin_eof = object()

    def read_stdin() -> None:
        try:
            for line in sys.stdin:
                if line.strip():
                    inbox.put(line)
        finally:
            inbox.put(stdin_eof)

    threading.Thread(target=read_stdin, daemon=True).start()
    started = time.monotonic()
    try:
        while session.opened:
            while True:
                try:
                    message = inbox.get_nowait()
                except queue.Empty:
                    break
                if message is stdin_eof:
                    _print_json(session.close("stdin-eof"))
                    break
                try:
                    _print_json(session.handle_stdin_line(str(message)))
                except ProbeError as exc:
                    _print_json(
                        session._emit("status", state="rejected", error=exc.code)
                    )
            if not session.opened:
                break
            try:
                session.renew_due()
                envelope = session.poll_once(timeout=0.25)
                if envelope is not None:
                    _print_json(envelope)
            except ProbeError as exc:
                _print_json(session._emit("status", state="error", error=exc.code))
            if (
                arguments.max_tenure_seconds > 0
                and time.monotonic() - started >= arguments.max_tenure_seconds
            ):
                break
    except KeyboardInterrupt:
        pass
    finally:
        if session.opened:
            _print_json(session.close("terminated"))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    serve = subcommands.add_parser("serve")
    serve.add_argument("--state-dir", type=Path, required=True)
    serve.add_argument("--port", type=int, default=0)

    def remote(name: str) -> argparse.ArgumentParser:
        command = subcommands.add_parser(name)
        command.add_argument("--url", required=True)
        token = command.add_mutually_exclusive_group()
        token.add_argument(
            "--token",
            help="legacy argv token; prefer --token-env or %s"
            % DEFAULT_TOKEN_ENV,
        )
        token.add_argument(
            "--token-env",
            metavar="NAME",
            help="read the bearer token from NAME",
        )
        return command

    supervise = remote("supervise")
    supervise.add_argument("--owner", required=True)
    supervise.add_argument("--stream-json", action="store_true")
    supervise.add_argument("--lease-seconds", type=float, default=30)
    supervise.add_argument("--max-tenure-seconds", type=float, default=0)
    enqueue = remote("enqueue")
    enqueue.add_argument("--text", required=True)
    enqueue.add_argument("--request-id", default=None)
    enqueue.add_argument("--round-id")
    publish_question = remote("publish-question")
    publish_question.add_argument("--round-id", required=True)
    publish_question.add_argument("--question", required=True)
    publish_question.add_argument("--request-id", default=None)
    answer_question = remote("answer-question")
    answer_question.add_argument("--round-id", required=True)
    answer_question.add_argument("--text", required=True)
    answer_question.add_argument("--request-id", default=None)
    control = remote("control")
    control.add_argument("action", choices=("pause", "switch"))
    remote("state")
    remote("status")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.command == "serve":
        with StateDirectoryOwner(arguments.state_dir) as state_owner:
            journal = Journal(state_owner.state_dir)
            server = make_server(journal, port=arguments.port)
            host, port = server.server_address
            _print_json(
                {
                    "type": "ready",
                    "url": f"http://{host}:{port}",
                    "token": server.token,
                    "runtime_epoch": journal.status()["runtime_epoch"],
                }
            )
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
        return 0
    token = _remote_token(arguments)
    if arguments.command == "supervise":
        return run_supervisor(arguments, token)
    if arguments.command == "enqueue":
        request_id = arguments.request_id or uuid.uuid4().hex
        _print_json(
            _http_json(
                arguments.url,
                token,
                "/enqueue",
                {
                    "text": arguments.text,
                    "request_id": request_id,
                    "round_id": arguments.round_id,
                },
            )
        )
        return 0
    if arguments.command == "publish-question":
        _print_json(
            _http_json(
                arguments.url,
                token,
                "/question/publish",
                {
                    "round_id": arguments.round_id,
                    "question": arguments.question,
                    "request_id": arguments.request_id or uuid.uuid4().hex,
                },
            )
        )
        return 0
    if arguments.command == "answer-question":
        _print_json(
            _http_json(
                arguments.url,
                token,
                "/question/answer",
                {
                    "round_id": arguments.round_id,
                    "text": arguments.text,
                    "request_id": arguments.request_id or uuid.uuid4().hex,
                },
            )
        )
        return 0
    if arguments.command == "control":
        _print_json(
            _http_json(
                arguments.url,
                token,
                "/control",
                {"action": arguments.action},
            )
        )
        return 0
    _print_json(
        _http_json(arguments.url, token, f"/{arguments.command}")
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as error:
        print(json.dumps({"error": error.code, "message": str(error)}), file=sys.stderr)
        raise SystemExit(1) from error
