from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import json
import os
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.bootstrap import (
    DialogueBootstrapError,
    bootstrap_config,
    decode_bootstrap_manifest,
    read_bootstrap_manifest,
)
from xsync_v2.event_codec import canonical_json_bytes, sha256_digest


def manifest_tree() -> dict[str, object]:
    return {
        "schema_version": 2,
        "record_type": "dialogue_bootstrap_manifest",
        "protocol_version": "x-sync-dialogue/2",
        "session_id": "session-1",
        "learner_id": "learner-1",
        "created_at": "2026-08-16T22:30:00+08:00",
        "task_scope": "Understand the runtime boundary",
        "language": "zh-CN",
        "channel": "web",
        "style": "socratic",
        "focus": "mixed",
        "question_count": 5,
        "evidence_sources": [
            {
                "evidence_id": "evidence.runtime",
                "kind": "spec",
                "claim_type": "implementation",
                "claim": "The runtime uses a durable state machine.",
                "relative_path": "architecture.md",
                "start_line": 1,
                "end_line": 2,
                "imported_from": None,
            }
        ],
    }


class DialogueBootstrapTest(unittest.TestCase):
    def test_exact_manifest_decodes_to_typed_sources_and_stable_config(self) -> None:
        manifest = decode_bootstrap_manifest(canonical_json_bytes(manifest_tree()))
        evidence_digest = sha256_digest(b"focused evidence")

        first = bootstrap_config(manifest, "repository-1", evidence_digest)
        second = bootstrap_config(manifest, "repository-1", evidence_digest)

        self.assertEqual(first, second)
        self.assertEqual("session-1", first.session_id)
        self.assertRegex(first.runtime_epoch, r"\Abootstrap\.[0-9a-f]{48}\Z")
        self.assertEqual("architecture.md", manifest.evidence_sources[0].relative_path)

    def test_schema_duplicate_ids_ranges_and_enums_fail_closed(self) -> None:
        invalid_values: list[bytes] = [
            b"{}",
            b'{"schema_version":2,"schema_version":2}',
            b'{"schema_version":NaN}',
        ]
        wrong_schema = manifest_tree()
        wrong_schema["schema_version"] = True
        invalid_values.append(canonical_json_bytes(wrong_schema))

        duplicate = manifest_tree()
        sources = duplicate["evidence_sources"]
        assert isinstance(sources, list)
        sources.append(dict(sources[0]))
        invalid_values.append(canonical_json_bytes(duplicate))

        bad_range = manifest_tree()
        source = bad_range["evidence_sources"]
        assert isinstance(source, list) and isinstance(source[0], dict)
        source[0]["end_line"] = None
        invalid_values.append(canonical_json_bytes(bad_range))

        bad_enum = manifest_tree()
        source = bad_enum["evidence_sources"]
        assert isinstance(source, list) and isinstance(source[0], dict)
        source[0]["kind"] = "secret"
        invalid_values.append(canonical_json_bytes(bad_enum))

        bad_channel = manifest_tree()
        bad_channel["channel"] = []
        invalid_values.append(canonical_json_bytes(bad_channel))

        bad_timestamp = manifest_tree()
        bad_timestamp["created_at"] = "not-a-timestamp"
        invalid_values.append(canonical_json_bytes(bad_timestamp))

        for raw in invalid_values:
            with self.subTest(raw=raw[:80]), self.assertRaisesRegex(
                DialogueBootstrapError,
                "BOOTSTRAP_MANIFEST_INVALID",
            ):
                decode_bootstrap_manifest(raw)

    def test_manifest_file_must_be_owner_only_regular_and_not_a_symlink(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "bootstrap.json"
            manifest_path.write_bytes(canonical_json_bytes(manifest_tree()))
            manifest_path.chmod(0o600)

            loaded = read_bootstrap_manifest(manifest_path)
            self.assertEqual("session-1", loaded.session_id)

            manifest_path.chmod(0o644)
            with self.assertRaisesRegex(
                DialogueBootstrapError,
                "BOOTSTRAP_MANIFEST_UNAVAILABLE",
            ):
                read_bootstrap_manifest(manifest_path)
            manifest_path.chmod(0o600)

            link = root / "bootstrap-link.json"
            os.symlink(manifest_path.name, link)
            with self.assertRaisesRegex(
                DialogueBootstrapError,
                "BOOTSTRAP_MANIFEST_UNAVAILABLE",
            ):
                read_bootstrap_manifest(link)

    def test_pretty_json_is_accepted_but_unknown_fields_are_not(self) -> None:
        raw = json.dumps(manifest_tree(), indent=2).encode()
        self.assertEqual("session-1", decode_bootstrap_manifest(raw).session_id)
        value = manifest_tree()
        value["unknown"] = "field"
        with self.assertRaisesRegex(
            DialogueBootstrapError,
            "BOOTSTRAP_MANIFEST_INVALID",
        ):
            decode_bootstrap_manifest(canonical_json_bytes(value))


if __name__ == "__main__":
    unittest.main()
