import json
import unittest
from dataclasses import replace

from xsync_v2.registry import (
    ActivateTarget,
    CreateAndActivateDialogue,
    PendingRegistryEvent,
    RegistryAccepted,
    decide,
    initial_registry_state,
    reduce,
)
from xsync_v2.registry_codec import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    build_committed_registry_transaction,
    build_registry_state_snapshot,
    build_registry_transaction_marker,
    build_stored_registry_event,
    canonical_json_bytes,
    decode_registry_state_snapshot,
    decode_registry_transaction_marker,
    decode_stored_registry_event,
    encode_registry_state_snapshot,
    encode_registry_transaction_marker,
    encode_stored_registry_event,
    registry_state_digest,
    sha256_digest,
    validate_committed_registry_transaction,
)

import tests.xsync_v2_path  # noqa: F401


def digest(label):
    import hashlib

    return "sha256:" + hashlib.sha256(label.encode("utf-8")).hexdigest()


def fixture():
    initial = initial_registry_state("registry-1")
    command = CreateAndActivateDialogue(
        "cmd.create",
        digest("create"),
        0,
        0,
        ActivateTarget("dlg-a", digest("config-a")),
    )
    decision = decide(initial, command)
    if type(decision) is not RegistryAccepted:
        raise AssertionError(decision)
    transaction = build_committed_registry_transaction(
        initial,
        decision,
        transaction_id="tx.create",
        event_ids=("evt.create.1", "evt.create.2"),
    )
    state = reduce(initial, transaction)
    first = build_stored_registry_event(
        initial.registry_id, transaction.events[0], None
    )
    second = build_stored_registry_event(
        initial.registry_id, transaction.events[1], first.event_hash
    )
    marker = build_registry_transaction_marker(
        registry_id=initial.registry_id,
        transaction=transaction,
        previous_marker_hash=None,
        state=state,
        events=(first, second),
    )
    snapshot = build_registry_state_snapshot(
        state,
        last_event_hash=second.event_hash,
        last_marker_hash=marker.marker_hash,
    )
    return initial, transaction, state, (first, second), marker, snapshot


class RegistryCodecTest(unittest.TestCase):
    def test_schema_v2_records_round_trip_as_one_canonical_form(self):
        _, transaction, state, events, marker, snapshot = fixture()
        validate_committed_registry_transaction(transaction)
        for event in events:
            encoded = encode_stored_registry_event(event)
            self.assertEqual(
                encoded,
                canonical_json_bytes(json.loads(encoded)),
            )
            self.assertEqual(event, decode_stored_registry_event(encoded))
        self.assertEqual(
            marker,
            decode_registry_transaction_marker(
                encode_registry_transaction_marker(marker)
            ),
        )
        self.assertEqual(
            snapshot,
            decode_registry_state_snapshot(
                encode_registry_state_snapshot(snapshot)
            ),
        )
        self.assertEqual(SCHEMA_VERSION, events[0].schema_version)
        self.assertEqual(PROTOCOL_VERSION, events[0].protocol_version)
        self.assertEqual(registry_state_digest(state), marker.state_digest)

    def test_event_and_transaction_digests_bind_every_committed_field(self):
        _, transaction, _, _, _, _ = fixture()
        changes = (
            replace(transaction.events[0], event_id="evt.changed"),
            replace(transaction.events[0], generation=1),
            replace(transaction.events[0], command_id="cmd.changed"),
        )
        for changed in changes:
            corrupted = replace(
                transaction,
                events=(changed, transaction.events[1]),
            )
            with self.assertRaisesRegex(
                ValueError, "REGISTRY_EVENT_DIGEST_MISMATCH"
            ):
                validate_committed_registry_transaction(corrupted)
        with self.assertRaisesRegex(
            ValueError, "REGISTRY_TRANSACTION_DIGEST_MISMATCH"
        ):
            validate_committed_registry_transaction(
                replace(transaction, transaction_id="tx.changed")
            )

    def test_unknown_duplicate_noncanonical_and_wrong_scalar_fail_closed(self):
        _, _, _, events, _, _ = fixture()
        encoded = encode_stored_registry_event(events[0])
        tree = json.loads(encoded)

        unknown = {**tree, "unknown": True}
        with self.assertRaisesRegex(ValueError, "INVALID_RECORD_KEYS"):
            decode_stored_registry_event(canonical_json_bytes(unknown))

        duplicate = encoded.replace(
            b"{", b'{"schema_version":2,', 1
        )
        with self.assertRaisesRegex(ValueError, "DUPLICATE_JSON_KEY"):
            decode_stored_registry_event(duplicate)

        with self.assertRaisesRegex(ValueError, "NON_CANONICAL_RECORD"):
            decode_stored_registry_event(b" " + encoded)

        tree["event"]["registry_sequence"] = True
        with self.assertRaisesRegex(ValueError, "INVALID_SCALAR_TYPE"):
            decode_stored_registry_event(canonical_json_bytes(tree))

    def test_hash_schema_protocol_and_exact_record_types_are_verified(self):
        _, _, _, events, marker, snapshot = fixture()
        event_tree = json.loads(encode_stored_registry_event(events[0]))
        event_tree["event_hash"] = digest("tampered")
        with self.assertRaisesRegex(ValueError, "STORED_REGISTRY_EVENT_HASH_MISMATCH"):
            decode_stored_registry_event(canonical_json_bytes(event_tree))

        marker_tree = json.loads(encode_registry_transaction_marker(marker))
        marker_tree["marker_hash"] = digest("tampered")
        with self.assertRaisesRegex(
            ValueError, "REGISTRY_TRANSACTION_MARKER_HASH_MISMATCH"
        ):
            decode_registry_transaction_marker(canonical_json_bytes(marker_tree))

        snapshot_tree = json.loads(encode_registry_state_snapshot(snapshot))
        snapshot_tree["state_digest"] = digest("tampered")
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_STATE_SNAPSHOT"):
            decode_registry_state_snapshot(canonical_json_bytes(snapshot_tree))

        event_tree = json.loads(encode_stored_registry_event(events[0]))
        event_tree["schema_version"] = 1
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_REGISTRY_EVENT"):
            decode_stored_registry_event(canonical_json_bytes(event_tree))

        event_tree = json.loads(encode_stored_registry_event(events[0]))
        event_tree["protocol_version"] = "x-sync-dialogue/1"
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_REGISTRY_EVENT"):
            decode_stored_registry_event(canonical_json_bytes(event_tree))

    def test_empty_snapshot_is_valid_but_cannot_claim_chain_hashes(self):
        initial = initial_registry_state("registry-1")
        snapshot = build_registry_state_snapshot(
            initial,
            last_event_hash=None,
            last_marker_hash=None,
        )
        self.assertEqual(
            snapshot,
            decode_registry_state_snapshot(
                encode_registry_state_snapshot(snapshot)
            ),
        )
        invalid = replace(snapshot, last_event_hash=digest("event"))
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_STATE_SNAPSHOT"):
            encode_registry_state_snapshot(invalid)

    def test_codec_entry_points_reject_wrong_runtime_and_wire_types(self):
        _, transaction, state, events, marker, snapshot = fixture()

        with self.assertRaisesRegex(TypeError, "exact bytes"):
            sha256_digest(bytearray(b"not-exact-bytes"))
        with self.assertRaisesRegex(ValueError, "INVALID_CANONICAL_VALUE"):
            canonical_json_bytes({"unsupported": object()})
        for raw, code in (
            (b"", "INVALID_RECORD_SIZE"),
            (b"\xff", "INVALID_JSON_RECORD"),
        ):
            with self.subTest(raw=raw):
                with self.assertRaisesRegex(ValueError, code):
                    decode_stored_registry_event(raw)

        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_STATE"):
            registry_state_digest(object())
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_REGISTRY_EVENT"):
            encode_stored_registry_event(object())

        event_tree = json.loads(encode_stored_registry_event(events[0]))
        event_tree["event"] = None
        with self.assertRaisesRegex(ValueError, "INVALID_RECORD_TYPE"):
            decode_stored_registry_event(canonical_json_bytes(event_tree))
        event_tree = json.loads(encode_stored_registry_event(events[0]))
        event_tree["event"]["payload"] = []
        with self.assertRaisesRegex(ValueError, "INVALID_UNION_VALUE"):
            decode_stored_registry_event(canonical_json_bytes(event_tree))
        event_tree = json.loads(encode_stored_registry_event(events[0]))
        event_tree["event"]["registry_sequence"] = []
        with self.assertRaisesRegex(ValueError, "INVALID_SCALAR_TYPE"):
            decode_stored_registry_event(canonical_json_bytes(event_tree))

        with self.assertRaisesRegex(ValueError, "INVALID_STORED_REGISTRY_EVENT"):
            encode_stored_registry_event(replace(events[0], registry_id=""))
        with self.assertRaisesRegex(ValueError, "INVALID_STORED_REGISTRY_EVENT"):
            build_stored_registry_event("bad/id", transaction.events[0], None)
        with self.assertRaisesRegex(
            ValueError, "INVALID_REGISTRY_TRANSACTION_MARKER"
        ):
            encode_registry_transaction_marker(replace(marker, generation=0))
        with self.assertRaisesRegex(
            ValueError, "INVALID_REGISTRY_TRANSACTION_MARKER"
        ):
            encode_registry_transaction_marker(
                replace(marker, events=(marker.events[0], marker.events[0]))
            )
        with self.assertRaisesRegex(
            ValueError, "INVALID_REGISTRY_STATE_SNAPSHOT"
        ):
            encode_registry_state_snapshot(replace(snapshot, last_event_hash=None))

        # A non-empty snapshot must bind both durable chain tips. The initial
        # snapshot is the only shape allowed to omit both hashes.
        empty = build_registry_state_snapshot(
            initial_registry_state("registry-1"),
            last_event_hash=None,
            last_marker_hash=None,
        )
        with self.assertRaisesRegex(
            ValueError, "INVALID_REGISTRY_STATE_SNAPSHOT"
        ):
            encode_registry_state_snapshot(
                replace(empty, last_marker_hash=digest("impossible-tip"))
            )
        self.assertEqual(state.registry_id, snapshot.registry_id)

    def test_transaction_builder_rejects_ambiguous_batches_and_marker_chains(self):
        initial, transaction, state, events, marker, _ = fixture()
        decision = decide(
            initial,
            CreateAndActivateDialogue(
                "cmd.create",
                digest("create"),
                0,
                0,
                ActivateTarget("dlg-a", digest("config-a")),
            ),
        )
        self.assertIsInstance(decision, RegistryAccepted)
        assert isinstance(decision, RegistryAccepted)

        invalid_builds = (
            (object(), decision, "tx.other", ("evt.1", "evt.2")),
            (initial, decision, "../escape", ("evt.1", "evt.2")),
            (initial, decision, "tx.other", ("evt.same", "evt.same")),
        )
        for build_state, accepted, transaction_id, event_ids in invalid_builds:
            with self.subTest(transaction_id=transaction_id, event_ids=event_ids):
                with self.assertRaisesRegex(
                    ValueError, "INVALID_REGISTRY_ACCEPTED_BATCH"
                ):
                    build_committed_registry_transaction(
                        build_state,
                        accepted,
                        transaction_id=transaction_id,
                        event_ids=event_ids,
                    )

        mismatched = RegistryAccepted(
            (
                decision.events[0],
                replace(decision.events[1], command_id="cmd.other"),
            )
        )
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_ACCEPTED_BATCH"):
            build_committed_registry_transaction(
                initial,
                mismatched,
                transaction_id="tx.other",
                event_ids=("evt.1", "evt.2"),
            )

        unsupported_payload = RegistryAccepted(
            (
                PendingRegistryEvent(
                    "cmd.unsupported",
                    digest("unsupported"),
                    object(),
                ),
            )
        )
        with self.assertRaisesRegex(ValueError, "INVALID_REGISTRY_ACCEPTED_BATCH"):
            build_committed_registry_transaction(
                initial,
                unsupported_payload,
                transaction_id="tx.unsupported",
                event_ids=("evt.unsupported",),
            )

        with self.assertRaisesRegex(
            ValueError, "INVALID_COMMITTED_REGISTRY_TRANSACTION"
        ):
            validate_committed_registry_transaction(object())
        with self.assertRaisesRegex(
            ValueError, "INVALID_COMMITTED_REGISTRY_TRANSACTION"
        ):
            validate_committed_registry_transaction(
                replace(transaction, events=(transaction.events[0],) * 2)
            )

        with self.assertRaisesRegex(
            ValueError, "INVALID_REGISTRY_TRANSACTION_MARKER"
        ):
            build_registry_transaction_marker(
                registry_id="registry-1",
                transaction=transaction,
                previous_marker_hash=None,
                state=initial,
                events=events,
            )
        disconnected_second = build_stored_registry_event(
            "registry-1", transaction.events[1], None
        )
        with self.assertRaisesRegex(
            ValueError, "INVALID_REGISTRY_TRANSACTION_MARKER"
        ):
            build_registry_transaction_marker(
                registry_id="registry-1",
                transaction=transaction,
                previous_marker_hash=None,
                state=state,
                events=(events[0], disconnected_second),
            )
        with self.assertRaisesRegex(
            ValueError, "INVALID_REGISTRY_TRANSACTION_MARKER"
        ):
            build_registry_transaction_marker(
                registry_id="wrong-registry",
                transaction=transaction,
                previous_marker_hash=marker.marker_hash,
                state=state,
                events=events,
            )


if __name__ == "__main__":
    unittest.main()
