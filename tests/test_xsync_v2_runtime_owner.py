# ruff: noqa: I001
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import tests.xsync_v2_path  # noqa: F401

from xsync_v2.lease_store import LeaseOperation, RuntimeAuthorityCheck
from xsync_v2.locking import DomainLockManager, LockError
from xsync_v2.runtime_owner import RuntimeEpochAuthority


class RuntimeEpochAuthorityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temporary.cleanup)
        self.locks = DomainLockManager(Path(self.temporary.name) / "locks")
        self.addCleanup(self.locks.close)
        self.owner = self.locks.acquire_runtime_owner("runtime-2")
        self.verifier = RuntimeEpochAuthority(self.locks, self.owner)

    @staticmethod
    def check(
        runtime_epoch: str = "runtime-2",
        *,
        prior_runtime_epoch: str | None = None,
        prior_must_be_inactive: bool = False,
    ) -> RuntimeAuthorityCheck:
        return RuntimeAuthorityCheck(
            LeaseOperation.RECLAIM,
            runtime_epoch,
            "owner-2",
            prior_runtime_epoch,
            "owner-1" if prior_runtime_epoch is not None else None,
            prior_must_be_inactive,
        )

    def test_live_epoch_accepts_normal_and_cross_epoch_operations(self) -> None:
        with self.locks.semantic_session("dlg-1") as authority:
            self.assertTrue(self.verifier(self.check(), authority))
            self.assertTrue(
                self.verifier(
                    self.check(
                        prior_runtime_epoch="runtime-1",
                        prior_must_be_inactive=True,
                    ),
                    authority,
                )
            )
            self.assertFalse(
                self.verifier(self.check(runtime_epoch="runtime-3"), authority)
            )
            self.assertFalse(
                self.verifier(
                    self.check(
                        prior_runtime_epoch="runtime-2",
                        prior_must_be_inactive=True,
                    ),
                    authority,
                )
            )

    def test_authority_is_bound_to_owner_namespace_and_lifetime(self) -> None:
        foreign = DomainLockManager(Path(self.temporary.name) / "foreign-locks")
        self.addCleanup(foreign.close)
        with foreign.semantic_session("dlg-1") as authority:
            self.assertFalse(self.verifier(self.check(), authority))

        self.locks.release_runtime_owner(self.owner)
        with self.locks.semantic_session("dlg-1") as authority:
            self.assertFalse(self.verifier(self.check(), authority))

    def test_constructor_and_malformed_calls_fail_closed(self) -> None:
        with self.assertRaisesRegex(LockError, "INVALID_RUNTIME_OWNER_CONFIGURATION"):
            RuntimeEpochAuthority(object(), self.owner)  # type: ignore[arg-type]
        with self.locks.semantic_session("dlg-1") as authority:
            self.assertFalse(self.verifier(object(), authority))  # type: ignore[arg-type]
            self.assertFalse(self.verifier(self.check(), object()))  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
