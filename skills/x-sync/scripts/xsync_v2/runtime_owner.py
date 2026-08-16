"""Runtime epoch authority backed by the process-wide OS lifetime fence."""

from __future__ import annotations

from .lease_store import RuntimeAuthorityCheck
from .locking import (
    DomainLockManager,
    LockError,
    RuntimeOwnerAuthority,
    SessionLockAuthority,
)


class RuntimeEpochAuthority:
    """Verify lease operations against one live daemon runtime epoch."""

    def __init__(
        self,
        locks: DomainLockManager,
        owner: RuntimeOwnerAuthority,
    ) -> None:
        if (
            type(locks) is not DomainLockManager
            or type(owner) is not RuntimeOwnerAuthority
        ):
            raise LockError("INVALID_RUNTIME_OWNER_CONFIGURATION")
        locks.assert_runtime_owner_authority(owner)
        self._locks = locks
        self._owner = owner

    def __call__(
        self,
        check: RuntimeAuthorityCheck,
        authority: SessionLockAuthority,
    ) -> bool:
        """Accept only the live epoch and proven cross-epoch replacement."""
        if (
            type(check) is not RuntimeAuthorityCheck
            or type(authority) is not SessionLockAuthority
        ):
            return False
        try:
            self._locks.assert_runtime_owner_authority(self._owner)
            self._locks.assert_session_authority(
                authority,
                authority.session_id,
            )
        except LockError:
            return False
        if check.runtime_epoch != self._owner.runtime_epoch:
            return False
        if check.prior_owner_must_be_inactive and (
            check.prior_runtime_epoch is None
            or check.prior_runtime_epoch == check.runtime_epoch
        ):
            return False
        return True


__all__ = ["RuntimeEpochAuthority"]
