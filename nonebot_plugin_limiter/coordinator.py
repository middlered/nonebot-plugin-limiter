import asyncio
from collections import defaultdict
from dataclasses import dataclass

from .identity import EntityIdentity


@dataclass(frozen=True, slots=True)
class NamedBucketKey:
    algorithm: str
    bucket_name: str
    identity: EntityIdentity


class NamedBucketCoordinator:
    def __init__(self) -> None:
        self._locks: dict[NamedBucketKey, asyncio.Lock] = {}
        self._pending: defaultdict[NamedBucketKey, int] = defaultdict(int)

    def lock(self, key: NamedBucketKey) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def pending(self, key: NamedBucketKey) -> int:
        return self._pending.get(key, 0)

    def reserve(self, key: NamedBucketKey, amount: int) -> None:
        self._pending[key] += amount

    def release(self, key: NamedBucketKey, amount: int) -> None:
        remaining = self._pending[key] - amount
        if remaining < 0:
            raise RuntimeError("Limiter pending reservation became negative.")
        if remaining:
            self._pending[key] = remaining
        else:
            self._pending.pop(key, None)


named_bucket_coordinator = NamedBucketCoordinator()
