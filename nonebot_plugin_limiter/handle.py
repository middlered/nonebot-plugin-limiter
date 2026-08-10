import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar, cast

_SnapshotT = TypeVar("_SnapshotT")
_ReadAction = Callable[[], Awaitable[_SnapshotT]]
_CommitAction = Callable[[], Awaitable[_SnapshotT]]
_ReleaseAction = Callable[[], Awaitable[None]]
_MISSING = object()


@dataclass(frozen=True, slots=True)
class FixedWindowSnapshot:
    remaining: int | None
    pending: int = 0

    @property
    def bypassed(self) -> bool:
        return self.remaining is None


@dataclass(frozen=True, slots=True)
class SlidingWindowSnapshot:
    remaining: int | None
    pending: int = 0

    @property
    def bypassed(self) -> bool:
        return self.remaining is None


@dataclass(frozen=True, slots=True)
class LeakyBucketSnapshot:
    used: int | None
    available_capacity: int | None
    pending: int = 0

    @property
    def bypassed(self) -> bool:
        return self.used is None


@dataclass(frozen=True, slots=True)
class TokenBucketSnapshot:
    available_tokens: int | None
    pending: int = 0

    @property
    def bypassed(self) -> bool:
        return self.available_tokens is None


class _Reservation(Generic[_SnapshotT]):
    def __init__(self, commit: _CommitAction[_SnapshotT], release: _ReleaseAction) -> None:
        self._commit_action = commit
        self._release_action = release
        self._active = True
        self._released = False
        self._result: _SnapshotT | object = _MISSING
        self._operation_lock = asyncio.Lock()

    async def commit(self) -> _SnapshotT:
        async with self._operation_lock:
            if self._released:
                raise RuntimeError("Cannot commit a limiter reservation after it has been released.")
            if not self._active:
                return cast(_SnapshotT, self._result)

            result = await self._commit_action()
            self._result = result
            self._active = False
            return result

    async def release(self) -> None:
        async with self._operation_lock:
            if not self._active:
                return
            await self._release_action()
            self._active = False
            self._released = True


class LimiterHandle(Generic[_SnapshotT]):
    """控制一次具体 limiter 准入并读取其状态。"""

    def __init__(
        self,
        read: _ReadAction[_SnapshotT],
        reservation: _Reservation[_SnapshotT] | None = None,
        committed: _SnapshotT | object = _MISSING,
    ) -> None:
        self._read_action = read
        self._reservation = reservation
        self._committed = committed

    async def execute(self) -> _SnapshotT:
        """提交当前 limiter 的预占并返回提交后的状态。

        Returns:
            当前算法的状态快照。延迟计数时只提交当前 Handle；立即计数时返回准入完成时的
            快照；bypass Handle 返回数值字段为 `None` 的快照。重复调用不会再次消费。

        Raises:
            RuntimeError: reservation 已在 Matcher 清理阶段释放。
            LimiterStorageError: 命名 bucket 的数据库事务或读取失败。
        """

        if self._reservation is not None:
            return await self._reservation.commit()
        if self._committed is not _MISSING:
            return cast(_SnapshotT, self._committed)
        return await self.read()

    async def read(self) -> _SnapshotT:
        """只读当前 limiter 的有效状态，不创建或修改额度记录。

        Returns:
            当前算法的状态快照。可用量已经扣除本进程内未提交的预占，`pending` 字段同时
            给出这部分预占的算法单位。
        """

        return await self._read_action()


class FixedWindowHandle(LimiterHandle[FixedWindowSnapshot]):
    pass


class SlidingWindowHandle(LimiterHandle[SlidingWindowSnapshot]):
    pass


class LeakyBucketHandle(LimiterHandle[LeakyBucketSnapshot]):
    pass


class TokenBucketHandle(LimiterHandle[TokenBucketSnapshot]):
    pass
