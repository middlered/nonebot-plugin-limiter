import asyncio
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, TypeVar

from nonebot.message import run_postprocessor
from nonebot.params import Depends
from nonebot.typing import T_State

from .handle import _CommitAction, _Reservation

_INCREASER_STATE_KEY = "plugin_limiter:increaser"
_AsyncAction = Callable[[], Awaitable[None]]
_SnapshotT = TypeVar("_SnapshotT")


class _Increaser:
    def __init__(self) -> None:
        self._reservations: list[_Reservation[Any]] = []
        self._operation_lock = asyncio.Lock()

    def reserve(
        self,
        commit: _CommitAction[_SnapshotT],
        release: _AsyncAction,
    ) -> _Reservation[_SnapshotT]:
        reservation = _Reservation(commit, release)
        self._reservations.append(reservation)
        return reservation

    async def execute(self) -> None:
        """确认当前 Matcher 已登记的所有限流预占。

        Raises:
            RuntimeError: 任一预占已在 Matcher 清理阶段释放。
            LimiterStorageError: 命名 bucket 的数据库事务失败。
        """

        async with self._operation_lock:
            if any(reservation._released for reservation in self._reservations):
                raise RuntimeError("Cannot commit limiter reservations after they have been released.")

            for reservation in self._reservations:
                await reservation.commit()
            self._reservations.clear()

    async def release(self) -> None:
        async with self._operation_lock:
            for reservation in self._reservations:
                await reservation.release()


def _ensure_increaser(state: T_State) -> _Increaser:
    increaser = state.get(_INCREASER_STATE_KEY)
    if increaser is None:
        increaser = _Increaser()
        state[_INCREASER_STATE_KEY] = increaser
    assert isinstance(increaser, _Increaser)
    return increaser


def _reserve_increaser(
    state: T_State,
    commit: _CommitAction[_SnapshotT],
    release: _AsyncAction,
) -> _Reservation[_SnapshotT]:
    return _ensure_increaser(state).reserve(commit, release)


def get_increaser(state: T_State) -> _Increaser:
    increaser = state.get(_INCREASER_STATE_KEY)
    if not isinstance(increaser, _Increaser):
        raise KeyError("Cannot get increaser, make sure you have enabled `set_increaser` in cooldown policy.")
    return increaser


@run_postprocessor
async def _release_uncommitted_increaser(state: T_State) -> None:
    increaser = state.pop(_INCREASER_STATE_KEY, None)
    if isinstance(increaser, _Increaser):
        await increaser.release()


Increaser = Annotated[_Increaser, Depends(get_increaser, use_cache=False)]
