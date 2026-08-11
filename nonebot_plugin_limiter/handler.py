import asyncio
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Annotated

from nonebot.message import run_postprocessor
from nonebot.params import Depends
from nonebot.typing import T_State

_INCREASER_STATE_KEY = "plugin_limiter:increaser"


class _Reservation:
    def __init__(
        self,
        commit: Callable[[], None],
        release: Callable[[], None],
        run_on_owner_loop: Callable[[Callable[[], None]], None],
    ) -> None:
        self._commit_action = commit
        self._release_action = release
        self._run_on_owner_loop = run_on_owner_loop
        self._active = True
        self._released = False

    def _commit(self) -> None:
        if not self._active:
            return
        self._commit_action()
        self._active = False

    def _release(self) -> None:
        if not self._active:
            return
        self._release_action()
        self._active = False
        self._released = True

    def release(self) -> None:
        self._run_on_owner_loop(self._release)


class _Increaser:
    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._reservations: list[_Reservation] = []

    def reserve(self, commit: Callable[[], None], release: Callable[[], None]) -> _Reservation:
        reservation = _Reservation(commit, release, self._run_on_owner_loop)
        self._reservations.append(reservation)
        return reservation

    def execute(self) -> None:
        """确认当前 Matcher 已登记的所有限流预占。

        同步 Handler 中调用时会等待所属事件循环完成提交；reservation 回调的异常会返回调用线程。

        Raises:
            RuntimeError: 预占已经释放，或 Increaser 所属事件循环已经关闭或停止运行。
        """

        self._run_on_owner_loop(self._commit_reservations)

    def _commit_reservations(self) -> None:
        if any(reservation._released for reservation in self._reservations):
            raise RuntimeError("Cannot commit limiter reservations after they have been released.")

        for reservation in self._reservations:
            reservation._commit()
        self._reservations.clear()

    def release(self) -> None:
        self._run_on_owner_loop(self._release_reservations)

    def _release_reservations(self) -> None:
        for reservation in self._reservations:
            reservation._release()

    def _run_on_owner_loop(self, action: Callable[[], None]) -> None:
        """在创建 Increaser 的事件循环中同步执行状态变更。

        Args:
            action: 要在所属事件循环中执行的无参状态变更。

        Raises:
            RuntimeError: 所属事件循环已经关闭或停止运行。
        """

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is self._loop:
            action()
            return

        if self._loop.is_closed() or not self._loop.is_running():
            raise RuntimeError("Cannot operate increaser after its event loop has stopped.")

        result: Future[None] = Future()

        def _execute_action() -> None:
            if not result.set_running_or_notify_cancel():
                return
            try:
                action()
            except BaseException as exception:
                result.set_exception(exception)
                if not isinstance(exception, Exception):
                    raise
            else:
                result.set_result(None)

        try:
            self._loop.call_soon_threadsafe(_execute_action)
        except RuntimeError as exception:
            raise RuntimeError("Cannot operate increaser after its event loop has stopped.") from exception

        # 避免事件循环在 callback 入队后停止，把 AnyIO 工作线程永久留在 Future.result() 中
        while True:
            try:
                result.result(timeout=0.1)
                return
            except FutureTimeoutError:
                if result.done():
                    result.result()
                    return
                if self._loop.is_running():
                    continue
                if result.cancel():
                    raise RuntimeError("Cannot operate increaser after its event loop has stopped.")


def _ensure_increaser(state: T_State) -> _Increaser:
    increaser = state.get(_INCREASER_STATE_KEY)
    if increaser is None:
        increaser = _Increaser()
        state[_INCREASER_STATE_KEY] = increaser
    elif not isinstance(increaser, _Increaser):
        raise TypeError(f"{_INCREASER_STATE_KEY!r} contains an invalid value")
    return increaser


def get_increaser(state: T_State) -> _Increaser:
    increaser = state.get(_INCREASER_STATE_KEY)
    if increaser is None:
        raise KeyError("Cannot get increaser, make sure you have enabled `set_increaser` in cooldown policy.")
    if not isinstance(increaser, _Increaser):
        raise TypeError(f"{_INCREASER_STATE_KEY!r} contains an invalid value")
    return increaser


@run_postprocessor
async def _release_uncommitted_increaser(state: T_State) -> None:
    increaser = state.pop(_INCREASER_STATE_KEY, None)
    if isinstance(increaser, _Increaser):
        increaser.release()


Increaser = Annotated[_Increaser, Depends(get_increaser, use_cache=False)]
