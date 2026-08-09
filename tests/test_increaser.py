import asyncio
import inspect

import pytest


def test_get_increaser_rejects_missing_capability() -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    with pytest.raises(KeyError, match="set_increaser"):
        get_increaser({})


@pytest.mark.asyncio
async def test_empty_increaser_is_a_stable_capability() -> None:
    from nonebot_plugin_limiter.handler import _ensure_increaser, get_increaser

    state = {}
    increaser = _ensure_increaser(state)

    assert _ensure_increaser(state) is increaser
    assert get_increaser(state) is increaser
    assert inspect.iscoroutinefunction(increaser.execute)

    await increaser.execute()
    await increaser.execute()

    commits: list[str] = []

    async def commit() -> None:
        commits.append("commit")

    async def release() -> None:
        pass

    increaser.reserve(commit, release)
    await increaser.execute()
    assert commits == ["commit"]


@pytest.mark.asyncio
async def test_released_reservation_blocks_the_entire_batch_commit() -> None:
    from nonebot_plugin_limiter.handler import _ensure_increaser

    actions: list[str] = []
    increaser = _ensure_increaser({})

    async def active_commit() -> None:
        actions.append("active-commit")

    async def active_release() -> None:
        actions.append("active-release")

    async def released_commit() -> None:
        actions.append("released-commit")

    async def released_release() -> None:
        actions.append("released-release")

    increaser.reserve(active_commit, active_release)
    released_reservation = increaser.reserve(released_commit, released_release)

    await released_reservation.release()

    assert actions == ["released-release"]
    with pytest.raises(RuntimeError, match="released"):
        await increaser.execute()
    assert actions == ["released-release"]

    await increaser.release()
    await increaser.release()
    assert actions == ["released-release", "active-release"]


@pytest.mark.asyncio
async def test_execute_waits_for_async_commit() -> None:
    from nonebot_plugin_limiter.handler import _ensure_increaser

    commit_started = asyncio.Event()
    allow_commit = asyncio.Event()
    commit_finished = asyncio.Event()
    increaser = _ensure_increaser({})

    async def commit() -> None:
        commit_started.set()
        await allow_commit.wait()
        commit_finished.set()

    async def release() -> None:
        pass

    increaser.reserve(commit, release)
    execute_task = asyncio.create_task(increaser.execute())

    await commit_started.wait()
    assert not execute_task.done()
    allow_commit.set()
    await execute_task
    assert commit_finished.is_set()


@pytest.mark.asyncio
async def test_execute_propagates_commit_error() -> None:
    from nonebot_plugin_limiter.handler import _ensure_increaser

    async def raise_timeout() -> None:
        raise TimeoutError("commit callback timed out")

    async def release() -> None:
        pass

    increaser = _ensure_increaser({})
    increaser.reserve(raise_timeout, release)

    with pytest.raises(TimeoutError, match="commit callback timed out"):
        await increaser.execute()
