import asyncio
import threading

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

    increaser.execute()
    increaser.execute()
    assert get_increaser(state) is increaser

    commits: list[str] = []
    increaser.reserve(lambda: commits.append("commit"), lambda: None)
    increaser.execute()
    assert commits == ["commit"]
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_released_reservation_blocks_the_entire_batch_commit() -> None:
    from nonebot_plugin_limiter.handler import _ensure_increaser

    actions: list[str] = []
    increaser = _ensure_increaser({})
    increaser.reserve(
        lambda: actions.append("active-commit"),
        lambda: actions.append("active-release"),
    )
    released_reservation = increaser.reserve(
        lambda: actions.append("released-commit"),
        lambda: actions.append("released-release"),
    )

    released_reservation.release()

    assert actions == ["released-release"]
    with pytest.raises(RuntimeError, match="released"):
        await asyncio.wait_for(asyncio.to_thread(increaser.execute), timeout=1)
    assert actions == ["released-release"]

    increaser.release()
    increaser.release()
    assert actions == ["released-release", "active-release"]
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_worker_thread_execute_commits_on_owner_loop() -> None:
    from nonebot_plugin_limiter.handler import _ensure_increaser

    owner_thread_id = threading.get_ident()
    commit_thread_ids: list[int] = []
    commit_started = threading.Event()
    allow_commit = threading.Event()
    execute_returned = threading.Event()
    observer_results: list[tuple[bool, bool]] = []
    increaser = _ensure_increaser({})

    def commit() -> None:
        commit_thread_ids.append(threading.get_ident())
        commit_started.set()
        if not allow_commit.wait(timeout=1):
            raise TimeoutError("test did not allow commit to finish")

    increaser.reserve(
        commit,
        lambda: None,
    )

    def observe_worker() -> None:
        commit_did_start = commit_started.wait(timeout=1)
        returned_before_commit_finished = execute_returned.wait(timeout=0.1) if commit_did_start else False
        observer_results.append((commit_did_start, returned_before_commit_finished))
        allow_commit.set()

    observer = threading.Thread(target=observe_worker)
    observer.start()

    def execute_from_worker() -> None:
        increaser.execute()
        execute_returned.set()

    try:
        await asyncio.wait_for(asyncio.to_thread(execute_from_worker), timeout=2)
    finally:
        allow_commit.set()
        observer.join(timeout=1)

    assert commit_thread_ids == [owner_thread_id]
    assert observer_results == [(True, False)]
    assert not observer.is_alive()
    assert execute_returned.is_set()
    increaser.execute()
    assert commit_thread_ids == [owner_thread_id]


@pytest.mark.asyncio
async def test_worker_thread_execute_propagates_commit_timeout() -> None:
    from nonebot_plugin_limiter.handler import _ensure_increaser

    def raise_timeout() -> None:
        raise TimeoutError("commit callback timed out")

    increaser = _ensure_increaser({})
    increaser.reserve(raise_timeout, lambda: None)

    with pytest.raises(TimeoutError, match="commit callback timed out"):
        await asyncio.wait_for(asyncio.to_thread(increaser.execute), timeout=1)
