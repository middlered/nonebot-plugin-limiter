import asyncio
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import uuid4

from nonebot.exception import FinishedException
import pytest

_LIMITER_KINDS = ("fixed", "sliding", "leaky", "token")


def _make_limiter(
    kind: str,
    name: str | None,
    *,
    set_increaser: bool = True,
) -> Callable[..., AsyncGenerator[None, None]]:
    from nonebot_plugin_limiter import cooldown as cooldown_module
    from nonebot_plugin_limiter.entity import BYPASS_ENTITY

    def entity_id() -> str:
        return BYPASS_ENTITY

    if kind == "fixed":
        marker = cooldown_module.Cooldown(entity_id, 60, limit=1, set_increaser=set_increaser, name=name)
    elif kind == "sliding":
        marker = cooldown_module.SlidingWindowCooldown(
            entity_id,
            60,
            limit=1,
            set_increaser=set_increaser,
            name=name,
        )
    elif kind == "leaky":
        marker = cooldown_module.LeakyBucketCooldown(
            entity_id,
            capacity=1,
            leak_speed=0,
            pour_size=1,
            set_increaser=set_increaser,
            name=name,
        )
    elif kind == "token":
        marker = cooldown_module.TokenBucketCooldown(
            entity_id,
            capacity=1,
            add_speed=0,
            consume_size=1,
            set_increaser=set_increaser,
            name=name,
        )
    else:
        raise AssertionError(f"Unsupported limiter kind: {kind}")

    assert marker.dependency is not None
    return cast(Callable[..., AsyncGenerator[None, None]], marker.dependency)


async def _seed_available_usage(kind: str, bucket_name: str, entity_id: str) -> None:
    from nonebot_plugin_limiter._state import (
        FixWindowUsage,
        LeakyBucketUsage,
        SlidingWindowUsage,
        TokenBucketUsage,
    )
    from nonebot_plugin_limiter.cooldown import _tz
    from nonebot_plugin_limiter.identity import identify_entity
    from nonebot_plugin_limiter.storage import get_limiter_storage

    now = datetime.now(tz=_tz)
    identity = identify_entity(entity_id)
    storage = get_limiter_storage()
    if kind == "fixed":
        await storage.mutate_fix_window(
            bucket_name,
            identity,
            FixWindowUsage(now, 1),
            lambda usage: setattr(usage, "available", 1),
        )
    elif kind == "sliding":
        await storage.mutate_sliding_window(
            bucket_name,
            identity,
            SlidingWindowUsage(),
            lambda usage: usage.timestamps.clear(),
        )
    elif kind == "leaky":
        await storage.mutate_leaky_bucket(
            bucket_name,
            identity,
            LeakyBucketUsage(now, 1, 0),
            lambda usage: setattr(usage, "used", 0),
        )
    elif kind == "token":
        await storage.mutate_token_bucket(
            bucket_name,
            identity,
            TokenBucketUsage(now, 1, 1),
            lambda usage: setattr(usage, "available", 1),
        )


async def _formal_usage(kind: str, bucket_name: str, entity_id: str) -> int:
    from nonebot_plugin_limiter._state import (
        FixWindowUsage,
        LeakyBucketUsage,
        SlidingWindowUsage,
        TokenBucketUsage,
    )
    from nonebot_plugin_limiter.cooldown import _tz
    from nonebot_plugin_limiter.identity import identify_entity
    from nonebot_plugin_limiter.storage import get_limiter_storage

    now = datetime.now(tz=_tz)
    identity = identify_entity(entity_id)
    storage = get_limiter_storage()
    if kind == "fixed":
        return await storage.mutate_fix_window(
            bucket_name,
            identity,
            FixWindowUsage(now, 1),
            lambda usage: usage.available,
        )
    if kind == "sliding":
        return await storage.mutate_sliding_window(
            bucket_name,
            identity,
            SlidingWindowUsage(),
            lambda usage: len(usage.timestamps),
        )
    if kind == "leaky":
        return await storage.mutate_leaky_bucket(
            bucket_name,
            identity,
            LeakyBucketUsage(now, 1, 0),
            lambda usage: usage.used,
        )
    if kind == "token":
        return await storage.mutate_token_bucket(
            bucket_name,
            identity,
            TokenBucketUsage(now, 1, 1),
            lambda usage: usage.available,
        )
    raise AssertionError(f"Unsupported limiter kind: {kind}")


def _expected_formal_usage(kind: str) -> tuple[int, int]:
    if kind in {"fixed", "token"}:
        return 1, 0
    if kind in {"sliding", "leaky"}:
        return 0, 1
    raise AssertionError(f"Unsupported limiter kind: {kind}")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_bypass_provides_empty_increaser_without_storage(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nonebot_plugin_limiter.entity import BYPASS_ENTITY
    from nonebot_plugin_limiter.handler import get_increaser

    class ForbiddenStorage:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"bypass accessed storage method {name}")

    monkeypatch.setattr("nonebot_plugin_limiter.storage._storage", ForbiddenStorage())
    dependency = _make_limiter(kind, f"test-bypass-{kind}-{uuid4()}")
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("bypass must not reject"))
    generator = dependency(state, BYPASS_ENTITY, 1, reject_cb)

    try:
        await anext(generator)
        increaser = get_increaser(state)
        await increaser.execute()
        await increaser.execute()
        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_immediate_named_limiter_commits_before_yield(kind: str) -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    name = f"test-immediate-{kind}-{uuid4()}"
    entity_id = "user"
    dependency = _make_limiter(kind, name, set_increaser=False)
    await _seed_available_usage(kind, name, entity_id)
    _, after = _expected_formal_usage(kind)
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("available usage must not reject"))
    generator = dependency(state, entity_id, 1, reject_cb)

    try:
        await anext(generator)
        assert await _formal_usage(kind, name, entity_id) == after
        with pytest.raises(KeyError, match="set_increaser"):
            get_increaser(state)
        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_execute_commits_delayed_named_usage_once(kind: str) -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    name = f"test-commit-{kind}-{uuid4()}"
    entity_id = "user"
    dependency = _make_limiter(kind, name)
    await _seed_available_usage(kind, name, entity_id)
    before, after = _expected_formal_usage(kind)
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("available usage must not reject"))
    generator = dependency(state, entity_id, 1, reject_cb)

    try:
        await anext(generator)
        assert await _formal_usage(kind, name, entity_id) == before

        increaser = get_increaser(state)
        await increaser.execute()
        assert await _formal_usage(kind, name, entity_id) == after
        await increaser.execute()
        assert await _formal_usage(kind, name, entity_id) == after

        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_uncommitted_named_usage_is_released(kind: str) -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    name = f"test-release-{kind}-{uuid4()}"
    entity_id = "user"
    dependency = _make_limiter(kind, name)
    await _seed_available_usage(kind, name, entity_id)
    before, _ = _expected_formal_usage(kind)
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("available usage must not reject"))
    generator = dependency(state, entity_id, 1, reject_cb)

    try:
        await anext(generator)
        increaser = get_increaser(state)
        assert await _formal_usage(kind, name, entity_id) == before

        await generator.aclose()
        assert await _formal_usage(kind, name, entity_id) == before
        with pytest.raises(RuntimeError, match="released"):
            await increaser.execute()
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_pending_named_usage_blocks_immediate_last_capacity(kind: str) -> None:
    name = f"test-pending-{kind}-{uuid4()}"
    entity_id = "user"
    dependency = _make_limiter(kind, name)
    immediate_dependency = _make_limiter(kind, name, set_increaser=False)
    await _seed_available_usage(kind, name, entity_id)
    before, _ = _expected_formal_usage(kind)
    first_reject = AsyncMock(side_effect=AssertionError("first usage must not reject"))
    second_reject = AsyncMock()
    first_generator = dependency({}, entity_id, 1, first_reject)
    second_generator = immediate_dependency({}, entity_id, 1, second_reject)

    try:
        await anext(first_generator)
        assert await _formal_usage(kind, name, entity_id) == before

        with pytest.raises(FinishedException):
            await anext(second_generator)
        second_reject.assert_awaited_once_with()
        assert await _formal_usage(kind, name, entity_id) == before

        await first_generator.aclose()
        first_reject.assert_not_awaited()
    finally:
        await first_generator.aclose()
        await second_generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_unnamed_bucket_never_accesses_storage(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ForbiddenStorage:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"unnamed bucket accessed storage method {name}")

    monkeypatch.setattr("nonebot_plugin_limiter.storage._storage", ForbiddenStorage())
    dependency = _make_limiter(kind, None, set_increaser=False)
    size = 0 if kind in {"leaky", "token"} else 1
    generator = dependency({}, "user", size, AsyncMock())
    try:
        await anext(generator)
    finally:
        await generator.aclose()


@pytest.mark.asyncio
async def test_named_storage_failure_is_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_limiter.storage import LimiterStorageError

    class FailingStorage:
        async def mutate_fix_window(self, *_args: Any, **_kwargs: Any) -> None:
            raise LimiterStorageError("database unavailable")

    monkeypatch.setattr("nonebot_plugin_limiter.storage._storage", FailingStorage())
    dependency = _make_limiter("fixed", f"test-failure-{uuid4()}", set_increaser=False)
    reject_cb = AsyncMock()
    generator = dependency({}, "user", 1, reject_cb)

    try:
        with pytest.raises(LimiterStorageError, match="database unavailable"):
            await anext(generator)
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()


@pytest.mark.asyncio
async def test_failed_delayed_commit_releases_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_limiter.handler import get_increaser
    import nonebot_plugin_limiter.storage as storage_module
    from nonebot_plugin_limiter.storage import LimiterStorageError

    name = f"test-failed-commit-{uuid4()}"
    entity_id = "user"
    await _seed_available_usage("fixed", name, entity_id)
    real_storage = storage_module._storage

    class FailCommitStorage:
        calls = 0

        async def mutate_fix_window(self, *args: Any, **kwargs: Any) -> Any:
            self.calls += 1
            if self.calls == 1:
                return await real_storage.mutate_fix_window(*args, **kwargs)
            raise LimiterStorageError("commit failed")

    monkeypatch.setattr(storage_module, "_storage", FailCommitStorage())
    state = {}
    generator = _make_limiter("fixed", name)(state, entity_id, 1, AsyncMock())
    try:
        await anext(generator)
        with pytest.raises(LimiterStorageError, match="commit failed"):
            await get_increaser(state).execute()
    finally:
        await generator.aclose()

    monkeypatch.setattr(storage_module, "_storage", real_storage)
    retry = _make_limiter("fixed", name, set_increaser=False)({}, entity_id, 1, AsyncMock())
    try:
        await anext(retry)
    finally:
        await retry.aclose()


@pytest.mark.asyncio
async def test_equal_unnamed_limiters_keep_independent_closure_state() -> None:
    first_dependency = _make_limiter("fixed", None, set_increaser=False)
    second_dependency = _make_limiter("fixed", None, set_increaser=False)

    first = first_dependency({}, "user", 1, AsyncMock())
    second = second_dependency({}, "user", 1, AsyncMock())
    try:
        await anext(first)
        await anext(second)
    finally:
        await first.aclose()
        await second.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_new_limiter_instance_observes_committed_database_state(kind: str) -> None:
    name = f"test-restart-{kind}-{uuid4()}"
    entity_id = "user"
    await _seed_available_usage(kind, name, entity_id)

    first = _make_limiter(kind, name, set_increaser=False)({}, entity_id, 1, AsyncMock())
    try:
        await anext(first)
    finally:
        await first.aclose()

    second_reject = AsyncMock()
    second = _make_limiter(kind, name, set_increaser=False)({}, entity_id, 1, second_reject)
    try:
        with pytest.raises(FinishedException):
            await anext(second)
        second_reject.assert_awaited_once_with()
    finally:
        await second.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_concurrent_immediate_named_requests_do_not_overspend(kind: str) -> None:
    name = f"test-concurrent-{kind}-{uuid4()}"
    entity_id = "user"
    await _seed_available_usage(kind, name, entity_id)
    dependency = _make_limiter(kind, name, set_increaser=False)
    reject_cb = AsyncMock()

    async def attempt() -> str:
        generator = dependency({}, entity_id, 1, reject_cb)
        try:
            await anext(generator)
            return "allowed"
        except FinishedException:
            return "rejected"
        finally:
            await generator.aclose()

    assert sorted(await asyncio.gather(attempt(), attempt())) == ["allowed", "rejected"]
    reject_cb.assert_awaited_once_with()
