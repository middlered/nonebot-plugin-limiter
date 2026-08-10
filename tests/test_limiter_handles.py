from collections.abc import AsyncGenerator, Callable
from datetime import datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

_LIMITER_KINDS = ("fixed", "sliding", "leaky", "token")


def test_limiter_handle_supports_named_and_parameterless_dependencies() -> None:
    from nonebot.dependencies import Dependent
    from nonebot.matcher import Matcher

    from nonebot_plugin_limiter import Cooldown, FixedWindowHandle, UserScope

    marker = Cooldown(UserScope(), 60, limit=2)

    async def named_handler(quota: FixedWindowHandle = marker) -> None:
        del quota

    async def parameterless_handler() -> None:
        pass

    named = Dependent.parse(call=named_handler, allow_types=Matcher.HANDLER_PARAM_TYPES)
    parameterless = Dependent.parse(
        call=parameterless_handler,
        parameterless=[marker],
        allow_types=Matcher.HANDLER_PARAM_TYPES,
    )

    assert [field.name for field in named.params] == ["quota"]
    assert named.parameterless == ()
    assert named.params[0].annotation is FixedWindowHandle
    assert parameterless.params == ()
    assert len(parameterless.parameterless) == 1


def _make_limiter(
    kind: str,
    name: str | None,
    *,
    set_increaser: bool,
    period: int = 60,
    speed: int = 0,
) -> tuple[Callable[..., AsyncGenerator[Any, None]], int]:
    from nonebot_plugin_limiter import (
        Cooldown,
        LeakyBucketCooldown,
        SlidingWindowCooldown,
        TokenBucketCooldown,
    )

    if kind == "fixed":
        marker = Cooldown(lambda: "unused", period, limit=2, name=name, set_increaser=set_increaser)
        resolved_size = 2
    elif kind == "sliding":
        marker = SlidingWindowCooldown(lambda: "unused", period, limit=2, name=name, set_increaser=set_increaser)
        resolved_size = 2
    elif kind == "leaky":
        marker = LeakyBucketCooldown(
            lambda: "unused",
            capacity=2,
            leak_speed=speed,
            pour_size=1,
            name=name,
            set_increaser=set_increaser,
        )
        resolved_size = 1
    elif kind == "token":
        marker = TokenBucketCooldown(
            lambda: "unused",
            capacity=2,
            add_speed=speed,
            consume_size=1,
            name=name,
            set_increaser=set_increaser,
        )
        resolved_size = 1
    else:
        raise AssertionError(f"Unsupported limiter kind: {kind}")

    assert marker.dependency is not None
    return cast(Callable[..., AsyncGenerator[Any, None]], marker.dependency), resolved_size


def _assert_available_snapshot(kind: str, snapshot: Any, *, pending: int) -> None:
    from nonebot_plugin_limiter import (
        FixedWindowSnapshot,
        LeakyBucketSnapshot,
        SlidingWindowSnapshot,
        TokenBucketSnapshot,
    )

    if kind == "fixed":
        assert snapshot == FixedWindowSnapshot(1, pending)
    elif kind == "sliding":
        assert snapshot == SlidingWindowSnapshot(1, pending)
    elif kind == "leaky":
        assert snapshot == LeakyBucketSnapshot(0 if pending else 1, 1, pending)
    elif kind == "token":
        assert snapshot == TokenBucketSnapshot(1, pending)
    else:
        raise AssertionError(f"Unsupported limiter kind: {kind}")


async def _stored_value(kind: str, name: str, entity_id: str) -> int:
    from nonebot_plugin_limiter.identity import identify_entity
    from nonebot_plugin_limiter.storage import get_limiter_storage

    identity = identify_entity(entity_id)
    storage = get_limiter_storage()
    if kind == "fixed":
        usage = await storage.read_fix_window(name, identity)
        assert usage is not None
        return usage.available
    if kind == "sliding":
        usage = await storage.read_sliding_window(name, identity)
        assert usage is not None
        return len(usage.timestamps)
    if kind == "leaky":
        usage = await storage.read_leaky_bucket(name, identity)
        assert usage is not None
        return usage.used
    if kind == "token":
        usage = await storage.read_token_bucket(name, identity)
        assert usage is not None
        return usage.available
    raise AssertionError(f"Unsupported limiter kind: {kind}")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
@pytest.mark.parametrize("named", [False, True])
async def test_immediate_handle_execute_is_read_only(kind: str, named: bool) -> None:
    from nonebot_plugin_limiter import (
        FixedWindowHandle,
        LeakyBucketHandle,
        SlidingWindowHandle,
        TokenBucketHandle,
    )

    handle_types = {
        "fixed": FixedWindowHandle,
        "sliding": SlidingWindowHandle,
        "leaky": LeakyBucketHandle,
        "token": TokenBucketHandle,
    }
    name = f"test-handle-immediate-{kind}-{uuid4()}" if named else None
    dependency, resolved_size = _make_limiter(kind, name, set_increaser=False)
    generator = dependency({}, "user", resolved_size, AsyncMock())

    try:
        handle = await anext(generator)
        assert isinstance(handle, handle_types[kind])
        first = await handle.execute()
        second = await handle.execute()
        current = await handle.read()
        _assert_available_snapshot(kind, first, pending=0)
        assert second == first
        assert current == first

        if name is not None:
            expected = 1
            assert await _stored_value(kind, name, "user") == expected
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
@pytest.mark.parametrize("named", [False, True])
async def test_delayed_handle_reads_pending_and_executes_only_once(kind: str, named: bool) -> None:
    name = f"test-handle-delayed-{kind}-{uuid4()}" if named else None
    dependency, resolved_size = _make_limiter(kind, name, set_increaser=True)
    generator = dependency({}, "user", resolved_size, AsyncMock())

    try:
        handle = await anext(generator)
        before = await handle.read()
        _assert_available_snapshot(kind, before, pending=1)

        committed = await handle.execute()
        repeated = await handle.execute()
        _assert_available_snapshot(kind, committed, pending=0)
        assert repeated == committed
        assert await handle.read() == committed

        if name is not None:
            assert await _stored_value(kind, name, "user") == 1
    finally:
        await generator.aclose()


@pytest.mark.asyncio
async def test_one_handle_execute_does_not_commit_other_reservation() -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    first_name = f"test-handle-first-{uuid4()}"
    second_name = f"test-handle-second-{uuid4()}"
    first_dependency, resolved_size = _make_limiter("fixed", first_name, set_increaser=True)
    second_dependency, _ = _make_limiter("fixed", second_name, set_increaser=True)
    state = {}
    first_generator = first_dependency(state, "user", resolved_size, AsyncMock())
    second_generator = second_dependency(state, "user", resolved_size, AsyncMock())

    try:
        first_handle = await anext(first_generator)
        second_handle = await anext(second_generator)

        assert (await first_handle.execute()).remaining == 1
        assert await _stored_value("fixed", first_name, "user") == 1
        assert await _stored_value("fixed", second_name, "user") == 2
        assert (await second_handle.read()).pending == 1

        await get_increaser(state).execute()
        assert await _stored_value("fixed", second_name, "user") == 1
        assert (await second_handle.execute()).remaining == 1
    finally:
        await first_generator.aclose()
        await second_generator.aclose()


@pytest.mark.asyncio
async def test_handle_keeps_resolved_dynamic_limit() -> None:
    from nonebot_plugin_limiter import Cooldown

    async def dynamic_limit() -> int:
        return 99

    marker = Cooldown(lambda: "unused", 60, limit=dynamic_limit, set_increaser=True)
    assert marker.dependency is not None
    dependency = cast(Callable[..., AsyncGenerator[Any, None]], marker.dependency)
    generator = dependency({}, "user", 3, AsyncMock())

    try:
        handle = await anext(generator)
        assert (await handle.read()).remaining == 2
        assert (await handle.execute()).remaining == 2
    finally:
        await generator.aclose()


@pytest.mark.asyncio
async def test_failed_handle_commit_can_be_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_limiter.storage import LimiterStorageError, get_limiter_storage

    name = f"test-handle-retry-{uuid4()}"
    dependency, resolved_size = _make_limiter("fixed", name, set_increaser=True)
    generator = dependency({}, "user", resolved_size, AsyncMock())
    handle = await anext(generator)
    storage = get_limiter_storage()
    mutate = storage.mutate_fix_window

    async def fail_commit(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise LimiterStorageError("commit failed")

    try:
        monkeypatch.setattr(storage, "mutate_fix_window", fail_commit)
        with pytest.raises(LimiterStorageError, match="commit failed"):
            await handle.execute()
        assert (await handle.read()).pending == 1

        monkeypatch.setattr(storage, "mutate_fix_window", mutate)
        assert (await handle.execute()).remaining == 1
        assert await _stored_value("fixed", name, "user") == 1
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_named_handle_read_does_not_recreate_missing_state(kind: str) -> None:
    from nonebot_plugin_orm import get_session

    from nonebot_plugin_limiter.model import (
        FixWindowState,
        LeakyBucketState,
        SlidingWindowEntry,
        SlidingWindowState,
        TokenBucketState,
    )

    models = {
        "fixed": FixWindowState,
        "sliding": SlidingWindowState,
        "leaky": LeakyBucketState,
        "token": TokenBucketState,
    }
    name = f"test-handle-missing-{kind}-{uuid4()}"
    dependency, resolved_size = _make_limiter(kind, name, set_increaser=False)
    generator = dependency({}, "user", resolved_size, AsyncMock())

    try:
        handle = await anext(generator)
        async with get_session() as session, session.begin():
            if kind == "sliding":
                state_id = await session.scalar(
                    select(SlidingWindowState.id).where(SlidingWindowState.bucket_name == name)
                )
                if state_id is not None:
                    await session.execute(delete(SlidingWindowEntry).where(SlidingWindowEntry.state_id == state_id))
            await session.execute(delete(models[kind]).where(models[kind].bucket_name == name))

        snapshot = await handle.read()
        if kind in {"fixed", "sliding"}:
            assert snapshot.remaining == 2
        elif kind == "leaky":
            assert snapshot.used == 0
            assert snapshot.available_capacity == 2
        else:
            assert snapshot.available_tokens == 2

        async with get_session() as session:
            assert await session.scalar(select(models[kind]).where(models[kind].bucket_name == name)) is None
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_named_handle_virtual_refresh_does_not_persist(kind: str) -> None:
    from nonebot_plugin_limiter._state import (
        FixWindowUsage,
        LeakyBucketUsage,
        SlidingWindowUsage,
        TokenBucketUsage,
    )
    from nonebot_plugin_limiter.cooldown import _tz
    from nonebot_plugin_limiter.identity import identify_entity
    from nonebot_plugin_limiter.storage import get_limiter_storage

    name = f"test-handle-refresh-{kind}-{uuid4()}"
    entity_id = "user"
    dependency, resolved_size = _make_limiter(kind, name, set_increaser=False, period=1, speed=2)
    generator = dependency({}, entity_id, resolved_size, AsyncMock())
    storage = get_limiter_storage()
    identity = identify_entity(entity_id)
    old = datetime.now(tz=_tz) - timedelta(seconds=5)

    try:
        handle = await anext(generator)
        if kind == "fixed":
            await storage.mutate_fix_window(
                name,
                identity,
                FixWindowUsage(old, 0),
                lambda usage: (setattr(usage, "start_time", old), setattr(usage, "available", 0)),
            )
            before = await storage.read_fix_window(name, identity)
        elif kind == "sliding":
            await storage.mutate_sliding_window(
                name,
                identity,
                SlidingWindowUsage(),
                lambda usage: (usage.timestamps.clear(), usage.timestamps.append(old)),
            )
            before = await storage.read_sliding_window(name, identity)
        elif kind == "leaky":
            await storage.mutate_leaky_bucket(
                name,
                identity,
                LeakyBucketUsage(old, 2, 2),
                lambda usage: (
                    setattr(usage, "last_update_time", old),
                    setattr(usage, "capacity", 2),
                    setattr(usage, "used", 2),
                ),
            )
            before = await storage.read_leaky_bucket(name, identity)
        else:
            await storage.mutate_token_bucket(
                name,
                identity,
                TokenBucketUsage(old, 2, 0),
                lambda usage: (
                    setattr(usage, "last_update_time", old),
                    setattr(usage, "capacity", 2),
                    setattr(usage, "available", 0),
                ),
            )
            before = await storage.read_token_bucket(name, identity)

        snapshot = await handle.read()
        if kind in {"fixed", "sliding"}:
            assert snapshot.remaining == 2
        elif kind == "leaky":
            assert snapshot.used == 0
            assert snapshot.available_capacity == 2
        else:
            assert snapshot.available_tokens == 2

        if kind == "fixed":
            after = await storage.read_fix_window(name, identity)
        elif kind == "sliding":
            after = await storage.read_sliding_window(name, identity)
        elif kind == "leaky":
            after = await storage.read_leaky_bucket(name, identity)
        else:
            after = await storage.read_token_bucket(name, identity)
        assert after == before
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_bypass_handle_has_no_storage_side_effect(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_limiter import BYPASS_ENTITY
    from nonebot_plugin_limiter.handler import get_increaser

    class ForbiddenStorage:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"bypass accessed storage method {name}")

    monkeypatch.setattr("nonebot_plugin_limiter.storage._storage", ForbiddenStorage())
    dependency, resolved_size = _make_limiter(kind, f"test-handle-bypass-{uuid4()}", set_increaser=True)
    state = {}
    generator = dependency(state, BYPASS_ENTITY, resolved_size, AsyncMock())

    try:
        handle = await anext(generator)
        assert (await handle.read()).bypassed
        assert (await handle.execute()).bypassed
        await get_increaser(state).execute()
    finally:
        await generator.aclose()


@pytest.mark.asyncio
async def test_handle_execute_after_release_fails_without_consuming() -> None:
    dependency, resolved_size = _make_limiter("fixed", None, set_increaser=True)
    generator = dependency({}, "user", resolved_size, AsyncMock())
    handle = await anext(generator)

    await generator.aclose()

    with pytest.raises(RuntimeError, match="released"):
        await handle.execute()
    assert (await handle.read()).remaining == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name",
    ["read_fix_window", "read_sliding_window", "read_leaky_bucket", "read_token_bucket"],
)
async def test_read_failure_is_wrapped_with_original_cause(
    method_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nonebot_plugin_limiter.identity import EntityIdentity
    from nonebot_plugin_limiter.storage import LimiterStorageError, OrmLimiterStorage

    def fail_session():
        raise OSError("database path unavailable")

    monkeypatch.setattr("nonebot_plugin_limiter.storage.get_session", fail_session)
    method = getattr(OrmLimiterStorage(), method_name)

    with pytest.raises(LimiterStorageError) as error:
        await method("failure", EntityIdentity("custom-v1", "user"))

    assert isinstance(error.value.__cause__, OSError)
