from collections.abc import AsyncGenerator, Callable
from typing import Any, cast
from unittest.mock import AsyncMock

from nonebot.exception import FinishedException
import pytest

_LIMITER_KINDS = ("fixed", "sliding", "leaky", "token")


def _make_limiter(
    kind: str,
    name: str,
    *,
    set_increaser: bool = True,
) -> tuple[
    Callable[..., AsyncGenerator[None, None]],
    dict[str, dict[str, Any]],
]:
    from nonebot_plugin_limiter import cooldown as cooldown_module
    from nonebot_plugin_limiter.entity import BYPASS_ENTITY

    def entity_id() -> str:
        return BYPASS_ENTITY

    if kind == "fixed":
        marker = cooldown_module.Cooldown(entity_id, 60, limit=1, set_increaser=set_increaser, name=name)
        registry = cooldown_module._FixWindowCooldownDict
    elif kind == "sliding":
        marker = cooldown_module.SlidingWindowCooldown(
            entity_id,
            60,
            limit=1,
            set_increaser=set_increaser,
            name=name,
        )
        registry = cooldown_module._SlidingWindowCooldownDict
    elif kind == "leaky":
        marker = cooldown_module.LeakyBucketCooldown(
            entity_id,
            capacity=1,
            leak_speed=0,
            pour_size=1,
            set_increaser=set_increaser,
            name=name,
        )
        registry = cooldown_module._LeakyBucketCooldownDict
    elif kind == "token":
        marker = cooldown_module.TokenBucketCooldown(
            entity_id,
            capacity=1,
            add_speed=0,
            consume_size=1,
            set_increaser=set_increaser,
            name=name,
        )
        registry = cooldown_module._TokenBucketCooldownDict
    else:
        raise AssertionError(f"Unsupported limiter kind: {kind}")

    assert marker.dependency is not None
    dependency = cast(Callable[..., AsyncGenerator[None, None]], marker.dependency)
    return dependency, registry


def _seed_available_usage(kind: str, bucket: dict[str, Any], entity_id: str) -> None:
    from nonebot_plugin_limiter import cooldown as cooldown_module

    now = cooldown_module.datetime.now(tz=cooldown_module._tz)
    if kind == "leaky":
        bucket[entity_id] = cooldown_module.LeakyBucketUsage(now, capacity=1, used=0)
    elif kind == "token":
        bucket[entity_id] = cooldown_module.TokenBucketUsage(now, capacity=1, available=1)


def _formal_usage(kind: str, usage: Any) -> int:
    if kind in {"fixed", "token"}:
        return cast(int, usage.available)
    if kind == "sliding":
        return len(usage.timestamps)
    if kind == "leaky":
        return cast(int, usage.used)
    raise AssertionError(f"Unsupported limiter kind: {kind}")


def _expected_formal_usage(kind: str) -> tuple[int, int]:
    if kind in {"fixed", "token"}:
        return 1, 0
    if kind in {"sliding", "leaky"}:
        return 0, 1
    raise AssertionError(f"Unsupported limiter kind: {kind}")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_bypass_provides_empty_increaser_without_usage(kind: str) -> None:
    from nonebot_plugin_limiter.entity import BYPASS_ENTITY
    from nonebot_plugin_limiter.handler import get_increaser

    name = f"test-bypass-{kind}"
    dependency, registry = _make_limiter(kind, name)
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("bypass must not reject"))
    generator = dependency(state, BYPASS_ENTITY, 1, reject_cb)

    try:
        await anext(generator)
        assert registry[name] == {}

        increaser = get_increaser(state)
        increaser.execute()
        increaser.execute()

        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        assert registry[name] == {}
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()
        registry.pop(name, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_immediate_limiter_does_not_provide_increaser(kind: str) -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    name = f"test-immediate-{kind}"
    entity_id = "user"
    dependency, registry = _make_limiter(kind, name, set_increaser=False)
    _seed_available_usage(kind, registry[name], entity_id)
    _, after = _expected_formal_usage(kind)
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("available usage must not reject"))
    generator = dependency(state, entity_id, 1, reject_cb)

    try:
        await anext(generator)
        usage = registry[name][entity_id]
        assert _formal_usage(kind, usage) == after
        assert usage.pending == 0
        with pytest.raises(KeyError, match="set_increaser"):
            get_increaser(state)

        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()
        registry.pop(name, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_execute_commits_delayed_usage_once(kind: str) -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    name = f"test-commit-{kind}"
    entity_id = "user"
    dependency, registry = _make_limiter(kind, name)
    _seed_available_usage(kind, registry[name], entity_id)
    before, after = _expected_formal_usage(kind)
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("available usage must not reject"))
    generator = dependency(state, entity_id, 1, reject_cb)

    try:
        await anext(generator)
        usage = registry[name][entity_id]
        assert _formal_usage(kind, usage) == before
        assert usage.pending == 1

        increaser = get_increaser(state)
        increaser.execute()
        assert _formal_usage(kind, usage) == after
        assert usage.pending == 0

        assert get_increaser(state) is increaser
        increaser.execute()
        assert _formal_usage(kind, usage) == after
        assert usage.pending == 0

        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()
        registry.pop(name, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_uncommitted_usage_is_released(kind: str) -> None:
    from nonebot_plugin_limiter.handler import get_increaser

    name = f"test-release-{kind}"
    entity_id = "user"
    dependency, registry = _make_limiter(kind, name)
    _seed_available_usage(kind, registry[name], entity_id)
    before, _ = _expected_formal_usage(kind)
    state = {}
    reject_cb = AsyncMock(side_effect=AssertionError("available usage must not reject"))
    generator = dependency(state, entity_id, 1, reject_cb)

    try:
        await anext(generator)
        usage = registry[name][entity_id]
        increaser = get_increaser(state)
        assert _formal_usage(kind, usage) == before
        assert usage.pending == 1

        await generator.aclose()
        assert _formal_usage(kind, usage) == before
        assert usage.pending == 0
        with pytest.raises(RuntimeError, match="released"):
            increaser.execute()
        reject_cb.assert_not_awaited()
    finally:
        await generator.aclose()
        registry.pop(name, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", _LIMITER_KINDS)
async def test_pending_usage_blocks_immediate_use_of_the_last_capacity(kind: str) -> None:
    name = f"test-pending-{kind}"
    entity_id = "user"
    dependency, registry = _make_limiter(kind, name)
    immediate_dependency, immediate_registry = _make_limiter(kind, name, set_increaser=False)
    assert immediate_registry is registry
    _seed_available_usage(kind, registry[name], entity_id)
    before, _ = _expected_formal_usage(kind)
    first_state = {}
    second_state = {}
    first_reject = AsyncMock(side_effect=AssertionError("first usage must not reject"))
    second_reject = AsyncMock()
    first_generator = dependency(first_state, entity_id, 1, first_reject)
    second_generator = immediate_dependency(second_state, entity_id, 1, second_reject)

    try:
        await anext(first_generator)
        usage = registry[name][entity_id]
        assert _formal_usage(kind, usage) == before
        assert usage.pending == 1

        with pytest.raises(FinishedException):
            await anext(second_generator)
        second_reject.assert_awaited_once_with()
        assert _formal_usage(kind, usage) == before
        assert usage.pending == 1

        await first_generator.aclose()
        assert _formal_usage(kind, usage) == before
        assert usage.pending == 0
        first_reject.assert_not_awaited()
    finally:
        await first_generator.aclose()
        await second_generator.aclose()
        registry.pop(name, None)
