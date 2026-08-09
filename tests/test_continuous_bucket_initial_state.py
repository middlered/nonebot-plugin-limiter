from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid4

from nonebot.exception import FinishedException
import pytest


def _make_limiter(
    kind: str,
    name: str | None,
    *,
    set_increaser: bool,
) -> Callable[..., AsyncGenerator[None, None]]:
    from nonebot_plugin_limiter import cooldown as cooldown_module
    from nonebot_plugin_limiter.entity import BYPASS_ENTITY

    def entity_id() -> str:
        return BYPASS_ENTITY

    if kind == "leaky":
        marker = cooldown_module.LeakyBucketCooldown(
            entity_id,
            capacity=2,
            leak_speed=0,
            pour_size=1,
            set_increaser=set_increaser,
            name=name,
        )
    elif kind == "token":
        marker = cooldown_module.TokenBucketCooldown(
            entity_id,
            capacity=2,
            add_speed=0,
            consume_size=1,
            set_increaser=set_increaser,
            name=name,
        )
    else:
        raise AssertionError(f"Unsupported limiter kind: {kind}")

    assert marker.dependency is not None
    return cast(Callable[..., AsyncGenerator[None, None]], marker.dependency)


async def _attempt(
    dependency: Callable[..., AsyncGenerator[None, None]],
    reject_cb: AsyncMock,
    *,
    set_increaser: bool,
) -> bool:
    from nonebot_plugin_limiter.handler import get_increaser

    state = {}
    generator = dependency(state, "user", 1, reject_cb)
    try:
        await anext(generator)
        if set_increaser:
            await get_increaser(state).execute()
        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        return True
    except FinishedException:
        return False
    finally:
        await generator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["leaky", "token"])
@pytest.mark.parametrize("named", [False, True])
@pytest.mark.parametrize("set_increaser", [False, True])
async def test_new_continuous_bucket_allows_capacity_burst(
    kind: str,
    named: bool,
    set_increaser: bool,
) -> None:
    name = f"test-initial-{kind}-{uuid4()}" if named else None
    dependency = _make_limiter(kind, name, set_increaser=set_increaser)
    reject_cb = AsyncMock()

    results = [await _attempt(dependency, reject_cb, set_increaser=set_increaser) for _ in range(3)]

    assert results == [True, True, False]
    reject_cb.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["leaky", "token"])
async def test_existing_named_continuous_bucket_keeps_stored_balance(kind: str) -> None:
    from nonebot_plugin_limiter._state import LeakyBucketUsage, TokenBucketUsage
    from nonebot_plugin_limiter.cooldown import _tz
    from nonebot_plugin_limiter.identity import identify_entity
    from nonebot_plugin_limiter.storage import get_limiter_storage

    name = f"test-existing-{kind}-{uuid4()}"
    now = datetime.now(tz=_tz)
    identity = identify_entity("user")
    storage = get_limiter_storage()
    if kind == "leaky":
        await storage.mutate_leaky_bucket(
            name,
            identity,
            LeakyBucketUsage(now, 2, 2),
            lambda usage: usage,
        )
    else:
        await storage.mutate_token_bucket(
            name,
            identity,
            TokenBucketUsage(now, 2, 0),
            lambda usage: usage,
        )

    dependency = _make_limiter(kind, name, set_increaser=False)
    reject_cb = AsyncMock()

    assert not await _attempt(dependency, reject_cb, set_increaser=False)
    reject_cb.assert_awaited_once_with()
