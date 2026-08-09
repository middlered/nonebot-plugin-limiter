from datetime import datetime
import importlib.util
import json

import pytest
from sqlalchemy import UniqueConstraint, select


def test_named_state_models_use_versioned_composite_identity() -> None:
    from nonebot_plugin_limiter.model import (
        FixWindowState,
        LeakyBucketState,
        SlidingWindowState,
        TokenBucketState,
    )

    expected_columns = {"bucket_name", "entity_key_schema", "entity_key"}
    for model in (FixWindowState, SlidingWindowState, LeakyBucketState, TokenBucketState):
        constraints = {
            frozenset(column.name for column in constraint.columns)
            for constraint in model.__table__.constraints
            if isinstance(constraint, UniqueConstraint)
        }
        assert expected_columns in constraints
        assert model.__table__.info["bind_key"] == "nonebot_plugin_limiter"


@pytest.mark.asyncio
async def test_same_entity_key_in_different_schemas_is_independent() -> None:
    from nonebot_plugin_orm import get_session

    from nonebot_plugin_limiter._state import FixWindowUsage
    from nonebot_plugin_limiter.cooldown import _tz
    from nonebot_plugin_limiter.identity import EntityIdentity
    from nonebot_plugin_limiter.model import FixWindowState
    from nonebot_plugin_limiter.storage import get_limiter_storage

    bucket_name = "schema-boundary"
    entity_key = "same-key"
    initial = FixWindowUsage(datetime.now(tz=_tz), 1)
    storage = get_limiter_storage()

    await storage.mutate_fix_window(
        bucket_name,
        EntityIdentity("custom-v1", entity_key),
        initial,
        lambda usage: setattr(usage, "available", 0),
    )
    await storage.mutate_fix_window(
        bucket_name,
        EntityIdentity("future-v2", entity_key),
        initial,
        lambda usage: setattr(usage, "available", 2),
    )

    async with get_session() as session:
        rows = (
            await session.scalars(
                select(FixWindowState).where(
                    FixWindowState.bucket_name == bucket_name,
                    FixWindowState.entity_key == entity_key,
                )
            )
        ).all()

    assert {(row.entity_key_schema, row.available) for row in rows} == {
        ("custom-v1", 0),
        ("future-v2", 2),
    }


def test_legacy_json_api_and_config_are_removed() -> None:
    import nonebot_plugin_limiter
    from nonebot_plugin_limiter.config import Config

    assert not hasattr(nonebot_plugin_limiter, "load_usage_data")
    assert not hasattr(nonebot_plugin_limiter, "save_usage_data")
    assert importlib.util.find_spec("nonebot_plugin_limiter.persist") is None
    assert "cooldown_enable_persistence" not in Config.model_fields
    assert "cooldown_save_interval" not in Config.model_fields


def test_plugin_metadata_extra_is_json_serializable() -> None:
    from nonebot_plugin_limiter import __plugin_meta__

    json.dumps(__plugin_meta__.extra)


@pytest.mark.asyncio
async def test_orm_failure_is_wrapped_with_original_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_limiter._state import FixWindowUsage
    from nonebot_plugin_limiter.cooldown import _tz
    from nonebot_plugin_limiter.identity import EntityIdentity
    from nonebot_plugin_limiter.storage import LimiterStorageError, OrmLimiterStorage

    def fail_session():
        raise OSError("database path unavailable")

    monkeypatch.setattr("nonebot_plugin_limiter.storage.get_session", fail_session)

    with pytest.raises(LimiterStorageError) as error:
        await OrmLimiterStorage().mutate_fix_window(
            "failure",
            EntityIdentity("custom-v1", "user"),
            FixWindowUsage(datetime.now(tz=_tz), 1),
            lambda usage: usage,
        )

    assert isinstance(error.value.__cause__, OSError)
