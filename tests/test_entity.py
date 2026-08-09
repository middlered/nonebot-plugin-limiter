from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from nonebot_plugin_uninfo import SceneType, Session, SupportScope

    from nonebot_plugin_limiter.entity import CooldownEntity


def make_session(
    *,
    scope: SupportScope | None = None,
    self_id: str = "bot-1",
    user_id: str = "user-1",
    scene_id: str = "scene-1",
    scene_type: SceneType | None = None,
    parent_id: str | None = None,
) -> Session:
    from nonebot_plugin_uninfo import Scene, SceneType, Session, SupportScope, User

    if scope is None:
        scope = SupportScope.qq_client
    if scene_type is None:
        scene_type = SceneType.GROUP
    parent = Scene(id=parent_id, type=SceneType.GUILD) if parent_id is not None else None
    return Session(
        self_id=self_id,
        adapter="OneBot V11",
        scope=scope,
        scene=Scene(id=scene_id, type=scene_type, parent=parent),
        user=User(id=user_id),
    )


@asynccontextmanager
async def use_session(monkeypatch: pytest.MonkeyPatch, session: Session | None) -> AsyncIterator[None]:
    async def _get_session(*_args: object) -> Session | None:
        return session

    monkeypatch.setattr("nonebot_plugin_limiter.entity.get_session", _get_session)
    yield


async def get_id(scope: CooldownEntity) -> str:
    return await scope.get_entity_id(object(), object())  # type: ignore[arg-type]


def test_reference_key_fixed_vectors() -> None:
    from nonebot_plugin_uninfo import SceneRef, SceneType, SupportScope, UserRef

    from nonebot_plugin_limiter.entity_key import encode_scene_ref, encode_user_ref, encode_user_scene_ref

    user_ref = UserRef(SupportScope.qq_client, "u:1")
    scene_ref = SceneRef(SupportScope.qq_client, "s:1", SceneType.CHANNEL_TEXT, "guild:1")

    assert encode_user_ref(user_ref) == 'uninfo-reference-v1:user:["QQClient","u:1"]'
    assert encode_scene_ref(scene_ref) == 'uninfo-reference-v1:scene:["QQClient","s:1",3,"guild:1"]'
    assert encode_user_scene_ref(user_ref, scene_ref) == (
        'uninfo-reference-v1:user-scene:["QQClient","u:1","s:1",3,"guild:1"]'
    )


def test_entity_identity_preserves_schema_boundary() -> None:
    from nonebot_plugin_limiter.entity_key import encode_user_ref
    from nonebot_plugin_limiter.identity import identify_entity

    legacy_key = "u`user-1`"
    new_key = encode_user_ref(make_session().user_ref)

    assert identify_entity(legacy_key).schema == "custom-v1"
    assert identify_entity(legacy_key).key == legacy_key
    assert identify_entity(new_key).schema == "uninfo-reference-v1"
    assert identify_entity(new_key).key == new_key


@pytest.mark.parametrize("scope_name", ["UserScope", "SceneScope", "UserSceneScope", "PublicScope"])
async def test_same_raw_ids_in_different_scopes_have_different_keys(
    monkeypatch: pytest.MonkeyPatch,
    scope_name: str,
) -> None:
    from nonebot_plugin_uninfo import SupportScope

    import nonebot_plugin_limiter

    scope_factory: type[Any] = getattr(nonebot_plugin_limiter, scope_name)
    scope = scope_factory()
    async with use_session(monkeypatch, make_session(scope=SupportScope.qq_client)):
        qq_key = await get_id(scope)
    async with use_session(monkeypatch, make_session(scope=SupportScope.discord)):
        discord_key = await get_id(scope)

    assert qq_key != discord_key


@pytest.mark.parametrize("scope_name", ["UserScope", "SceneScope", "UserSceneScope", "PublicScope"])
async def test_same_scope_shares_keys_across_bots(
    monkeypatch: pytest.MonkeyPatch,
    scope_name: str,
) -> None:
    import nonebot_plugin_limiter

    scope_factory: type[Any] = getattr(nonebot_plugin_limiter, scope_name)
    scope = scope_factory()
    async with use_session(monkeypatch, make_session(self_id="bot-1")):
        first_key = await get_id(scope)
    async with use_session(monkeypatch, make_session(self_id="bot-2")):
        second_key = await get_id(scope)

    assert first_key == second_key


async def test_scene_key_includes_type_and_parent(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_uninfo import SceneType

    from nonebot_plugin_limiter import SceneScope

    scope = SceneScope()
    async with use_session(monkeypatch, make_session(scene_type=SceneType.GROUP)):
        group_key = await get_id(scope)
    async with use_session(
        monkeypatch,
        make_session(scene_type=SceneType.CHANNEL_TEXT, parent_id="guild-1"),
    ):
        channel_key = await get_id(scope)
    async with use_session(
        monkeypatch,
        make_session(scene_type=SceneType.CHANNEL_TEXT, parent_id="guild-2"),
    ):
        other_parent_key = await get_id(scope)

    assert len({group_key, channel_key, other_parent_key}) == 3


async def test_user_scene_scope_supports_private_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_uninfo import SceneType

    from nonebot_plugin_limiter import UserSceneScope

    session = make_session(scene_type=SceneType.PRIVATE)
    assert session.member_ref is None

    async with use_session(monkeypatch, session):
        key = await get_id(UserSceneScope())

    assert key.startswith("uninfo-reference-v1:user-scene:")


async def test_raw_user_whitelist_remains_cross_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_uninfo import SupportScope

    from nonebot_plugin_limiter import BYPASS_ENTITY, UserScope

    scope = UserScope(whitelist=("user-1",))

    async with use_session(monkeypatch, make_session(scope=SupportScope.qq_client)):
        assert await get_id(scope) == BYPASS_ENTITY
    async with use_session(monkeypatch, make_session(scope=SupportScope.discord)):
        assert await get_id(scope) == BYPASS_ENTITY


async def test_reference_user_whitelist_is_scope_specific(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_uninfo import SupportScope, UserRef

    from nonebot_plugin_limiter import BYPASS_ENTITY, UserScope

    scope = UserScope(whitelist=(UserRef(SupportScope.qq_client, "user-1"),))

    async with use_session(monkeypatch, make_session(scope=SupportScope.qq_client)):
        assert await get_id(scope) == BYPASS_ENTITY
    async with use_session(monkeypatch, make_session(scope=SupportScope.discord)):
        assert await get_id(scope) != BYPASS_ENTITY


async def test_reference_scene_whitelist_is_scope_specific(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_uninfo import SceneRef, SceneType, SupportScope

    from nonebot_plugin_limiter import BYPASS_ENTITY, SceneScope

    scope = SceneScope(whitelist=(SceneRef(SupportScope.qq_client, "scene-1", SceneType.GROUP),))

    async with use_session(monkeypatch, make_session(scope=SupportScope.qq_client)):
        assert await get_id(scope) == BYPASS_ENTITY
    async with use_session(monkeypatch, make_session(scope=SupportScope.discord)):
        assert await get_id(scope) != BYPASS_ENTITY


async def test_user_scene_whitelist_accepts_reference_pair_and_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nonebot_plugin_limiter import BYPASS_ENTITY, UserSceneScope

    session = make_session()
    reference_pair = (session.user_ref, session.scene_ref)
    assert session.member_ref is not None

    async with use_session(monkeypatch, session):
        assert await get_id(UserSceneScope(whitelist=(reference_pair,))) == BYPASS_ENTITY
        assert await get_id(UserSceneScope(whitelist=(session.member_ref,))) == BYPASS_ENTITY


async def test_user_scene_raw_wildcard_remains_cross_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nonebot_plugin_uninfo import SupportScope

    from nonebot_plugin_limiter import BYPASS_ENTITY, UserSceneScope

    scope = UserSceneScope(whitelist=(("user-1", "*"),))

    async with use_session(monkeypatch, make_session(scope=SupportScope.qq_client)):
        assert await get_id(scope) == BYPASS_ENTITY
    async with use_session(monkeypatch, make_session(scope=SupportScope.discord)):
        assert await get_id(scope) == BYPASS_ENTITY


def test_user_scene_whitelist_rejects_mixed_reference_scopes() -> None:
    from nonebot_plugin_uninfo import SceneRef, SceneType, SupportScope, UserRef

    from nonebot_plugin_limiter import UserSceneScope

    with pytest.raises(ValueError, match="same scope"):
        UserSceneScope(
            whitelist=(
                (
                    UserRef(SupportScope.qq_client, "user-1"),
                    SceneRef(SupportScope.discord, "scene-1", SceneType.GROUP),
                ),
            )
        )


async def test_private_and_public_scope_applicability(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_uninfo import SceneType

    from nonebot_plugin_limiter import BYPASS_ENTITY, PrivateScope, PublicScope

    async with use_session(monkeypatch, make_session(scene_type=SceneType.PRIVATE)):
        assert await get_id(PrivateScope()) != BYPASS_ENTITY
        assert await get_id(PublicScope()) == BYPASS_ENTITY

    async with use_session(monkeypatch, make_session(scene_type=SceneType.GROUP)):
        assert await get_id(PrivateScope()) == BYPASS_ENTITY
        assert await get_id(PublicScope()) != BYPASS_ENTITY


async def test_private_scope_key_is_namespaced_and_bot_agnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nonebot_plugin_uninfo import SceneType, SupportScope

    from nonebot_plugin_limiter import PrivateScope

    scope = PrivateScope()
    async with use_session(
        monkeypatch,
        make_session(
            scope=SupportScope.qq_client,
            self_id="bot-1",
            scene_type=SceneType.PRIVATE,
        ),
    ):
        first_key = await get_id(scope)
    async with use_session(
        monkeypatch,
        make_session(
            scope=SupportScope.qq_client,
            self_id="bot-2",
            scene_type=SceneType.PRIVATE,
        ),
    ):
        second_key = await get_id(scope)
    async with use_session(
        monkeypatch,
        make_session(
            scope=SupportScope.discord,
            scene_type=SceneType.PRIVATE,
        ),
    ):
        other_scope_key = await get_id(scope)

    assert first_key == second_key
    assert first_key != other_scope_key


async def test_missing_session_bypasses(monkeypatch: pytest.MonkeyPatch) -> None:
    from nonebot_plugin_limiter import BYPASS_ENTITY, UserScope

    async with use_session(monkeypatch, None):
        assert await get_id(UserScope()) == BYPASS_ENTITY
