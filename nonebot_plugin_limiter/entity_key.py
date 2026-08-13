from dataclasses import dataclass
import json
from typing import TypeAlias

from nonebot_plugin_uninfo import SceneType, SupportScope

ENTITY_KEY_PREFIX = "limiter-entity:"


@dataclass(frozen=True)
class UserKey:
    scope: str
    user_id: str


@dataclass(frozen=True)
class SceneKey:
    scope: str
    scene_id: str
    scene_type: int
    parent_id: str | None = None


@dataclass(frozen=True)
class SceneUserKey:
    scene: SceneKey
    user_id: str


EntityKey: TypeAlias = str | UserKey | SceneKey | SceneUserKey


def make_user_key(scope: SupportScope, user_id: str) -> UserKey:
    return UserKey(scope.value, user_id)


def make_scene_key(
    scope: SupportScope,
    scene_id: str,
    scene_type: SceneType,
    parent_id: str | None,
) -> SceneKey:
    return SceneKey(scope.value, scene_id, scene_type.value, parent_id)


def serialize_entity_key(key: EntityKey) -> str:
    match key:
        case str():
            return key
        case UserKey(scope, user_id):
            value = ["user", scope, user_id]
        case SceneKey(scope, scene_id, scene_type, parent_id):
            value = ["scene", scope, scene_id, scene_type, parent_id]
        case SceneUserKey(SceneKey(scope, scene_id, scene_type, parent_id), user_id):
            value = [
                "scene_user",
                scope,
                scene_id,
                scene_type,
                parent_id,
                user_id,
            ]
        case _:
            raise TypeError(f"unsupported entity key type: {type(key)!r}")
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return f"{ENTITY_KEY_PREFIX}{payload}"


def deserialize_entity_key(value: str) -> EntityKey:
    if not value.startswith(ENTITY_KEY_PREFIX):
        return value

    try:
        data = json.loads(value[len(ENTITY_KEY_PREFIX) :])
        if not isinstance(data, list) or not data or not isinstance(data[0], str):
            return value
        kind = data[0]
        if kind == "user" and len(data) == 3 and all(isinstance(item, str) for item in data[1:]):
            return UserKey(data[1], data[2])
        if kind == "scene" and _is_scene_payload(data):
            return SceneKey(data[1], data[2], data[3], data[4])
        if kind == "scene_user" and len(data) == 6 and _is_scene_payload(data[:5]) and isinstance(data[5], str):
            scene = SceneKey(data[1], data[2], data[3], data[4])
            return SceneUserKey(scene, data[5])
    except json.JSONDecodeError:
        pass
    return value


def _is_scene_payload(value: list[object]) -> bool:
    return (
        len(value) == 5
        and isinstance(value[1], str)
        and isinstance(value[2], str)
        and isinstance(value[3], int)
        and not isinstance(value[3], bool)
        and (value[4] is None or isinstance(value[4], str))
    )
