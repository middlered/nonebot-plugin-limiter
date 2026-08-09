import json

from nonebot_plugin_uninfo import SceneRef, UserRef

ENTITY_KEY_SCHEMA = "uninfo-reference-v1"


def encode_user_ref(ref: UserRef) -> str:
    return _encode("user", ref.scope.value, ref.id)


def encode_scene_ref(ref: SceneRef) -> str:
    return _encode("scene", ref.scope.value, ref.id, ref.type.value, ref.parent_id)


def encode_user_scene_ref(user_ref: UserRef, scene_ref: SceneRef) -> str:
    """编码同一平台身份域内的用户与场景组合。

    Args:
        user_ref: 用户的轻量 reference。
        scene_ref: 场景的轻量 reference。

    Returns:
        带 schema 版本的稳定实体键。

    Raises:
        ValueError: 用户和场景不属于同一 Uninfo scope。
    """

    if user_ref.scope != scene_ref.scope:
        raise ValueError("user and scene must use the same scope")
    return _encode(
        "user-scene",
        user_ref.scope.value,
        user_ref.id,
        scene_ref.id,
        scene_ref.type.value,
        scene_ref.parent_id,
    )


def _encode(kind: str, *components: str | int | None) -> str:
    payload = json.dumps(components, ensure_ascii=False, separators=(",", ":"))
    return f"{ENTITY_KEY_SCHEMA}:{kind}:{payload}"
