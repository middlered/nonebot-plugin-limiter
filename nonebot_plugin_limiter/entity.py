from abc import abstractmethod
from typing import Literal

from nonebot.adapters import Bot, Event
from nonebot.permission import Permission
from nonebot_plugin_uninfo import MemberRef, SceneRef, UserRef, get_session

from .entity_key import encode_scene_ref, encode_user_ref, encode_user_scene_ref

_IdType = str | int
_UserWhitelistValue = _IdType | UserRef
_SceneWhitelistValue = _IdType | SceneRef
_UserSceneWhitelistValue = tuple[_IdType | Literal["*"] | UserRef, _IdType | Literal["*"] | SceneRef] | MemberRef
BYPASS_ENTITY = "__bypass"


def _normalize_user_whitelist(
    whitelist: tuple[_UserWhitelistValue, ...] | None,
) -> tuple[str | UserRef, ...] | None:
    if whitelist is None:
        return None
    return tuple(value if isinstance(value, UserRef) else str(value) for value in whitelist)


def _normalize_scene_whitelist(
    whitelist: tuple[_SceneWhitelistValue, ...] | None,
) -> tuple[str | SceneRef, ...] | None:
    if whitelist is None:
        return None
    return tuple(value if isinstance(value, SceneRef) else str(value) for value in whitelist)


def _matches_user(value: str | UserRef, ref: UserRef) -> bool:
    return value == ref if isinstance(value, UserRef) else value == ref.id


def _matches_scene(value: str | SceneRef, ref: SceneRef) -> bool:
    return value == ref if isinstance(value, SceneRef) else value == ref.id


def _normalize_user_scene_whitelist(
    whitelist: tuple[_UserSceneWhitelistValue, ...] | None,
) -> tuple[tuple[str | UserRef, str | SceneRef] | MemberRef, ...] | None:
    """规范化用户场景白名单，同时保留 reference 的身份域。

    Raises:
        ValueError: 同一白名单项中的精确用户与场景 reference 不属于同一 scope。
    """

    if whitelist is None:
        return None

    normalized: list[tuple[str | UserRef, str | SceneRef] | MemberRef] = []
    for value in whitelist:
        if isinstance(value, MemberRef):
            normalized.append(value)
            continue

        user, scene = value
        if isinstance(user, UserRef) and isinstance(scene, SceneRef) and user.scope != scene.scope:
            raise ValueError("whitelist user and scene references must use the same scope")
        normalized.append(
            (
                user if isinstance(user, UserRef) else str(user),
                scene if isinstance(scene, SceneRef) else str(scene),
            )
        )
    return tuple(normalized)


class CooldownEntity:
    """
    **限制实体类**
    """

    __slots__ = ()

    @abstractmethod
    def __init__(self) -> None: ...

    @abstractmethod
    async def get_entity_id(self, bot: Bot, event: Event) -> str:
        """
        返回被限制实体的唯一标识符，统一为 str
        """
        ...


class GlobalScope(CooldownEntity):
    """
    **全局限制实体**

    限制所有用户在所有场景下的使用情况。
    """

    def __init__(self) -> None:
        pass

    async def get_entity_id(self, bot: Bot, event: Event) -> str:
        return "__global"


class UserScope(CooldownEntity):
    """
    **用户限制实体**

    限制单个用户在所有场景下的使用情况。

    使用 Uninfo scope 与用户 ID 共同标识用户；同一 scope 下的多个 Bot 账号共享额度。
    """

    def __init__(
        self,
        *,
        whitelist: tuple[_UserWhitelistValue, ...] | None = None,
        permission: Permission | None = None,
    ) -> None:
        """
        可选参数:
            whitelist (tuple[str | int | UserRef]):
                裸用户 ID 匹配任意 scope；UserRef 只匹配指定平台身份域。
            permission: (Permission):
                NoneBot 权限，通过该权限检查的将不受限制。

        注：whitelist 与 permission 不互斥，通过任意一个条件即不受限制
        """

        self.whitelist = _normalize_user_whitelist(whitelist)
        self.permission = permission

    async def get_entity_id(self, bot: Bot, event: Event) -> str:
        sess = await get_session(bot, event)
        if sess is None:
            return BYPASS_ENTITY

        user_ref = sess.user_ref
        if self.whitelist is not None and any(_matches_user(value, user_ref) for value in self.whitelist):
            return BYPASS_ENTITY
        if self.permission is not None and (await self.permission(bot, event)):
            return BYPASS_ENTITY
        return encode_user_ref(user_ref)


class SceneScope(CooldownEntity):
    """
    **场景限制实体**

    限制每个场景下在该场景内的所有用户使用情况。
    """

    def __init__(
        self,
        *,
        whitelist: tuple[_SceneWhitelistValue, ...] | None = None,
        permission: Permission | None = None,
    ) -> None:
        """
        可选参数:
            whitelist (tuple[str | int | SceneRef]):
                裸场景 ID 匹配任意 scope；SceneRef 只匹配指定平台身份域和场景结构。
            permission: (Permission):
                NoneBot 权限，通过该权限检查的将不受限制。

        注：whitelist 与 permission 不互斥，通过任意一个条件即不受限制
        """

        self.whitelist = _normalize_scene_whitelist(whitelist)
        self.permission = permission

    async def get_entity_id(self, bot: Bot, event: Event) -> str:
        sess = await get_session(bot, event)
        if sess is None:
            return BYPASS_ENTITY

        scene_ref = sess.scene_ref
        if self.whitelist is not None and any(_matches_scene(value, scene_ref) for value in self.whitelist):
            return BYPASS_ENTITY
        if self.permission is not None and (await self.permission(bot, event)):
            return BYPASS_ENTITY
        return encode_scene_ref(scene_ref)


class UserSceneScope(CooldownEntity):
    """
    **用户场景限制实体**

    限制单个用户在不同场景下的使用情况，各个场景使用情况相互独立。
    """

    def __init__(
        self,
        *,
        whitelist: tuple[_UserSceneWhitelistValue, ...] | None = None,
        permission: Permission | None = None,
    ) -> None:
        """
        可选参数:
            whitelist:
                用户与场景二元组或 MemberRef。裸 ID 匹配任意 scope，UserRef/SceneRef/MemberRef
                只匹配指定平台身份域。
            permission: (Permission):
                NoneBot 权限，通过该权限检查的将不受限制。

        注：
            - 白名单中用户 ID 与场景 ID 组合为二元组，用户 ID 在前场景 ID 在后。
            - 用户 ID 和场景 ID 均可为 `*`，表示任意用户或任意场景。
            - whitelist 与 permission 不互斥，通过任意一个条件即不受限制。
        """

        self.whitelist = _normalize_user_scene_whitelist(whitelist)
        self.permission = permission

    async def get_entity_id(self, bot: Bot, event: Event) -> str:
        sess = await get_session(bot, event)
        if sess is None:
            return BYPASS_ENTITY

        user_ref = sess.user_ref
        scene_ref = sess.scene_ref
        if self.whitelist is not None:
            for value in self.whitelist:
                if isinstance(value, MemberRef):
                    if value == sess.member_ref:
                        return BYPASS_ENTITY
                    continue
                user, scene = value
                if (user == "*" or _matches_user(user, user_ref)) and (
                    scene == "*" or _matches_scene(scene, scene_ref)
                ):
                    return BYPASS_ENTITY
        if self.permission is not None and (await self.permission(bot, event)):
            return BYPASS_ENTITY
        return encode_user_scene_ref(user_ref, scene_ref)


class PrivateScope(CooldownEntity):
    """
    **用户私聊限制实体**

    限制单个用户在私聊下的使用情况。

    使用 Uninfo scope 与用户 ID 共同标识用户；同一 scope 下的多个 Bot 账号共享额度。
    """

    def __init__(
        self,
        *,
        whitelist: tuple[_UserWhitelistValue, ...] | None = None,
        permission: Permission | None = None,
    ) -> None:
        """
        可选参数:
            whitelist (tuple[str | int | UserRef]):
                裸用户 ID 匹配任意 scope；UserRef 只匹配指定平台身份域。
            permission: (Permission):
                NoneBot 权限，通过该权限检查的将不受限制。

        注：whitelist 与 permission 不互斥，通过任意一个条件即不受限制
        """

        self.whitelist = _normalize_user_whitelist(whitelist)
        self.permission = permission

    async def get_entity_id(self, bot: Bot, event: Event) -> str:
        sess = await get_session(bot, event)
        if sess is None or not sess.scene.is_private:
            return BYPASS_ENTITY

        user_ref = sess.user_ref
        if self.whitelist is not None and any(_matches_user(value, user_ref) for value in self.whitelist):
            return BYPASS_ENTITY
        if self.permission is not None and (await self.permission(bot, event)):
            return BYPASS_ENTITY
        return encode_user_ref(user_ref)


class PublicScope(CooldownEntity):
    """
    **用户非私聊限制实体**

    限制单个用户在非私聊下的使用情况。

    使用 Uninfo scope 与用户 ID 共同标识用户；同一 scope 下的多个 Bot 账号共享额度。
    """

    def __init__(
        self,
        *,
        whitelist: tuple[_UserWhitelistValue, ...] | None = None,
        permission: Permission | None = None,
    ) -> None:
        """
        可选参数:
            whitelist (tuple[str | int | UserRef]):
                裸用户 ID 匹配任意 scope；UserRef 只匹配指定平台身份域。
            permission: (Permission):
                NoneBot 权限，通过该权限检查的将不受限制。

        注：whitelist 与 permission 不互斥，通过任意一个条件即不受限制
        """

        self.whitelist = _normalize_user_whitelist(whitelist)
        self.permission = permission

    async def get_entity_id(self, bot: Bot, event: Event) -> str:
        sess = await get_session(bot, event)
        if sess is None or sess.scene.is_private:
            return BYPASS_ENTITY

        user_ref = sess.user_ref
        if self.whitelist is not None and any(_matches_user(value, user_ref) for value in self.whitelist):
            return BYPASS_ENTITY
        if self.permission is not None and (await self.permission(bot, event)):
            return BYPASS_ENTITY
        return encode_user_ref(user_ref)
