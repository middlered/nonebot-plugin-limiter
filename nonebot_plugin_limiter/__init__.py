from nonebot import require
from nonebot.plugin import PluginMetadata, inherit_supported_adapters

require("nonebot_plugin_uninfo")
require("nonebot_plugin_alconna")
require("nonebot_plugin_orm")
from .config import Config

__plugin_meta__ = PluginMetadata(
    name="命令冷却",
    description="通用命令冷却限制器",
    usage="",
    type="library",
    homepage="https://github.com/MiddleRed/nonebot-plugin-limiter",
    config=Config,
    supported_adapters=inherit_supported_adapters("nonebot_plugin_alconna", "nonebot_plugin_uninfo"),
    extra={"author": "MiddleRed <middlered@outlook.com>"},
)

from .cooldown import Cooldown as Cooldown
from .cooldown import LeakyBucketCooldown as LeakyBucketCooldown
from .cooldown import SlidingWindowCooldown as SlidingWindowCooldown
from .cooldown import TokenBucketCooldown as TokenBucketCooldown
from .entity import BYPASS_ENTITY as BYPASS_ENTITY
from .entity import CooldownEntity as CooldownEntity
from .entity import GlobalScope as GlobalScope
from .entity import PrivateScope as PrivateScope
from .entity import PublicScope as PublicScope
from .entity import SceneScope as SceneScope
from .entity import UserSceneScope as UserSceneScope
from .entity import UserScope as UserScope
from .handle import FixedWindowHandle as FixedWindowHandle
from .handle import FixedWindowSnapshot as FixedWindowSnapshot
from .handle import LeakyBucketHandle as LeakyBucketHandle
from .handle import LeakyBucketSnapshot as LeakyBucketSnapshot
from .handle import LimiterHandle as LimiterHandle
from .handle import SlidingWindowHandle as SlidingWindowHandle
from .handle import SlidingWindowSnapshot as SlidingWindowSnapshot
from .handle import TokenBucketHandle as TokenBucketHandle
from .handle import TokenBucketSnapshot as TokenBucketSnapshot
from .handler import Increaser as Increaser
from .storage import LimiterStorageError as LimiterStorageError
