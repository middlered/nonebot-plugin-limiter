# nonebot-plugin-limiter
[![PyPI](https://img.shields.io/pypi/v/nonebot-plugin-limiter?logo=python&logoColor=edb641)](https://pypi.org/project/nonebot-plugin-limiter/) [![Python Version](https://img.shields.io/badge/python->=3.10-blue?logo=python&logoColor=edb641)]()

提供一个简单易用的冷却（Cooldown）和限流的依赖项用于命令消息速率限制，支持跨平台。

## 安装

<details open>
<summary>使用 nb-cli 安装</summary>
在 nonebot2 项目的根目录下打开命令行, 输入以下指令即可安装

    nb plugin install nonebot-plugin-limiter --upgrade
使用 **pypi** 源安装

    nb plugin install nonebot-plugin-limiter --upgrade -i "https://pypi.org/simple"
使用**清华源**安装

    nb plugin install nonebot-plugin-limiter --upgrade -i "https://pypi.tuna.tsinghua.edu.cn/simple"


</details>

<details>
<summary>使用包管理器安装</summary>
在 nonebot2 项目的插件目录下, 打开命令行, 根据你使用的包管理器, 输入相应的安装命令

<details open>
<summary>uv</summary>

    uv add nonebot-plugin-limiter
安装仓库 master 分支

    uv add git+https://github.com/MiddleRed/nonebot-plugin-limiter@master
</details>

<details>
<summary>pdm</summary>

    pdm add nonebot-plugin-limiter
安装仓库 master 分支

    pdm add git+https://github.com/MiddleRed/nonebot-plugin-limiter@master
</details>
<details>
<summary>poetry</summary>

    poetry add nonebot-plugin-limiter
安装仓库 master 分支

    poetry add git+https://github.com/MiddleRed/nonebot-plugin-limiter@master
</details>

打开 nonebot2 项目根目录下的 `pyproject.toml` 文件, 在 `[tool.nonebot]` 部分追加写入

    plugins = ["nonebot_plugin_limiter"]

</details>

## 使用
三种限流算法，自带六种限制对象，具体使用细节请阅读 docstring 。
```python
from nonebot_plugin_limiter import (
    Cooldown,
    SlidingWindowCooldown,
    TokenBucketCooldown,
    GlobalScope,
    UserScope,
    SceneScope,
    UserSceneScope,
    PrivateScope,
    PublicScope,
)
```
配置项
```bash
COOLDOWN_ENABLE_PERSISTENCE = false # 开启持久化
COOLDOWN_SAVE_INTERVAL = 60 # 开启持久化后保存时间，单位为秒
```
修改持久化本地存储目录请参考 localstore [插件配置方法](https://github.com/nonebot/plugin-localstore?tab=readme-ov-file#%E9%85%8D%E7%BD%AE%E9%A1%B9) 更改 `LOCALSTORE_PLUGIN_DATA_DIR`

## 快速上手

基本使用方式
```python
from nonebot import require
from nonebot.permission import SUPERUSER

from nonebot_plugin_uninfo import Uninfo

require("nonebot_plugin_limiter")
from nonebot_plugin_limiter import UserScope, Cooldown

matcher = on()


@matcher.handle(
    parameterless=[
        Cooldown(
            UserScope(  # entity, `UserScope` 统计范围为所有用户在任意场景的使用量
                whitelist=[114514, "justAuserId"], permission=SUPERUSER
            ),  # 两种白名单方式
            5,  # period, 冷却时长，单位为秒
            limit=2,  # 最大触发次数
            reject="操作过于频繁，请稍后再试。",  # 可选，超额使用时的提示词
            name="my_limiter",  # 可选，使用统计集合名称，填写名称将开启该集合的持久化
        )
    ]
)
async def handler(): ...
```

自定义限制对象、定制最大使用量。
```python
from datetime import timedelta # 支持传入 timedelta
from nonebot_plugin_limiter import BYPASS_ENTITY

# 同步样例。获取限制对象的唯一 ID
def get_entity_id(bot: Bot, event: Event): # 可依赖注入
    if <any_condition>:
        return BYPASS_ENTITY   # 返回 BYPASS_ENTITY 限制器将不会约束该对象的使用量
    return event.get_user_id()

# 异步样例。获取不同用户的最大使用量
async def get_user_max_usage(info: Uninfo): # 推荐使用 UniMessage 和 Uninfo  
    user: User = await any_orm(...)
    return user.max_usage

@test.handle(parameterless = [ # entity 和 limit 可传入自定义函数
    Cooldown(get_entity_id, timedelta(seconds = 10), limit = get_user_max_usage)
])
async def _():
    await test.finish("pass")
```

串联多个限制器。
```python
cmd = on_startswith("cmd")


@cmd.handle(
    parameterless=[
        Cooldown(UserScope(), 2, limit=2, reject="UserScope reject"),
        Cooldown(GlobalScope(), 3600, limit=4, reject="GlobalScope reject"),
    ]
)
async def _(): ...
```

多命令共享使用统计集合。  
> [!IMPORTANT]
> 请确保使用相同使用统计集合的限制器限流参数一致（限制对象，限制时间，最大使用量），否则可能会有预期之外的行为。
```python
# 注意，不同限流算法下的使用统计集合无法共享
cmd1 = on_startswith("cmd1")


@cmd1.handle(parameterless=[Cooldown(UserScope(), 100, limit=2, reject="reject1", name="shared_set")])
async def _(): ...


cmd2 = on_startswith("cmd2")


@cmd2.handle(parameterless=[Cooldown(UserScope(), 100, limit=2, reject="reject2", name="shared_set")])
async def _(): ...
```

手动管理限制器使用记录的增加操作，适用于没有成功完成事件处理时避免给用户添加限制。
```python
from nonebot_plugin_limiter import Increaser

cmd = on_startswith("cmd")
@cmd.handle(parameterless=[
    Cooldown(UserScope(), 100, limit = 2, set_increaser = True)
])
async def _(increaser: Increaser):
    if <not_meet_condition>:
        await cmd.finish("Run failed")
    else:
        increaser.execute() # 这会给该实体添加一条使用记录
        await cmd.finish("Run successed")
```

丰富的 `reject` 选择
```python
from nonebot.matcher import Matcher
from nonebot_plugin_alconna import UniMessage


async def reject_callback(bot: Bot, matcher: Matcher):
    await matcher.finish("Quota exceeded.")


cmd = on_startswith("cmd")


@cmd.handle(
    parameterless=[
        Cooldown(UserScope(), 10, reject="Quota exceeded"),
        Cooldown(UserScope(), 20, reject=MessageSegment.text("Quota exceeded.")),
        Cooldown(UserScope(), 30, reject=UniMessage.text("Quota exceeded.")),
        Cooldown(UserScope(), 40, reject=reject_callback),  # 试验性支持
    ]
)
async def _(): ...
```

使用固定窗口策略实现每日签到。
```python
dailysign = on_startswith("签到")


@dailysign.handle(
    parameterless=[
        Cooldown(
            UserScope(),
            "0 0 * * *",  # Cooldown 支持传入 cron 格式定时任务
            limit=1,
            reject="你今天已经签到过了！",
            name="dailysign",  # 为使用统计集合命名，实现持久化
        ),
    ]
)
async def _():
    await dailysign.finish("签到成功！")
```

### Feature
- [x] 固定窗口
- [x] 滑动窗口
- ~~[] 漏桶~~（已废弃，请使用令牌桶）
- [x] 令牌桶
- [x] reject 依赖注入回调函数（试验性支持）
- [ ] 重置用量
- [x] 本地持久化状态

## 鸣谢
本插件部分代码参考了 [nonebot/adapter-onebot](https://github.com/nonebot/adapter-onebot) 的 `Cooldown` [实现](https://github.com/nonebot/adapter-onebot/blob/51294404cc8bf0b3d03008e09f34d3dd1a6acfd8/nonebot/adapters/onebot/v11/helpers.py#L224) ，在此表示感谢
