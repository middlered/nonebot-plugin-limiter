from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime, timedelta
from functools import partial
import inspect
from typing import Any, cast

from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from nonebot.adapters import Bot, Event, Message, MessageSegment, MessageTemplate
from nonebot.exception import FinishedException
from nonebot.matcher import Matcher
from nonebot.params import Depends
from nonebot.rule import Rule as Rule
from nonebot.typing import T_State, _DependentCallable
from nonebot.utils import is_coroutine_callable, run_sync
from nonebot_plugin_alconna import UniMessage
from tzlocal import get_localzone

from ._state import FixWindowUsage, LeakyBucketUsage, SlidingWindowUsage, TokenBucketUsage
from .coordinator import NamedBucketKey, named_bucket_coordinator
from .entity import BYPASS_ENTITY, CooldownEntity
from .handle import (
    FixedWindowHandle,
    FixedWindowSnapshot,
    LeakyBucketHandle,
    LeakyBucketSnapshot,
    SlidingWindowHandle,
    SlidingWindowSnapshot,
    TokenBucketHandle,
    TokenBucketSnapshot,
)
from .handler import _ensure_increaser, _reserve_increaser
from .identity import identify_entity
from .storage import get_limiter_storage

_tz = get_localzone()
SupportMsgType = str | Message | MessageSegment | MessageTemplate | UniMessage


def _calculate_continuous_recovery(
    last_update_time: datetime,
    now: datetime,
    speed: int,
    recoverable_capacity: int,
) -> tuple[int, datetime]:
    """计算连续桶本次可兑现的恢复量和新的时间基准。

    Args:
        last_update_time: 上次恢复后尚未兑现的时间起点。
        now: 本次检查时间。
        speed: 每秒恢复的整数单位数。
        recoverable_capacity: 当前距离空桶或满桶边界还可恢复的单位数。

    Returns:
        本次恢复的整数单位数与新的时间基准。达到饱和边界时丢弃更早的时间债务；
        尚不足一个恢复单位时保留原时间基准。
    """

    if speed <= 0 or recoverable_capacity <= 0:
        return 0, now if recoverable_capacity <= 0 else last_update_time

    elapsed_seconds = max((now - last_update_time).total_seconds(), 0)
    recovered = min(int(elapsed_seconds * speed), recoverable_capacity)

    if recovered == recoverable_capacity:
        return recovered, now
    if recovered == 0:
        return 0, last_update_time
    return recovered, last_update_time + timedelta(seconds=recovered / speed)


def _validate_positive(name: str, value: float) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


def _validate_non_negative(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _entity_id_dep_wrapper(entity: CooldownEntity | _DependentCallable[str]) -> _DependentCallable[str]:
    if isinstance(entity, CooldownEntity):
        entity_id_dep = entity.get_entity_id
    else:
        entity_id_dep = entity
    return entity_id_dep


def _named_bucket_key(algorithm: str, bucket_name: str, entity_id: str) -> NamedBucketKey:
    return NamedBucketKey(algorithm, bucket_name, identify_entity(entity_id))


def _fixed_window_trigger(period: int | timedelta | str) -> BaseTrigger:
    if isinstance(period, str):
        return cast(BaseTrigger, CronTrigger.from_crontab(period))

    interval_length = period.total_seconds() if isinstance(period, timedelta) else period
    _validate_positive("period", interval_length)
    return cast(BaseTrigger, IntervalTrigger(seconds=interval_length))


def _advance_fixed_window(
    usage: FixWindowUsage,
    current_time: datetime,
    trigger: BaseTrigger,
    limit: int,
) -> None:
    reset_time = trigger.get_next_fire_time(usage.start_time, current_time)
    assert reset_time is not None, "reset_time should not be None"
    if current_time >= reset_time:
        usage.start_time = current_time
        usage.available = limit


def _fixed_window_snapshot(usage: FixWindowUsage, pending: int) -> FixedWindowSnapshot:
    return FixedWindowSnapshot(max(usage.available - pending, 0), pending)


def _drop_expired_sliding_window(
    usage: SlidingWindowUsage,
    current_time: datetime,
    window_length: float,
) -> None:
    while usage.timestamps and (current_time - usage.timestamps[0]).total_seconds() >= window_length:
        usage.timestamps.popleft()


def _sliding_window_snapshot(
    usage: SlidingWindowUsage,
    limit: int,
    pending: int,
) -> SlidingWindowSnapshot:
    return SlidingWindowSnapshot(max(limit - len(usage.timestamps) - pending, 0), pending)


def _leak_bucket(usage: LeakyBucketUsage, current_time: datetime, leak_speed: int) -> None:
    leaked_size, usage.last_update_time = _calculate_continuous_recovery(
        usage.last_update_time,
        current_time,
        leak_speed,
        usage.used,
    )
    usage.used = max(usage.used - leaked_size, 0)


def _leaky_bucket_snapshot(usage: LeakyBucketUsage, pending: int) -> LeakyBucketSnapshot:
    return LeakyBucketSnapshot(
        used=usage.used,
        available_capacity=max(usage.capacity - usage.used - pending, 0),
        pending=pending,
    )


def _refill_token_bucket(usage: TokenBucketUsage, current_time: datetime, add_speed: int) -> None:
    resume_size, usage.last_update_time = _calculate_continuous_recovery(
        usage.last_update_time,
        current_time,
        add_speed,
        usage.capacity - usage.available,
    )
    usage.available = min(usage.available + resume_size, usage.capacity)


def _token_bucket_snapshot(usage: TokenBucketUsage, pending: int) -> TokenBucketSnapshot:
    return TokenBucketSnapshot(max(usage.available - pending, 0), pending)


def _limit_dep_wrapper(limit: int | _DependentCallable[int]) -> _DependentCallable[int]:
    if isinstance(limit, int):
        limit_dep = lambda: limit  # noqa: E731
    else:
        limit_dep = limit
    return limit_dep


def _reject_dep_wrapper(reject: None | SupportMsgType | _DependentCallable[Any]) -> _DependentCallable[Any]:
    if isinstance(reject, SupportMsgType):

        async def _send_msg(bot: Bot, matcher: Matcher, event: Event):
            if isinstance(reject, UniMessage):
                await reject.finish(event, bot)
            else:
                await matcher.finish(reject)

        reject_func = _send_msg
    elif reject is not None:  # callable
        if not is_coroutine_callable(reject):
            reject = run_sync(reject)
        reject_func = reject
    else:

        async def _null():
            return None

        reject_func = _null

    async def _inject_wrapper(*args, **kwargs):
        sig = inspect.signature(reject_func)
        bound = sig.bind(*args, **kwargs)
        return partial(reject_func, **bound.arguments)

    setattr(_inject_wrapper, "__signature__", inspect.signature(reject_func))
    # _inject_wrapper.__signature__ = inspect.signature(reject_func)
    return _inject_wrapper


# region: FixWindow
def Cooldown(
    entity: CooldownEntity | _DependentCallable[str],
    period: int | timedelta | str,
    *,
    limit: int | _DependentCallable[int] = 5,
    reject: None | SupportMsgType | _DependentCallable[Any] = None,
    set_increaser: bool = False,
    name: None | str = None,
):
    """
    **固定窗口速率限制**

    用于限制指定对象在固定时间周期内的消息触发次数。

    参数:
        entity (CooldownEntity | _DependentCallable[str]):
            设置需要进行速率限制的对象。
            - 可传入 `CooldownEntity` 对象，如 `UserScope`, `SceneScope` 等。
            - 可传入返回值为 `str` 的函数，自定义限制对象的**唯一 ID**，支持依赖注入。

        period (int | datetime.timedelta | str):
            设置速率限制的重置时间。
            - 若为 `int` 或 `datetime.timedelta`，表示周期开始后经过指定时间后重置限制，且必须大于零。
            - 若为 `str`，应为合法的 cron 表达式，表示按计划任务方式重置限制。

        limit (int | _DependentCallable[int]):
            可选，设置在每个周期内允许的最大触发次数。默认为 5。
            - 可传入返回值为 `int` 的函数，自定义最大触发次数，支持依赖注入。
            - 静态值和依赖注入返回值都不能为负数。

        reject (None | SupportMsgType | _DependentCallable):
            可选，当超出限制时的响应行为。默认为 `None`。
            - 若为 `str` 或消息对象，将作为限制使用时的提示消息发送给用户。
            - 若为依赖注入函数，将会在拒绝时进行调用。

        set_increaser (bool):
            可选，是否获取限制器的增加器。默认为 False。
            - 启用后会先预占额度；业务成功时可执行当前 Handle 或批量 Increaser 确认计数。
            - 未确认的预占会在 Matcher 结束时自动释放。

        name (None | str):
            可选，设置当前限制器的使用统计集合。默认为 `None` ，即私有集合。
            - 当传入 `str` ，将创建或加入一个同名公共集合，可用于与其他命令的限制器共享使用统计。

    Returns:
        可作为 parameterless 依赖使用的 NoneBot `Depends`；声明为具名 Handler 参数时注入
        `FixedWindowHandle`，用于只读状态或逐项确认预占。

    Raises:
        ValueError: 当非 cron 周期不大于零，或静态、动态 `limit` 为负数时抛出。

    示例:
    ```python
    from nonebot.permission import SUPERUSER
    from nonebot_plugin_limiter.entity import UserScope

    # 在 5 秒内最多触发 2 次
    @matcher.handle(parameterless=[
        Cooldown(
            UserScope(permission=SUPERUSER),
            5,
            limit = 2,
            reject="操作过于频繁，请稍后再试。"
        )
    ])
    async def handler(...): ...
    ```
    """

    if isinstance(limit, int):
        _validate_non_negative("limit", limit)

    trigger = _fixed_window_trigger(period)

    bucket: dict[str, FixWindowUsage] = {}

    async def _limiter_dependency(
        state: T_State,
        entity_id: str = Depends(_entity_id_dep_wrapper(entity)),
        limit: int = Depends(_limit_dep_wrapper(limit)),
        reject_cb: Callable[..., Awaitable[Any]] = Depends(_reject_dep_wrapper(reject)),
    ) -> AsyncGenerator[FixedWindowHandle, None]:
        if set_increaser:
            _ensure_increaser(state)
        if entity_id == BYPASS_ENTITY:
            snapshot = FixedWindowSnapshot(None)

            async def _read_bypass() -> FixedWindowSnapshot:
                return snapshot

            yield FixedWindowHandle(_read_bypass, committed=snapshot)
            return
        _validate_non_negative("limit", limit)

        now = datetime.now(tz=_tz)

        if name is not None:
            key = _named_bucket_key("fixed", name, entity_id)
            lock = named_bucket_coordinator.lock(key)
            initial = FixWindowUsage(now, limit)
            reservation = None

            async def _read_named() -> FixedWindowSnapshot:
                read_time = datetime.now(tz=_tz)
                async with lock:
                    read_usage = await get_limiter_storage().read_fix_window(name, key.identity)
                    if read_usage is None:
                        read_usage = FixWindowUsage(read_time, limit)
                    _advance_fixed_window(read_usage, read_time, trigger, limit)
                    return _fixed_window_snapshot(read_usage, named_bucket_coordinator.pending(key))

            async with lock:
                pending = named_bucket_coordinator.pending(key)

                def _admit(usage: FixWindowUsage) -> tuple[bool, FixedWindowSnapshot]:
                    _advance_fixed_window(usage, now, trigger, limit)
                    if usage.available - pending <= 0:
                        return False, _fixed_window_snapshot(usage, pending)
                    if not set_increaser:
                        usage.available -= 1
                    return True, _fixed_window_snapshot(usage, pending)

                allowed, committed_snapshot = await get_limiter_storage().mutate_fix_window(
                    name, key.identity, initial, _admit
                )
                if allowed and set_increaser:
                    named_bucket_coordinator.reserve(key, 1)

                    async def _commit_action() -> FixedWindowSnapshot:
                        async with lock:
                            commit_time = datetime.now(tz=_tz)
                            pending_after = named_bucket_coordinator.pending(key) - 1

                            def _commit(usage: FixWindowUsage) -> FixedWindowSnapshot:
                                _advance_fixed_window(usage, commit_time, trigger, limit)
                                usage.available -= 1
                                return _fixed_window_snapshot(usage, pending_after)

                            snapshot = await get_limiter_storage().mutate_fix_window(
                                name, key.identity, initial, _commit
                            )
                            named_bucket_coordinator.release(key, 1)
                            return snapshot

                    async def _release_action() -> None:
                        async with lock:
                            named_bucket_coordinator.release(key, 1)

                    reservation = _reserve_increaser(state, _commit_action, _release_action)

            if allowed:
                handle = FixedWindowHandle(
                    _read_named,
                    reservation,
                    committed=committed_snapshot,
                )
                try:
                    yield handle
                finally:
                    if reservation is not None:
                        await reservation.release()
                return

            await reject_cb()
            raise FinishedException()

        if entity_id not in bucket:
            bucket[entity_id] = FixWindowUsage(now, limit)
        usage = bucket[entity_id]

        _advance_fixed_window(usage, now, trigger, limit)

        async def _read_unnamed() -> FixedWindowSnapshot:
            read_time = datetime.now(tz=_tz)
            read_usage = FixWindowUsage(usage.start_time, usage.available)
            _advance_fixed_window(read_usage, read_time, trigger, limit)
            return _fixed_window_snapshot(read_usage, usage.pending)

        if usage.available - usage.pending > 0:
            if not set_increaser:
                usage.available -= 1
                snapshot = _fixed_window_snapshot(usage, usage.pending)
                yield FixedWindowHandle(_read_unnamed, committed=snapshot)
                return

            async def _commit_action() -> FixedWindowSnapshot:
                commit_time = datetime.now(tz=_tz)
                _advance_fixed_window(usage, commit_time, trigger, limit)
                usage.pending -= 1
                usage.available -= 1
                return _fixed_window_snapshot(usage, usage.pending)

            async def _release_action() -> None:
                usage.pending -= 1

            reservation = _reserve_increaser(state, _commit_action, _release_action)
            usage.pending += 1
            try:
                yield FixedWindowHandle(_read_unnamed, reservation)
            finally:
                await reservation.release()
            return

        # Exceeded
        await reject_cb()
        raise FinishedException()

    return Depends(_limiter_dependency, use_cache=False)


# endregion


# region: SlidingWindow
def SlidingWindowCooldown(
    entity: CooldownEntity | _DependentCallable[str],
    period: int | timedelta,
    *,
    limit: int | _DependentCallable[int] = 5,
    reject: None | SupportMsgType | _DependentCallable[Any] = None,
    set_increaser: bool = False,
    name: None | str = None,
):
    """
    **滑动窗口速率限制**

    用于限制指定对象在任意长度为设定周期的时间窗口内的消息触发次数。

    参数:
        entity (CooldownEntity | _DependentCallable[str]):
            设置需要进行速率限制的对象。
            - 可传入 `CooldownEntity` 对象，如 `UserScope`, `SceneScope` 等。
            - 可传入返回值为 `str` 的函数，自定义限制对象的**唯一 ID**，支持依赖注入。

        period (int | datetime.timedelta):
            设置滑动窗口的时间长度，且必须大于零。

        limit (int | _DependentCallable[int]):
            可选，设置在每个滑动窗口周期内允许的最大触发次数。默认为 5。
            - 可传入返回值为 `int` 的函数，自定义最大触发次数，支持依赖注入。
            - 静态值和依赖注入返回值都不能为负数。

        reject (None | SupportMsgType):
            可选，当超出限制时的响应行为。默认为 `None`。
            - 若为 `str` 或消息对象，将作为限制使用时的提示消息发送给用户。
            - 若为依赖注入函数，将会在拒绝时进行调用。

        set_increaser (bool):
            可选，是否获取限制器的增加器。默认为 False。
            - 启用后会先预占额度；业务成功时可执行当前 Handle 或批量 Increaser 确认计数。
            - 未确认的预占会在 Matcher 结束时自动释放。

        name (None | str):
            可选，设置当前限制器的使用统计集合。默认为 `None` ，即私有集合。
            - 当传入 `str` ，将创建或加入一个同名公共集合，可用于与其他命令的限制器共享使用统计。

    Returns:
        可作为 parameterless 依赖使用的 NoneBot `Depends`；声明为具名 Handler 参数时注入
        `SlidingWindowHandle`，用于只读状态或逐项确认预占。

    Raises:
        ValueError: 当周期不大于零，或静态、动态 `limit` 为负数时抛出。

    示例:
    ```python
    from nonebot.permission import SUPERUSER
    from nonebot_plugin_limiter.entity import UserScope

    # 任意一分钟内最多触发 5 次
    @matcher.handle(parameterless=[
        SlidingWindowCooldown(
            UserScope(permission=SUPERUSER),
            60,
            limit=5,
            reject="请求过于频繁，请稍后再试。"
        )
    ])
    async def handler(...): ...
    ```
    """

    if isinstance(limit, int):
        _validate_non_negative("limit", limit)

    if isinstance(period, timedelta):
        window_length = period.total_seconds()
    else:
        window_length = period
    _validate_positive("period", window_length)

    bucket: dict[str, SlidingWindowUsage] = {}

    async def _limiter_dependency(
        state: T_State,
        entity_id: str = Depends(_entity_id_dep_wrapper(entity)),
        limit: int = Depends(_limit_dep_wrapper(limit)),
        reject_cb: Callable[..., Awaitable[Any]] = Depends(_reject_dep_wrapper(reject)),
    ) -> AsyncGenerator[SlidingWindowHandle, None]:
        if set_increaser:
            _ensure_increaser(state)
        if entity_id == BYPASS_ENTITY:
            snapshot = SlidingWindowSnapshot(None)

            async def _read_bypass() -> SlidingWindowSnapshot:
                return snapshot

            yield SlidingWindowHandle(_read_bypass, committed=snapshot)
            return
        _validate_non_negative("limit", limit)

        now = datetime.now(tz=_tz)

        if name is not None:
            key = _named_bucket_key("sliding", name, entity_id)
            lock = named_bucket_coordinator.lock(key)
            initial = SlidingWindowUsage()
            reservation = None

            async def _read_named() -> SlidingWindowSnapshot:
                read_time = datetime.now(tz=_tz)
                async with lock:
                    read_usage = await get_limiter_storage().read_sliding_window(name, key.identity)
                    if read_usage is None:
                        read_usage = SlidingWindowUsage()
                    _drop_expired_sliding_window(read_usage, read_time, window_length)
                    return _sliding_window_snapshot(read_usage, limit, named_bucket_coordinator.pending(key))

            async with lock:
                pending = named_bucket_coordinator.pending(key)

                def _admit(usage: SlidingWindowUsage) -> tuple[bool, SlidingWindowSnapshot]:
                    _drop_expired_sliding_window(usage, now, window_length)
                    if len(usage.timestamps) + pending >= limit:
                        return False, _sliding_window_snapshot(usage, limit, pending)
                    if not set_increaser:
                        usage.timestamps.append(now)
                    return True, _sliding_window_snapshot(usage, limit, pending)

                allowed, committed_snapshot = await get_limiter_storage().mutate_sliding_window(
                    name, key.identity, initial, _admit
                )
                if allowed and set_increaser:
                    named_bucket_coordinator.reserve(key, 1)

                    async def _commit_action() -> SlidingWindowSnapshot:
                        async with lock:
                            commit_time = datetime.now(tz=_tz)
                            pending_after = named_bucket_coordinator.pending(key) - 1

                            def _commit(usage: SlidingWindowUsage) -> SlidingWindowSnapshot:
                                _drop_expired_sliding_window(usage, commit_time, window_length)
                                usage.timestamps.append(commit_time)
                                return _sliding_window_snapshot(usage, limit, pending_after)

                            snapshot = await get_limiter_storage().mutate_sliding_window(
                                name, key.identity, initial, _commit
                            )
                            named_bucket_coordinator.release(key, 1)
                            return snapshot

                    async def _release_action() -> None:
                        async with lock:
                            named_bucket_coordinator.release(key, 1)

                    reservation = _reserve_increaser(state, _commit_action, _release_action)

            if allowed:
                handle = SlidingWindowHandle(
                    _read_named,
                    reservation,
                    committed=committed_snapshot,
                )
                try:
                    yield handle
                finally:
                    if reservation is not None:
                        await reservation.release()
                return

            await reject_cb()
            raise FinishedException()

        if entity_id not in bucket:
            bucket[entity_id] = SlidingWindowUsage()
        usage = bucket[entity_id]

        _drop_expired_sliding_window(usage, now, window_length)

        async def _read_unnamed() -> SlidingWindowSnapshot:
            read_time = datetime.now(tz=_tz)
            read_usage = SlidingWindowUsage(usage.timestamps.copy())
            _drop_expired_sliding_window(read_usage, read_time, window_length)
            return _sliding_window_snapshot(read_usage, limit, usage.pending)

        if len(usage.timestamps) + usage.pending < limit:
            if not set_increaser:
                usage.timestamps.append(now)
                snapshot = _sliding_window_snapshot(usage, limit, usage.pending)
                yield SlidingWindowHandle(_read_unnamed, committed=snapshot)
                return

            async def _commit_action() -> SlidingWindowSnapshot:
                commit_time = datetime.now(tz=_tz)
                _drop_expired_sliding_window(usage, commit_time, window_length)
                usage.pending -= 1
                usage.timestamps.append(commit_time)
                return _sliding_window_snapshot(usage, limit, usage.pending)

            async def _release_action() -> None:
                usage.pending -= 1

            reservation = _reserve_increaser(state, _commit_action, _release_action)
            usage.pending += 1
            try:
                yield SlidingWindowHandle(_read_unnamed, reservation)
            finally:
                await reservation.release()
            return  # Didn't exceed

        # Exceeded
        await reject_cb()
        raise FinishedException()

    return Depends(_limiter_dependency, use_cache=False)


# endregion


# region: LeakyBucket
def LeakyBucketCooldown(
    entity: CooldownEntity | _DependentCallable[str],
    capacity: int,
    leak_speed: int,
    *,
    pour_size: int | _DependentCallable[int] = 10,
    reject: None | SupportMsgType | _DependentCallable[Any] = None,
    set_increaser: bool = False,
    name: None | str = None,
):
    """
    **漏桶算法限制器**

    用于控制给定时间内能处理的最大请求量。

    参数:
        entity (CooldownEntity | _DependentCallable[str]):
            设置需要进行速率限制的对象。
            - 可传入 `CooldownEntity` 对象，如 `UserScope`, `SceneScope` 等。
            - 可传入返回值为 `str` 的函数，自定义限制对象的**唯一 ID**，支持依赖注入。

        capacity (int):
            设置漏桶的最大容量，且不能为负数。新实体从空桶开始。

        leak_speed (int):
            设置漏桶每秒的漏水量，且不能为负数。

        pour_size (int, _DependentCallable[int]):
            可选，设置每次添加任务时倒入的水量。默认为 10。
            - 可传入返回值为 `int` 的函数，自定义注入水量，支持依赖注入。
            - 静态值和依赖注入返回值都不能为负数。

        reject (None | SupportMsgType | _DependentCallable):
            可选，当超出限制时的响应行为。默认为 `None`。
            - 若为 `str` 或消息对象，将作为限制使用时的提示消息发送给用户。
            - 若为依赖注入函数，将会在拒绝时进行调用。

        set_increaser (bool):
            可选，是否获取限制器的增加器。默认为 False。
            - 启用后会先预占额度；业务成功时可执行当前 Handle 或批量 Increaser 确认计数。
            - 未确认的预占会在 Matcher 结束时自动释放。

        name (None | str):
            可选，设置当前限制器的使用统计集合。默认为 `None` ，即私有集合。
            - 当传入 `str` ，将创建或加入一个同名公共集合，可用于与其他命令的限制器共享使用统计。

    Returns:
        可作为 parameterless 依赖使用的 NoneBot `Depends`；声明为具名 Handler 参数时注入
        `LeakyBucketHandle`，用于只读状态或逐项确认预占。

    Raises:
        ValueError: 当容量、恢复速度或静态、动态单次注入量为负数时抛出。

    示例:
    ```python
    from nonebot.permission import SUPERUSER
    from nonebot_plugin_limiter.entity import UserScope

    # 收到一个请求，处理消化这个请求需要 10 秒，最多同时处理两个请求
    @matcher.handle(parameterless=[
        LeakyBucketCooldown(
            UserScope(permission=SUPERUSER),
            20,
            1,
            pour_size = 10,
            reject="操作过于频繁，请稍后再试。"
        )
    ])
    async def handler(...): ...
    ```
    """

    _validate_non_negative("capacity", capacity)
    _validate_non_negative("leak_speed", leak_speed)
    if isinstance(pour_size, int):
        _validate_non_negative("pour_size", pour_size)

    bucket: dict[str, LeakyBucketUsage] = {}

    async def _limiter_dependency(
        state: T_State,
        entity_id: str = Depends(_entity_id_dep_wrapper(entity)),
        pour_size: int = Depends(_limit_dep_wrapper(pour_size)),
        reject_cb: Callable[..., Awaitable[Any]] = Depends(_reject_dep_wrapper(reject)),
    ) -> AsyncGenerator[LeakyBucketHandle, None]:
        if set_increaser:
            _ensure_increaser(state)
        if entity_id == BYPASS_ENTITY:
            snapshot = LeakyBucketSnapshot(None, None)

            async def _read_bypass() -> LeakyBucketSnapshot:
                return snapshot

            yield LeakyBucketHandle(_read_bypass, committed=snapshot)
            return
        _validate_non_negative("pour_size", pour_size)

        now = datetime.now(tz=_tz)

        if name is not None:
            key = _named_bucket_key("leaky", name, entity_id)
            lock = named_bucket_coordinator.lock(key)
            initial = LeakyBucketUsage(now, capacity, 0)
            reservation = None

            async def _read_named() -> LeakyBucketSnapshot:
                read_time = datetime.now(tz=_tz)
                async with lock:
                    read_usage = await get_limiter_storage().read_leaky_bucket(name, key.identity)
                    if read_usage is None:
                        read_usage = LeakyBucketUsage(read_time, capacity, 0)
                    _leak_bucket(read_usage, read_time, leak_speed)
                    return _leaky_bucket_snapshot(read_usage, named_bucket_coordinator.pending(key))

            async with lock:
                pending = named_bucket_coordinator.pending(key)

                def _admit(usage: LeakyBucketUsage) -> tuple[bool, LeakyBucketSnapshot]:
                    _leak_bucket(usage, now, leak_speed)
                    if usage.used + pending + pour_size > usage.capacity:
                        return False, _leaky_bucket_snapshot(usage, pending)
                    if not set_increaser:
                        usage.used += pour_size
                    return True, _leaky_bucket_snapshot(usage, pending)

                allowed, committed_snapshot = await get_limiter_storage().mutate_leaky_bucket(
                    name, key.identity, initial, _admit
                )
                if allowed and set_increaser:
                    named_bucket_coordinator.reserve(key, pour_size)

                    async def _commit_action() -> LeakyBucketSnapshot:
                        async with lock:
                            commit_time = datetime.now(tz=_tz)
                            pending_after = named_bucket_coordinator.pending(key) - pour_size

                            def _commit(usage: LeakyBucketUsage) -> LeakyBucketSnapshot:
                                _leak_bucket(usage, commit_time, leak_speed)
                                usage.used += pour_size
                                return _leaky_bucket_snapshot(usage, pending_after)

                            snapshot = await get_limiter_storage().mutate_leaky_bucket(
                                name, key.identity, initial, _commit
                            )
                            named_bucket_coordinator.release(key, pour_size)
                            return snapshot

                    async def _release_action() -> None:
                        async with lock:
                            named_bucket_coordinator.release(key, pour_size)

                    reservation = _reserve_increaser(state, _commit_action, _release_action)

            if allowed:
                handle = LeakyBucketHandle(
                    _read_named,
                    reservation,
                    committed=committed_snapshot,
                )
                try:
                    yield handle
                finally:
                    if reservation is not None:
                        await reservation.release()
                return

            await reject_cb()
            raise FinishedException()

        if entity_id not in bucket:
            bucket[entity_id] = LeakyBucketUsage(now, capacity, 0)
        usage = bucket[entity_id]

        _leak_bucket(usage, now, leak_speed)

        async def _read_unnamed() -> LeakyBucketSnapshot:
            read_time = datetime.now(tz=_tz)
            read_usage = LeakyBucketUsage(usage.last_update_time, usage.capacity, usage.used)
            _leak_bucket(read_usage, read_time, leak_speed)
            return _leaky_bucket_snapshot(read_usage, usage.pending)

        if usage.used + usage.pending + pour_size <= usage.capacity:
            if not set_increaser:
                usage.used += pour_size
                snapshot = _leaky_bucket_snapshot(usage, usage.pending)
                yield LeakyBucketHandle(_read_unnamed, committed=snapshot)
                return

            async def _commit_action() -> LeakyBucketSnapshot:
                commit_time = datetime.now(tz=_tz)
                _leak_bucket(usage, commit_time, leak_speed)
                usage.pending -= pour_size
                usage.used += pour_size
                return _leaky_bucket_snapshot(usage, usage.pending)

            async def _release_action() -> None:
                usage.pending -= pour_size

            reservation = _reserve_increaser(state, _commit_action, _release_action)
            usage.pending += pour_size
            try:
                yield LeakyBucketHandle(_read_unnamed, reservation)
            finally:
                await reservation.release()
            return  # Didn't exceed

        # Exceeded
        await reject_cb()
        raise FinishedException()

    return Depends(_limiter_dependency, use_cache=False)


# endregion


# region: TokenBucket
def TokenBucketCooldown(
    entity: CooldownEntity | _DependentCallable[str],
    capacity: int,
    add_speed: int,
    *,
    consume_size: int | _DependentCallable[int] = 10,
    reject: None | SupportMsgType | _DependentCallable[Any] = None,
    set_increaser: bool = False,
    name: None | str = None,
):
    """
    **令牌桶算法限制器**

    用于控制给定资源内能处理的最大请求量。

    参数:
        entity (CooldownEntity | _DependentCallable[str]):
            设置需要进行速率限制的对象。
            - 可传入 `CooldownEntity` 对象，如 `UserScope`, `SceneScope` 等。
            - 可传入返回值为 `str` 的函数，自定义限制对象的**唯一 ID**，支持依赖注入。

        capacity (int):
            设置令牌桶的最大容量，且不能为负数。新实体从满桶开始。

        add_speed (int):
            设置令牌桶每秒添加 token 的数量，且不能为负数。

        consume_size (int, _DependentCallable[int]):
            可选，设置每次添加任务时需要消耗的 token 数量。默认为 10。
            - 可传入返回值为 `int` 的函数，自定义消耗数量，支持依赖注入。
            - 静态值和依赖注入返回值都不能为负数。

        reject (None | SupportMsgType | _DependentCallable):
            可选，当超出限制时的响应行为。默认为 `None`。
            - 若为 `str` 或消息对象，将作为限制使用时的提示消息发送给用户。
            - 若为依赖注入函数，将会在拒绝时进行调用。

        set_increaser (bool):
            可选，是否获取限制器的增加器。默认为 False。
            - 启用后会先预占额度；业务成功时可执行当前 Handle 或批量 Increaser 确认计数。
            - 未确认的预占会在 Matcher 结束时自动释放。

        name (None | str):
            可选，设置当前限制器的使用统计集合。默认为 `None` ，即私有集合。
            - 当传入 `str` ，将创建或加入一个同名公共集合，可用于与其他命令的限制器共享使用统计。

    Returns:
        可作为 parameterless 依赖使用的 NoneBot `Depends`；声明为具名 Handler 参数时注入
        `TokenBucketHandle`，用于只读状态或逐项确认预占。

    Raises:
        ValueError: 当容量、恢复速度或静态、动态单次消费量为负数时抛出。

    示例:
    ```python
    from nonebot.permission import SUPERUSER
    from nonebot_plugin_limiter.entity import UserScope

    # 收到一个请求，处理消化这个请求需要 10 个 token ，最多同时处理两个请求
    @matcher.handle(parameterless=[
        TokenBucketCooldown(
            UserScope(permission=SUPERUSER),
            20,
            1,
            consume_size = 10,
            reject="操作过于频繁，请稍后再试。"
        )
    ])
    async def handler(...): ...
    ```
    """

    _validate_non_negative("capacity", capacity)
    _validate_non_negative("add_speed", add_speed)
    if isinstance(consume_size, int):
        _validate_non_negative("consume_size", consume_size)

    bucket: dict[str, TokenBucketUsage] = {}

    async def _limiter_dependency(
        state: T_State,
        entity_id: str = Depends(_entity_id_dep_wrapper(entity)),
        consume_size: int = Depends(_limit_dep_wrapper(consume_size)),
        reject_cb: Callable[..., Awaitable[Any]] = Depends(_reject_dep_wrapper(reject)),
    ) -> AsyncGenerator[TokenBucketHandle, None]:
        if set_increaser:
            _ensure_increaser(state)
        if entity_id == BYPASS_ENTITY:
            snapshot = TokenBucketSnapshot(None)

            async def _read_bypass() -> TokenBucketSnapshot:
                return snapshot

            yield TokenBucketHandle(_read_bypass, committed=snapshot)
            return
        _validate_non_negative("consume_size", consume_size)

        now = datetime.now(tz=_tz)

        if name is not None:
            key = _named_bucket_key("token", name, entity_id)
            lock = named_bucket_coordinator.lock(key)
            initial = TokenBucketUsage(now, capacity, capacity)
            reservation = None

            async def _read_named() -> TokenBucketSnapshot:
                read_time = datetime.now(tz=_tz)
                async with lock:
                    read_usage = await get_limiter_storage().read_token_bucket(name, key.identity)
                    if read_usage is None:
                        read_usage = TokenBucketUsage(read_time, capacity, capacity)
                    _refill_token_bucket(read_usage, read_time, add_speed)
                    return _token_bucket_snapshot(read_usage, named_bucket_coordinator.pending(key))

            async with lock:
                pending = named_bucket_coordinator.pending(key)

                def _admit(usage: TokenBucketUsage) -> tuple[bool, TokenBucketSnapshot]:
                    _refill_token_bucket(usage, now, add_speed)
                    if usage.available - pending < consume_size:
                        return False, _token_bucket_snapshot(usage, pending)
                    if not set_increaser:
                        usage.available -= consume_size
                    return True, _token_bucket_snapshot(usage, pending)

                allowed, committed_snapshot = await get_limiter_storage().mutate_token_bucket(
                    name, key.identity, initial, _admit
                )
                if allowed and set_increaser:
                    named_bucket_coordinator.reserve(key, consume_size)

                    async def _commit_action() -> TokenBucketSnapshot:
                        async with lock:
                            commit_time = datetime.now(tz=_tz)
                            pending_after = named_bucket_coordinator.pending(key) - consume_size

                            def _commit(usage: TokenBucketUsage) -> TokenBucketSnapshot:
                                _refill_token_bucket(usage, commit_time, add_speed)
                                usage.available -= consume_size
                                return _token_bucket_snapshot(usage, pending_after)

                            snapshot = await get_limiter_storage().mutate_token_bucket(
                                name, key.identity, initial, _commit
                            )
                            named_bucket_coordinator.release(key, consume_size)
                            return snapshot

                    async def _release_action() -> None:
                        async with lock:
                            named_bucket_coordinator.release(key, consume_size)

                    reservation = _reserve_increaser(state, _commit_action, _release_action)

            if allowed:
                handle = TokenBucketHandle(
                    _read_named,
                    reservation,
                    committed=committed_snapshot,
                )
                try:
                    yield handle
                finally:
                    if reservation is not None:
                        await reservation.release()
                return

            await reject_cb()
            raise FinishedException()

        if entity_id not in bucket:
            bucket[entity_id] = TokenBucketUsage(now, capacity, capacity)
        usage = bucket[entity_id]

        _refill_token_bucket(usage, now, add_speed)

        async def _read_unnamed() -> TokenBucketSnapshot:
            read_time = datetime.now(tz=_tz)
            read_usage = TokenBucketUsage(usage.last_update_time, usage.capacity, usage.available)
            _refill_token_bucket(read_usage, read_time, add_speed)
            return _token_bucket_snapshot(read_usage, usage.pending)

        if usage.available - usage.pending >= consume_size:
            if not set_increaser:
                usage.available -= consume_size
                snapshot = _token_bucket_snapshot(usage, usage.pending)
                yield TokenBucketHandle(_read_unnamed, committed=snapshot)
                return

            async def _commit_action() -> TokenBucketSnapshot:
                commit_time = datetime.now(tz=_tz)
                _refill_token_bucket(usage, commit_time, add_speed)
                usage.pending -= consume_size
                usage.available -= consume_size
                return _token_bucket_snapshot(usage, usage.pending)

            async def _release_action() -> None:
                usage.pending -= consume_size

            reservation = _reserve_increaser(state, _commit_action, _release_action)
            usage.pending += consume_size
            try:
                yield TokenBucketHandle(_read_unnamed, reservation)
            finally:
                await reservation.release()
            return  # Didn't exceed

        # Exceeded
        await reject_cb()
        raise FinishedException()

    return Depends(_limiter_dependency, use_cache=False)


# endregion
