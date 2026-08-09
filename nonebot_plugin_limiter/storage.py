from collections import deque
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Protocol, TypeVar

from nonebot_plugin_orm import get_session
from sqlalchemy import delete, select
from sqlalchemy.exc import SQLAlchemyError

from ._state import FixWindowUsage, LeakyBucketUsage, SlidingWindowUsage, TokenBucketUsage
from .identity import EntityIdentity
from .model import (
    FixWindowState,
    LeakyBucketState,
    SlidingWindowEntry,
    SlidingWindowState,
    TokenBucketState,
)

_T = TypeVar("_T")


class LimiterStorageError(RuntimeError):
    """表示命名 bucket 无法完成可靠的数据库事务。"""


class LimiterStorage(Protocol):
    async def mutate_fix_window(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: FixWindowUsage,
        mutate: Callable[[FixWindowUsage], _T],
    ) -> _T: ...

    async def mutate_sliding_window(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: SlidingWindowUsage,
        mutate: Callable[[SlidingWindowUsage], _T],
    ) -> _T: ...

    async def mutate_leaky_bucket(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: LeakyBucketUsage,
        mutate: Callable[[LeakyBucketUsage], _T],
    ) -> _T: ...

    async def mutate_token_bucket(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: TokenBucketUsage,
        mutate: Callable[[TokenBucketUsage], _T],
    ) -> _T: ...


def _to_microseconds(value: datetime) -> int:
    return round(value.timestamp() * 1_000_000)


def _from_microseconds(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)


def _identity_filter(model: type, bucket_name: str, identity: EntityIdentity):
    return (
        model.bucket_name == bucket_name,
        model.entity_key_schema == identity.schema,
        model.entity_key == identity.key,
    )


class OrmLimiterStorage:
    async def mutate_fix_window(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: FixWindowUsage,
        mutate: Callable[[FixWindowUsage], _T],
    ) -> _T:
        try:
            async with get_session() as session, session.begin():
                statement = (
                    select(FixWindowState)
                    .where(*_identity_filter(FixWindowState, bucket_name, identity))
                    .with_for_update()
                )
                row = await session.scalar(statement)
                if row is None:
                    row = FixWindowState(
                        bucket_name=bucket_name,
                        entity_key_schema=identity.schema,
                        entity_key=identity.key,
                        start_time_us=_to_microseconds(initial.start_time),
                        available=initial.available,
                    )
                    session.add(row)

                usage = FixWindowUsage(_from_microseconds(row.start_time_us), row.available)
                result = mutate(usage)
                row.start_time_us = _to_microseconds(usage.start_time)
                row.available = usage.available
                return result
        except (SQLAlchemyError, OSError) as exception:
            raise LimiterStorageError(f"Failed to update named fixed-window bucket {bucket_name!r}.") from exception

    async def mutate_sliding_window(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: SlidingWindowUsage,
        mutate: Callable[[SlidingWindowUsage], _T],
    ) -> _T:
        try:
            async with get_session() as session, session.begin():
                statement = (
                    select(SlidingWindowState)
                    .where(*_identity_filter(SlidingWindowState, bucket_name, identity))
                    .with_for_update()
                )
                row = await session.scalar(statement)
                if row is None:
                    row = SlidingWindowState(
                        bucket_name=bucket_name,
                        entity_key_schema=identity.schema,
                        entity_key=identity.key,
                    )
                    session.add(row)
                    await session.flush()
                    timestamps = deque(initial.timestamps)
                else:
                    timestamps = deque(
                        _from_microseconds(value)
                        for value in (
                            await session.scalars(
                                select(SlidingWindowEntry.timestamp_us)
                                .where(SlidingWindowEntry.state_id == row.id)
                                .order_by(SlidingWindowEntry.position)
                            )
                        ).all()
                    )

                usage = SlidingWindowUsage(timestamps)
                result = mutate(usage)
                await session.execute(delete(SlidingWindowEntry).where(SlidingWindowEntry.state_id == row.id))
                session.add_all(
                    SlidingWindowEntry(
                        state_id=row.id,
                        position=position,
                        timestamp_us=_to_microseconds(timestamp),
                    )
                    for position, timestamp in enumerate(usage.timestamps)
                )
                return result
        except (SQLAlchemyError, OSError) as exception:
            raise LimiterStorageError(f"Failed to update named sliding-window bucket {bucket_name!r}.") from exception

    async def mutate_leaky_bucket(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: LeakyBucketUsage,
        mutate: Callable[[LeakyBucketUsage], _T],
    ) -> _T:
        try:
            async with get_session() as session, session.begin():
                statement = (
                    select(LeakyBucketState)
                    .where(*_identity_filter(LeakyBucketState, bucket_name, identity))
                    .with_for_update()
                )
                row = await session.scalar(statement)
                if row is None:
                    row = LeakyBucketState(
                        bucket_name=bucket_name,
                        entity_key_schema=identity.schema,
                        entity_key=identity.key,
                        last_update_time_us=_to_microseconds(initial.last_update_time),
                        capacity=initial.capacity,
                        used=initial.used,
                    )
                    session.add(row)

                usage = LeakyBucketUsage(
                    _from_microseconds(row.last_update_time_us),
                    row.capacity,
                    row.used,
                )
                result = mutate(usage)
                row.last_update_time_us = _to_microseconds(usage.last_update_time)
                row.capacity = usage.capacity
                row.used = usage.used
                return result
        except (SQLAlchemyError, OSError) as exception:
            raise LimiterStorageError(f"Failed to update named leaky bucket {bucket_name!r}.") from exception

    async def mutate_token_bucket(
        self,
        bucket_name: str,
        identity: EntityIdentity,
        initial: TokenBucketUsage,
        mutate: Callable[[TokenBucketUsage], _T],
    ) -> _T:
        try:
            async with get_session() as session, session.begin():
                statement = (
                    select(TokenBucketState)
                    .where(*_identity_filter(TokenBucketState, bucket_name, identity))
                    .with_for_update()
                )
                row = await session.scalar(statement)
                if row is None:
                    row = TokenBucketState(
                        bucket_name=bucket_name,
                        entity_key_schema=identity.schema,
                        entity_key=identity.key,
                        last_update_time_us=_to_microseconds(initial.last_update_time),
                        capacity=initial.capacity,
                        available=initial.available,
                    )
                    session.add(row)

                usage = TokenBucketUsage(
                    _from_microseconds(row.last_update_time_us),
                    row.capacity,
                    row.available,
                )
                result = mutate(usage)
                row.last_update_time_us = _to_microseconds(usage.last_update_time)
                row.capacity = usage.capacity
                row.available = usage.available
                return result
        except (SQLAlchemyError, OSError) as exception:
            raise LimiterStorageError(f"Failed to update named token bucket {bucket_name!r}.") from exception


_storage: LimiterStorage = OrmLimiterStorage()


def get_limiter_storage() -> LimiterStorage:
    return _storage
