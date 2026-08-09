from nonebot_plugin_orm import Model
from sqlalchemy import BigInteger, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

_BIND_KEY = "nonebot_plugin_limiter"
_BUCKET_NAME_LENGTH = 255
_ENTITY_SCHEMA_LENGTH = 64
_ENTITY_KEY_LENGTH = 2048


class _EntityStateMixin:
    bucket_name: Mapped[str] = mapped_column(String(_BUCKET_NAME_LENGTH), nullable=False)
    entity_key_schema: Mapped[str] = mapped_column(String(_ENTITY_SCHEMA_LENGTH), nullable=False)
    entity_key: Mapped[str] = mapped_column(String(_ENTITY_KEY_LENGTH), nullable=False)


class FixWindowState(_EntityStateMixin, Model):
    __tablename__ = "nonebot_plugin_limiter_fix_window_state"
    __bind_key__ = _BIND_KEY
    __table_args__ = (
        UniqueConstraint(
            "bucket_name",
            "entity_key_schema",
            "entity_key",
            name="uq_limiter_fix_window_entity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    start_time_us: Mapped[int] = mapped_column(BigInteger, nullable=False)
    available: Mapped[int] = mapped_column(Integer, nullable=False)


class SlidingWindowState(_EntityStateMixin, Model):
    __tablename__ = "nonebot_plugin_limiter_sliding_window_state"
    __bind_key__ = _BIND_KEY
    __table_args__ = (
        UniqueConstraint(
            "bucket_name",
            "entity_key_schema",
            "entity_key",
            name="uq_limiter_sliding_window_entity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


class SlidingWindowEntry(Model):
    __tablename__ = "nonebot_plugin_limiter_sliding_window_entry"
    __bind_key__ = _BIND_KEY
    __table_args__ = (UniqueConstraint("state_id", "position", name="uq_limiter_sliding_window_entry_position"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    state_id: Mapped[int] = mapped_column(
        ForeignKey("nonebot_plugin_limiter_sliding_window_state.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    timestamp_us: Mapped[int] = mapped_column(BigInteger, nullable=False)


class LeakyBucketState(_EntityStateMixin, Model):
    __tablename__ = "nonebot_plugin_limiter_leaky_bucket_state"
    __bind_key__ = _BIND_KEY
    __table_args__ = (
        UniqueConstraint(
            "bucket_name",
            "entity_key_schema",
            "entity_key",
            name="uq_limiter_leaky_bucket_entity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    last_update_time_us: Mapped[int] = mapped_column(BigInteger, nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    used: Mapped[int] = mapped_column(Integer, nullable=False)


class TokenBucketState(_EntityStateMixin, Model):
    __tablename__ = "nonebot_plugin_limiter_token_bucket_state"
    __bind_key__ = _BIND_KEY
    __table_args__ = (
        UniqueConstraint(
            "bucket_name",
            "entity_key_schema",
            "entity_key",
            name="uq_limiter_token_bucket_entity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    last_update_time_us: Mapped[int] = mapped_column(BigInteger, nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    available: Mapped[int] = mapped_column(Integer, nullable=False)
