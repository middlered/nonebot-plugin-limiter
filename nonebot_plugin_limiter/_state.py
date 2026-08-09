from collections import deque
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class FixWindowUsage:
    start_time: datetime
    available: int
    pending: int = 0


@dataclass
class SlidingWindowUsage:
    timestamps: deque[datetime] = field(default_factory=deque)
    pending: int = 0


@dataclass
class LeakyBucketUsage:
    last_update_time: datetime
    capacity: int
    used: int
    pending: int = 0


@dataclass
class TokenBucketUsage:
    last_update_time: datetime
    capacity: int
    available: int
    pending: int = 0
