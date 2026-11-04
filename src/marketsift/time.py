"""Instants used by acquisition and local queries."""

from datetime import UTC, datetime


def instant(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("include a UTC offset in times, for example +08:00")
    return value.astimezone(UTC)


def stamp(value=None):
    return instant(value or datetime.now(UTC)).isoformat(timespec="microseconds")
