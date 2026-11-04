"""Small source-value helpers shared by actual adapters."""

import math
import re
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime

from ..time import instant, stamp


def number(value):
    if value in (None, "", ".", "N/A", "null"):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def from_ms(value):
    return stamp(datetime.fromtimestamp(int(value) / 1000, UTC))


def milliseconds(value):
    return int(instant(value).timestamp() * 1000)


def published(label):
    if not label:
        return None
    try:
        value = datetime.fromisoformat(label.replace("Z", "+00:00"))
    except ValueError:
        try:
            value = parsedate_to_datetime(label)
        except (ValueError, TypeError):
            return None
    return stamp(value) if value.tzinfo else None


def period(day, kind="day"):
    begin = date.fromisoformat(day)
    if kind == "year":
        end = date(begin.year + 1, 1, 1)
    elif kind in {"month", "quarter"}:
        month = begin.month + (3 if kind == "quarter" else 1)
        end = date(begin.year + (month - 1) // 12, (month - 1) % 12 + 1, 1)
    else:
        end = begin + timedelta(days=7 if kind == "week" else 1)
    return dict(
        period_kind=kind, period_start=begin.isoformat(), period_end=end.isoformat()
    )


def source_read(source, download, *, detail, request=None, status="ok", **metadata):
    result = dict(
        source=source,
        requested_at=download.requested_at,
        received_at=download.received_at,
        status=status,
        detail=detail,
        url=download.url,
        **metadata,
    )
    if request:
        for field in ("since", "until", "instruments", "datasets"):
            if request.get(field) is not None:
                result[field] = request[field]
    return result


def china_time(label):
    match = re.search(r"\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}(?::\d{2})?", label or "")
    if not match:
        return None
    from datetime import timezone

    return stamp(
        datetime.fromisoformat(match[0].replace("/", "-")).replace(
            tzinfo=timezone(timedelta(hours=8))
        )
    )


def json_binding(tree, name):
    for script in tree.select("script"):
        match = re.search(r"\bvar\s+" + re.escape(name) + r"\s*=", script.text)
        if match:
            import json

            return json.JSONDecoder().raw_decode(script.text[match.end() :].lstrip())[0]
    raise ValueError("missing native JSON binding: " + name)
