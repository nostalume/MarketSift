"""Local event/price connections, not causal labels or trading decisions."""

import re
from datetime import timedelta

from . import comparison, series
from .selection import publication
from .time import instant, stamp


def duration(value):
    if isinstance(value, (float, int)):
        return timedelta(seconds=value)
    matched = re.fullmatch(r"(\d+(?:\.\d+)?)(s|m|h|d)", value)
    if not matched:
        raise ValueError("duration needs s/m/h/d, e.g. 20m")
    return timedelta(
        seconds=float(matched[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[matched[2]]
    )


def response(
    events,
    prices,
    *,
    anchor,
    before,
    after,
    subjects=(),
    benchmark=None,
    groups=None,
    as_of=None,
    context=(),
    event_dataset="documents",
):
    if anchor not in {"published", "observed"}:
        raise ValueError("event anchor must be published or observed")
    lead, lag = duration(before), duration(after)
    if lead.total_seconds() <= 0 or lag.total_seconds() <= 0:
        raise ValueError("event windows must be positive")
    result = []
    for event in events:
        point, end = (
            publication(event)
            if anchor == "published"
            else (event["observed_at"], None)
        )
        row = dict(
            event=dict(dataset=event_dataset, id=event["id"]),
            anchor=anchor,
            subject_basis="research-specified"
            if subjects
            else "literal candidates; roles unresolved",
        )
        if point is None or end is not None or event.get("conflict"):
            row.update(
                gap="unknown/imprecise anchor or conflicting event",
                publication_label=event.get("published_label"),
            )
        else:
            at = instant(point)
            members = list(
                dict.fromkeys(
                    subjects
                    or [c for v in event.get("mentions", {}).values() for c in v]
                )
            )
            row.update(
                anchor_at=stamp(at),
                subjects=members,
                context=series.available(context, at=at) if context else None,
            )
            if not members:
                row["gap"] = "no explicit or literal subjects"
            else:
                windows = dict(
                    before=dict(since=stamp(at - lead), until=stamp(at)),
                    after=dict(since=stamp(at), until=stamp(at + lag)),
                )
                row.update(
                    after_pending=at + lag > instant(stamp(as_of)),
                    response=comparison.compare(
                        prices,
                        windows=windows,
                        pool=members,
                        benchmark=benchmark,
                        groups=groups,
                        pairs=[["before", "after"]],
                        as_of=as_of,
                    ),
                )
        result.append(row)
    return dict(
        rows=result,
        meaning="Stored observations around an explicit anchor; shared bar IDs are shared evidence, not independent confirmations or causal proof.",
    )
