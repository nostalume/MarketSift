"""Time placement over already selected facts; no implicit observation fallback."""

from datetime import timedelta

from .time import instant, stamp


def publication(row):
    point = row.get("published_at")
    precision = row.get("metadata", {}).get("publication_precision")
    if point:
        end = (
            stamp(instant(point) + timedelta(minutes=1))
            if precision == "minute"
            else None
        )
        return point, end
    bounds = row.get("metadata", {}).get("publication_bounds")
    return (bounds["since"], bounds["until"]) if bounds else (None, None)


def place(rows, time, dataset):
    if not time:
        return rows, []
    axes = {
        "documents": {"published", "observed"},
        "bars": {"event", "observed"},
        "series": {"period", "published", "observed"},
        "trades": {"event", "observed"},
        "references": {"observed"},
        "series_definitions": {"observed"},
        "acquisitions": {"received"},
        "instruments": set(),
    }
    axis = time["axis"]
    if axis not in axes[dataset]:
        raise ValueError(f"{dataset} does not support time axis {axis!r}")
    lower, upper = time.get("since"), time.get("until")
    # Date periods and aware instants are deliberately different coordinates.
    bounded = lower is not None or upper is not None
    date_axis = (
        axis == "period"
        and bounded
        and all(len(str(v)) == 10 for v in (lower, upper) if v)
    )
    if not date_axis:
        lower, upper = stamp(lower) if lower else None, stamp(upper) if upper else None
    if lower and upper and lower >= upper:
        raise ValueError("since must precede until")
    selected, unplaced = [], []
    for row in rows:
        end = None
        if axis == "published":
            point, end = publication(row)
        elif axis == "period":
            point, end = row["period_start"], row["period_end"]
            if bounded and (row["period_kind"] == "instant") == date_axis:
                unplaced.append(
                    dict(
                        row=row, reason="native period and requested coordinate differ"
                    )
                )
                continue
        elif axis == "event":
            point = row.get("start", row.get("ts"))
            end = row.get("end")
        else:
            point = row["received_at" if axis == "received" else "observed_at"]
        if point is None:
            unplaced.append(dict(row=row, reason=f"{axis} time unknown; no fallback"))
            continue
        if lower and (end <= lower if end else point < lower):
            continue
        if upper and point >= upper:
            continue
        if (
            axis == "published"
            and end
            and ((lower and point < lower) or (upper and end > upper))
        ):
            unplaced.append(
                dict(
                    row=row, reason="publication precision straddles requested boundary"
                )
            )
            continue
        selected.append(row)
    return selected, unplaced
