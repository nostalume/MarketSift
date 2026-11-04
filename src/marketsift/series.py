"""Native period changes, revisions and observations available at a time."""

from collections import defaultdict

from .time import instant, stamp


def changes(rows, *, axis):
    if axis not in {"period", "revision"}:
        raise ValueError("change axis must be period or revision")
    grouped = defaultdict(list)
    for row in rows:
        key = (row["series_id"], row["native_id"], row["period_kind"])
        if axis == "revision":
            key += (row["period_start"], row["period_end"])
        grouped[key].append(row)
    result = []
    for group in grouped.values():
        if axis == "period":
            latest = defaultdict(list)
            for r in group:
                latest[(r["period_start"], r["period_end"])].append(r)
            group = [
                r
                for v in latest.values()
                for r in v
                if r["observed_at"] == max(x["observed_at"] for x in v)
            ]
        group.sort(
            key=lambda r: (
                r["observed_at"] if axis == "revision" else r["period_start"],
                r["observed_at"],
            )
        )
        points = defaultdict(list)
        for r in group:
            points[
                r["observed_at"]
                if axis == "revision"
                else (r["period_start"], r["period_end"])
            ].append(r)
        ordered = list(points.values())
        for a, b in zip(ordered, ordered[1:]):
            before, after = a[0], b[0]
            known = len(a) == len(b) == 1 and all(
                r["value"] is not None and not r.get("conflict")
                for r in (before, after)
            )
            result.append(
                dict(
                    series=after["series"],
                    unit=after["unit"],
                    axis=axis,
                    before_ids=[r["id"] for r in a],
                    after_ids=[r["id"] for r in b],
                    before_period=[
                        before["period_kind"],
                        before["period_start"],
                        before["period_end"],
                    ],
                    after_period=[
                        after["period_kind"],
                        after["period_start"],
                        after["period_end"],
                    ],
                    difference=after["value"] - before["value"] if known else None,
                    relative_change=after["value"] / before["value"] - 1
                    if known and before["value"]
                    else None,
                    before_observed_at=before["observed_at"],
                    after_observed_at=after["observed_at"],
                )
            )
    return dict(rows=result)


def available(rows, *, at):
    cutoff = stamp(at)
    grouped = defaultdict(list)
    for row in rows:
        if row["observed_at"] <= cutoff:
            grouped[row["series"]].append(row)
    result = []
    for values in grouped.values():
        latest = max(r["observed_at"] for r in values)
        for r in values:
            if r["observed_at"] == latest:
                result.append(
                    dict(
                        r,
                        age_seconds=(instant(cutoff) - instant(latest)).total_seconds(),
                    )
                )
    return dict(
        at=cutoff,
        rows=result,
        meaning="Latest observed batch may contain multiple periods, estimates or conflicts. Native periods retained; no period forward fill.",
    )
