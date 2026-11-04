"""Pure path measurements; requested coverage and observed subtotals differ."""

from collections import defaultdict

from .time import instant, stamp


def eligible(rows, *, as_of=None):
    cutoff = stamp(as_of)
    grouped = defaultdict(list)
    for row in rows:
        grouped[
            tuple(
                row[k] for k in ("source", "instrument", "interval", "basis", "start")
            )
        ].append(row)
    good, excluded = [], []
    for versions in grouped.values():
        for row in versions:
            reason = (
                "conflict"
                if row.get("conflict")
                else "multiple selected versions; select latest or exact version"
                if len(versions) > 1
                else "forming"
                if not row.get("complete") or row["end"] > cutoff
                else None
            )
            (excluded if reason else good).append(
                dict(row, exclusion=reason) if reason else row
            )

    return good, excluded


def measure(path, *, window=None):
    if not path:
        return None
    path = sorted(path, key=lambda r: r["start"])
    start, end = path[0]["start"], path[-1]["end"]
    requested = (
        dict(since=stamp(window["since"]), until=stamp(window["until"]))
        if window
        else dict(since=start, until=end)
    )
    endpoints = start == requested["since"] and end == requested["until"]
    continuous = all(a["end"] == b["start"] for a, b in zip(path, path[1:]))
    complete_span = endpoints and continuous
    opening, closing = path[0]["open"], path[-1]["close"]
    price_units = {r["units"].get("price") for r in path}
    price_consistent = len(price_units) == 1
    observed_return = (
        closing / opening - 1
        if price_consistent and opening and closing is not None
        else None
    )
    peak, drawdown = opening, 0
    for row in path:
        close = row["close"]
        if close is not None:
            peak = max(peak or close, close)
            if peak:
                drawdown = max(drawdown, 1 - close / peak)
    result = dict(
        bars=len(path),
        start=start,
        end=end,
        requested_window=requested,
        open=opening,
        close=closing,
        high=max((r["high"] for r in path if r["high"] is not None), default=None),
        low=min((r["low"] for r in path if r["low"] is not None), default=None),
        observed_return=observed_return,
        window_return=observed_return if endpoints else None,
        observed_close_drawdown=drawdown
        if price_consistent and any(r["close"] is not None for r in path)
        else None,
        close_drawdown=drawdown
        if complete_span
        and price_consistent
        and all(r["close"] is not None for r in path)
        else None,
        coverage=dict(
            endpoints=endpoints,
            continuous_observed_span=continuous,
            full_contiguous_span=complete_span,
            expected_session_slots=None,
        ),
        bar_ids=[r["id"] for r in path],
        price_unit=next(iter(price_units)) if price_consistent else None,
    )
    for metric in ("amount", "volume"):
        units = {r["units"].get(metric) for r in path}
        unit = next(iter(units)) if len(units) == 1 else None
        subtotal = defaultdict(float)
        for row in path:
            if row.get(metric) is not None:
                subtotal[row["units"].get(metric) or "unknown"] += row[metric]
        total = (
            sum(r[metric] for r in path)
            if unit and all(r.get(metric) is not None for r in path)
            else None
        )
        result.update(
            {
                metric: total,
                metric + "_unit": unit,
                "known_" + metric + "_by_unit": dict(subtotal),
                "window_" + metric: total if complete_span else None,
            }
        )
    return result


def summarize(rows, *, window=None, as_of=None, instruments=(), paths=False):
    good, excluded = eligible(rows, as_of=as_of)
    boundary = []
    if window:
        a, b = instant(window["since"]), instant(window["until"])
        boundary = [
            r
            for r in good
            if instant(r["start"]) < b
            and instant(r["end"]) > a
            and not (a <= instant(r["start"]) and instant(r["end"]) <= b)
        ]
        good = [r for r in good if a <= instant(r["start"]) and instant(r["end"]) <= b]
    groups = defaultdict(list)
    for row in good:
        groups[
            tuple(row[k] for k in ("source", "instrument", "interval", "basis"))
            + (row["units"].get("price"),)
        ].append(row)
    result = []
    for key, path in groups.items():
        row = dict(
            zip(("source", "instrument", "interval", "basis", "price_unit"), key)
        )
        row.update(measure(path, window=window))
        if paths:
            row["path"] = path
        result.append(row)
    return dict(
        rows=result,
        excluded=excluded,
        boundary_bars=boundary,
        missing_instruments=sorted(set(instruments) - {r["instrument"] for r in good}),
        meaning="Window return uses endpoints, not previous close. Amount is observed subtotal; window_amount requires continuous coverage. Calendar coverage unknown. No flow/tradability inference.",
    )
