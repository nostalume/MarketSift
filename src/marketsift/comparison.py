"""Explicit windows and local comparison domains over ordinary price records."""

from collections import defaultdict
from statistics import mean

from .prices import eligible, measure
from .time import instant, stamp


def slots(rows, window, alignment):
    a, b = instant(window["since"]), instant(window["until"])
    if a >= b:
        raise ValueError("window since must precede until")
    result = {}
    for r in rows:
        start, end = instant(r["start"]), instant(r["end"])
        if a <= start and end <= b:
            key = (
                (start - a).total_seconds()
                if alignment == "window_offset"
                else stamp(start),
                (end - start).total_seconds(),
            )
            if key in result:
                raise ValueError(
                    "duplicate comparison slot; select one source/basis/version"
                )
            result[key] = r
    return result


def aggregate(members, values):
    valid = {c: values[c] for c in members if values.get(c)}
    amounts = [v["amount"] for v in valid.values()]
    units = {v["amount_unit"] for v in valid.values()}
    unit = next(iter(units)) if len(units) == 1 else None
    amount = (
        sum(amounts)
        if valid
        and len(valid) == len(members)
        and unit
        and all(a is not None for a in amounts)
        else None
    )
    returns = {
        c: v["window_return"]
        for c, v in valid.items()
        if v["window_return"] is not None
    }
    return dict(
        members=members,
        observed_members=list(valid),
        missing_members=sorted(set(members) - set(valid)),
        amount=amount,
        amount_unit=unit,
        known_amounts={c: v["known_amount_by_unit"] for c, v in valid.items()},
        return_members=list(returns),
        mean_return=mean(returns.values()) if returns else None,
        up_fraction=sum(r > 0 for r in returns.values()) / len(returns)
        if returns
        else None,
        amount_hhi=sum((a / amount) ** 2 for a in amounts) if amount else None,
        bar_ids=[i for v in valid.values() for i in v["bar_ids"]],
    )


def matched(by_code, members, window):
    common = (
        set.intersection(*(set(by_code.get(c, {})) for c in members))
        if members
        else set()
    )
    return {
        c: measure([by_code[c][s] for s in sorted(common)], window=window)
        for c in members
    }, sorted(common)


def compare(
    rows,
    *,
    windows,
    pool,
    groups=None,
    benchmark=None,
    pairs=(),
    alignment="window_offset",
    as_of=None,
    paths=False,
    local_currency=False,
):
    if alignment not in {"timestamp", "window_offset"}:
        raise ValueError("compare alignment must be timestamp or window_offset")
    if not windows or not pool:
        raise ValueError("compare needs explicit windows and pool")
    pool = list(dict.fromkeys(pool))
    definitions = {
        k: dict(
            members=list(dict.fromkeys(v if isinstance(v, list) else v["members"])),
            basis=None if isinstance(v, list) else v.get("basis"),
        )
        for k, v in (groups or {}).items()
    }
    if any(set(d["members"]) - set(pool) for d in definitions.values()):
        raise ValueError("group members must belong to pool")
    if any(a not in windows or b not in windows for a, b in pairs):
        raise ValueError("pairs must name supplied windows")
    good, excluded = eligible(rows, as_of=as_of)
    parts = defaultdict(list)
    for r in good:
        parts[
            (
                r["source"],
                r["interval"],
                r["basis"],
                None if local_currency else r["units"].get("price"),
            )
        ].append(r)
    results = []
    for key, data in parts.items():
        native, window_results = {}, {}
        codes = list(dict.fromkeys([*pool, *([benchmark] if benchmark else [])]))
        for name, window in windows.items():
            by_code = {
                c: slots([r for r in data if r["instrument"] == c], window, alignment)
                for c in codes
            }
            native[name] = by_code
            measures = {
                c: measure(list(by_code[c].values()), window=window) for c in codes
            }
            members = {}
            for c in pool:
                m = measures[c]
                pair, common = (
                    matched(by_code, [c, benchmark], window) if benchmark else ({}, [])
                )
                excess = None
                if benchmark and pair.get(c) and pair.get(benchmark):
                    a, b = pair[c], pair[benchmark]
                    if (
                        a["observed_return"] is not None
                        and b["observed_return"] is not None
                        and (
                            local_currency
                            or a["price_unit"] == b["price_unit"] is not None
                        )
                    ):
                        excess = a["observed_return"] - b["observed_return"]
                members[c] = dict(
                    observed=m,
                    excess_return=excess,
                    benchmark_slots=common,
                    gap=None
                    if m
                    else "no eligible fully contained bars in this window",
                )
                if paths and m:
                    m["path"] = list(by_code[c].values())
            pool_values, common = matched(by_code, pool, window)
            denominator = aggregate(pool, pool_values)
            group_rows = {}
            for group, definition in definitions.items():
                values, group_slots = matched(by_code, definition["members"], window)
                g = aggregate(definition["members"], values)
                numerator = aggregate(definition["members"], pool_values)
                raw_returns = {
                    c: measures[c]["window_return"]
                    for c in definition["members"]
                    if measures[c] and measures[c]["window_return"] is not None
                }
                g.update(
                    basis=definition["basis"],
                    common_slots=group_slots,
                    return_members=list(raw_returns),
                    mean_return=mean(raw_returns.values()) if raw_returns else None,
                    up_fraction=sum(v > 0 for v in raw_returns.values())
                    / len(raw_returns)
                    if raw_returns
                    else None,
                    observed={c: measures[c] for c in definition["members"]},
                    amount_share=numerator["amount"] / denominator["amount"]
                    if numerator["amount"] is not None and denominator["amount"]
                    else None,
                )
                group_rows[group] = g
            for c in pool:
                v = pool_values.get(c)
                members[c]["amount_share"] = (
                    v["amount"] / denominator["amount"]
                    if v and v["amount"] is not None and denominator["amount"]
                    else None
                )
            window_results[name] = dict(
                members=members,
                groups=group_rows,
                pool=denominator,
                common_slots=common,
                benchmark=measures.get(benchmark),
                boundary_bar_ids=[
                    r["id"]
                    for r in data
                    if instant(r["start"]) < instant(window["until"])
                    and instant(r["end"]) > instant(window["since"])
                    and not (
                        instant(window["since"]) <= instant(r["start"])
                        and instant(r["end"]) <= instant(window["until"])
                    )
                ],
            )
        changes = []
        for before, after in pairs:
            changes_by_code = {}
            for c in pool:
                a, b = native[before][c], native[after][c]
                common = sorted(set(a) & set(b))
                changes_by_code[c] = dict(
                    **delta(
                        measure([a[s] for s in common]), measure([b[s] for s in common])
                    ),
                    common_slots=common,
                )

            def pair_amounts(participants):
                common = (
                    set.intersection(
                        *(
                            set(native[w][c])
                            for w in (before, after)
                            for c in participants
                        )
                    )
                    if participants
                    else set()
                )
                aggregates = [
                    aggregate(
                        participants,
                        {
                            c: measure([native[w][c][s] for s in sorted(common)])
                            for c in participants
                        },
                    )
                    for w in (before, after)
                ]
                return dict(
                    **delta(*aggregates),
                    common_slots=sorted(common),
                    before=aggregates[0],
                    after=aggregates[1],
                )

            changes.append(
                dict(
                    before=before,
                    after=after,
                    members=changes_by_code,
                    groups={
                        g: pair_amounts(d["members"]) for g, d in definitions.items()
                    },
                    pool=pair_amounts(pool),
                )
            )
        results.append(
            dict(
                source=key[0],
                interval=key[1],
                basis=key[2],
                price_unit=key[3],
                windows=window_results,
                changes=changes,
            )
        )
    return dict(
        pool=pool,
        benchmark=benchmark,
        alignment=alignment,
        local_currency_returns=local_currency,
        comparisons=results,
        excluded=excluded,
        missing_instruments=sorted(set(pool) - {r["instrument"] for r in good}),
        meaning="Group/benchmark/pair align within their participants. Shares require the whole explicit pool at common observed slots; observed amount is not full-window amount or net flow. Calendar coverage unknown; groups may overlap.",
    )


def delta(before, after):
    before, after = before or {}, after or {}
    a, b = before.get("amount"), after.get("amount")
    ra, rb = (
        before.get("observed_return", before.get("mean_return")),
        after.get("observed_return", after.get("mean_return")),
    )
    return dict(
        amount_ratio=b / a - 1
        if a and b is not None and before.get("amount_unit") == after.get("amount_unit")
        else None,
        return_change=rb - ra if ra is not None and rb is not None else None,
        amount_share_change=after["amount_share"] - before["amount_share"]
        if before.get("amount_share") is not None
        and after.get("amount_share") is not None
        else None,
    )
