"""Native trades, funding events and current snapshots; no fabricated OI history."""

from ...time import stamp
from .. import http
from ..common import from_ms, milliseconds, number, source_read
from . import get


def scalar(product, metric, unit, response, ts, value, **metadata):
    key = f"okx:{product['symbol']}:{metric}"
    definition = dict(
        series=key,
        source="okx",
        native_id=metric,
        instrument=product["instrument"],
        name=f"{product['symbol']} {metric}",
        unit=unit,
        frequency="source event/snapshot",
        measure=metric,
        aggregation="point; not a time sum",
        metadata={"source_field": metric},
    )
    point = dict(
        series=key,
        period_kind="instant",
        period_start=from_ms(ts),
        period_end=None,
        value=number(value),
        observed_at=response.received_at,
        metadata=metadata,
    )
    return definition, point


def history(client, product, request, *, funding):
    path = (
        "/api/v5/public/funding-rate-history"
        if funding
        else "/api/v5/market/history-trades"
    )
    if not request.get("since") or not request.get("until"):
        raise ValueError("OKX event history needs since/until")
    since, until = milliseconds(request["since"]), milliseconds(request["until"])
    cursor = until
    while True:
        params = dict(instId=product["symbol"], after=str(cursor), limit="100")
        if not funding:
            params["type"] = "2"
        try:
            response, raw = get(client, path, params)
            definitions, values, ticks, times, issues = {}, [], [], [], []
            for row in raw:
                try:
                    ts = int(row["fundingTime" if funding else "ts"])
                    times.append(ts)
                    if not since <= ts < until:
                        continue
                    if funding:
                        for field, metric in (
                            ("fundingRate", "funding_rate"),
                            ("realizedRate", "realized_funding_rate"),
                        ):
                            if field not in row:
                                continue
                            definition, point = scalar(
                                product,
                                metric,
                                "fraction per source settlement event",
                                response,
                                ts,
                                row[field],
                                formula_type=row.get("formulaType"),
                                method=row.get("method"),
                            )
                            definitions[definition["series"]] = definition
                            values.append(point)
                    else:
                        ticks.append(
                            dict(
                                source="okx",
                                instrument=product["instrument"],
                                native_id=str(row["tradeId"]),
                                ts=from_ms(ts),
                                price=number(row["px"]),
                                size=number(row["sz"]),
                                side=row.get("side"),
                                units={
                                    "price": product["rules"].get("quote"),
                                    "size": product["rules"].get("base")
                                    if product["kind"] == "SPOT"
                                    else "contracts",
                                },
                                observed_at=response.received_at,
                                metadata={"native_source": row.get("source")},
                            )
                        )
                except (ValueError, KeyError, TypeError) as exc:
                    issues.append(str(exc))
            oldest = min(times) if times else None
            reached = oldest is not None and oldest <= since
            stopped = not raw or oldest is None or oldest >= cursor or reached
            detail = (
                "requested boundary reached"
                if reached
                else "source exhausted/stalled"
                if stopped
                else "next native page"
            )
            yield dict(
                series=list(definitions.values()),
                series_values=values,
                trades=ticks,
                read=source_read(
                    "okx",
                    response,
                    request=request,
                    status="partial" if issues or (stopped and not reached) else "ok",
                    detail=detail,
                    issues=issues,
                    first_time=from_ms(oldest) if oldest is not None else None,
                    last_time=from_ms(max(times)) if times else None,
                ),
            )
            if stopped:
                return
            cursor = oldest
        except http.FetchError as exc:
            yield {"read": dict(source="okx", **exc.read)}
            return
        except (ValueError, KeyError, TypeError) as exc:
            yield {
                "read": dict(
                    source="okx",
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="failed",
                    detail=str(exc),
                )
            }
            return


def trades(client, product, request):
    if request.get("options", {}).get("archive") or request.get("options", {}).get(
        "archive_urls"
    ):
        from . import archives

        yield from archives.fetch(client, product, request)
    else:
        yield from history(client, product, request, funding=False)


def fetch(client, product, request):
    metrics = request.get("options", {}).get(
        "metrics", ["funding_history", "open_interest"]
    )
    for metric in metrics:
        if metric.startswith("funding") and not product["rules"]["funding"]:
            yield {
                "read": dict(
                    source="okx",
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="ok",
                    detail=f"{metric} not applicable",
                    instruments=[product["instrument"]],
                )
            }
            continue
        if metric == "funding_history":
            if request.get("options", {}).get("archive") or request.get(
                "options", {}
            ).get("archive_urls"):
                from . import archives

                yield from archives.fetch(client, product, request, funding=True)
            else:
                yield from history(client, product, request, funding=True)
            continue
        if metric in {"open_interest", "mark_price"} and product["kind"] == "SPOT":
            yield {
                "read": dict(
                    source="okx",
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="ok",
                    detail=f"{metric} not applicable to spot",
                )
            }
            continue
        kind, native, rules = product["kind"], product["symbol"], product["rules"]
        paths = {
            "open_interest": (
                "/api/v5/public/open-interest",
                {"instType": kind, "instId": native},
            ),
            "funding_current": ("/api/v5/public/funding-rate", {"instId": native}),
            "ticker": ("/api/v5/market/ticker", {"instId": native}),
            "mark_price": (
                "/api/v5/public/mark-price",
                {"instType": kind, "instId": native},
            ),
            "index_price": (
                "/api/v5/market/index-tickers",
                {"instId": rules.get("underlying") or native},
            ),
        }
        if metric not in paths:
            raise ValueError(f"unknown OKX metric {metric}")
        try:
            response, raw = get(client, *paths[metric])
            definitions, values = [], []
            for row in raw:
                ts = row.get("ts") or milliseconds(response.received_at)
                if metric == "open_interest":
                    fields = [
                        ("oi", "open_interest_contracts", "contracts"),
                        ("oiCcy", "open_interest_ccy", rules.get("base")),
                        ("oiUsd", "open_interest_usd", "USD"),
                    ]
                elif metric == "funding_current":
                    fields = [
                        (
                            "fundingRate",
                            "estimated_funding_rate",
                            "fraction per source event",
                        ),
                        (
                            "nextFundingRate",
                            "next_estimated_funding_rate",
                            "fraction per source event",
                        ),
                    ]
                elif metric == "ticker":
                    fields = [
                        (k, k, rules.get("quote"))
                        for k in (
                            "last",
                            "bidPx",
                            "askPx",
                            "open24h",
                            "high24h",
                            "low24h",
                            "sodUtc0",
                            "sodUtc8",
                        )
                    ]
                    fields += [
                        (
                            "vol24h",
                            "volume_24h",
                            rules.get("base") if kind == "SPOT" else "contracts",
                        ),
                        (
                            "volCcy24h",
                            "volume_ccy_24h",
                            rules.get("quote") if kind == "SPOT" else rules.get("base"),
                        ),
                    ]
                else:
                    fields = [
                        (
                            "markPx" if metric == "mark_price" else "idxPx",
                            metric,
                            rules.get("quote"),
                        )
                    ]
                for field, name, unit in fields:
                    if row.get(field) in (None, ""):
                        continue
                    point_time = (
                        row.get(
                            "nextFundingTime"
                            if field == "nextFundingRate"
                            else "fundingTime",
                            ts,
                        )
                        if metric == "funding_current"
                        else ts
                    )
                    definition, point = scalar(
                        product,
                        name,
                        unit,
                        response,
                        point_time,
                        row[field],
                        history_supported=False,
                        native_funding_time=row.get("fundingTime"),
                        next_funding_time=row.get("nextFundingTime"),
                        native_interval=row.get("fundingInterval"),
                        time_basis="source funding period"
                        if metric == "funding_current"
                        else "source timestamp"
                        if row.get("ts")
                        else "acquisition timestamp; native event time unavailable",
                    )
                    definitions.append(definition)
                    values.append(point)
            yield dict(
                series=definitions,
                series_values=values,
                read=source_read(
                    "okx",
                    response,
                    detail=f"{metric}: current snapshot only; does not backfill requested history",
                    instruments=[product["instrument"]],
                    metric=metric,
                    history_supported=False,
                ),
            )
        except http.FetchError as exc:
            yield {"read": dict(source="okx", **exc.read)}
        except (ValueError, KeyError, TypeError) as exc:
            yield {
                "read": dict(
                    source="okx",
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="failed",
                    detail=str(exc),
                )
            }
