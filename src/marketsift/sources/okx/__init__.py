"""Public OKX facts, preserving venue/product rules and actual source boundaries."""

import json

from ...time import stamp
from .. import http
from ..common import number, source_read

BASE = "https://www.okx.com"
KINDS = ("SPOT", "SWAP", "FUTURES")
CATEGORIES = {
    "1": "crypto",
    "3": "stocks",
    "4": "commodities",
    "5": "forex",
    "6": "bonds",
}


def get(client, path, params=None, *, method="GET", payload=None):
    response = http.request(
        client,
        method,
        BASE + path,
        params=params,
        **({"json": payload} if payload is not None else {}),
    )
    body = json.loads(response.body)
    if str(body.get("code")) != "0":
        raise ValueError(f"OKX {path}: {body.get('code')} {body.get('msg')}")
    return response, body["data"]


def reference(row, observed):
    kind, native = row["instType"], row["instId"]
    inverse = row.get("ctType") == "inverse"
    base = (
        row.get("baseCcy")
        or (row.get("settleCcy") if inverse else row.get("ctValCcy"))
        or None
    )
    quote = (
        row.get("quoteCcy")
        or (row.get("ctValCcy") if inverse else row.get("settleCcy"))
        or None
    )
    rules = {
        "asset_class": CATEGORIES.get(row.get("instCategory")),
        "inst_category": row.get("instCategory"),
        "base": base,
        "quote": quote,
        "settlement": row.get("settleCcy") or quote,
        "contract_type": row.get("ctType") or None,
        "contract_value": number(row.get("ctVal")),
        "contract_value_currency": row.get("ctValCcy") or None,
        "multiplier": number(row.get("ctMult")),
        "rule_type": row.get("ruleType"),
        "funding": kind == "SWAP" or row.get("ruleType") == "xperp",
        "expiry_ms": row.get("expTime") or None,
        "listed_ms": row.get("listTime") or None,
        "underlying": row.get("uly") or row.get("instFamily"),
        "state": row.get("state"),
        "tick_size": row.get("tickSz"),
        "lot_size": row.get("lotSz"),
        "min_size": row.get("minSz"),
    }
    return dict(
        instrument=f"OKX:{kind}:{native}",
        venue="OKX",
        kind=kind,
        symbol=native,
        name=native,
        aliases=[native],
        source="okx",
        observed_at=observed,
        rules=rules,
        metadata={
            "native": row,
            "rules_scope": "at acquisition; historical contract changes not inferred",
        },
    )


def fetch(request, *, timeout=15, transport=None):
    from . import candles, derivatives

    options = request.get("options", {})
    wanted = list(request.get("instruments", ()))
    datasets = request.get("datasets", ["instruments"])
    if any(d in datasets for d in ("prices", "trades", "series")) and not wanted:
        raise ValueError(
            "OKX market data needs explicit instruments; collect instruments first"
        )
    if any(d in datasets for d in ("prices", "trades")) and not (
        request.get("since") and request.get("until")
    ):
        raise ValueError("OKX prices/trades need since/until")
    if (
        "series" in datasets
        and "funding_history"
        in options.get("metrics", ["funding_history", "open_interest"])
        and not (request.get("since") and request.get("until"))
    ):
        raise ValueError(
            "OKX funding_history needs since/until; use metrics for current snapshots"
        )
    products = {}
    queries = []
    if wanted:
        for identifier in wanted:
            parts = identifier.split(":", 2)
            kinds = (
                [parts[1]]
                if len(parts) == 3 and parts[0] == "OKX"
                else options.get("inst_types", KINDS)
            )
            native = parts[2] if len(parts) == 3 else identifier
            queries.extend((kind, native) for kind in kinds)
    else:
        queries = [(kind, None) for kind in options.get("inst_types", KINDS)]
    with http.open_client(timeout=timeout, transport=transport) as client:
        for kind, native in dict.fromkeys(queries):
            try:
                response, rows = get(
                    client,
                    "/api/v5/public/instruments",
                    {"instType": kind, **({"instId": native} if native else {})},
                )
                references, issues = [], []
                for row in rows:
                    try:
                        if row["instType"] != kind or (
                            native and row["instId"] != native
                        ):
                            raise ValueError(
                                "OKX directory returned a different requested product"
                            )
                        item = reference(row, response.received_at)
                        references.append(item)
                        products[item["instrument"]] = item
                    except (ValueError, KeyError, TypeError) as exc:
                        issues.append(str(exc))
                read = source_read(
                    "okx",
                    response,
                    detail="source-native product directory; unknown/non-live rules retained",
                    request=request,
                    status="partial" if issues else "ok",
                    issues=issues,
                    product_type=kind,
                )
                yield dict(instruments=references, read=read)
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
        for identifier in wanted:
            if not any(
                identifier in (p["instrument"], p["symbol"]) for p in products.values()
            ):
                yield {
                    "read": dict(
                        source="okx",
                        requested_at=stamp(),
                        received_at=stamp(),
                        status="failed",
                        detail=f"product unavailable in returned directories: {identifier}",
                    )
                }
        for product in products.values():
            if "prices" in datasets:
                yield from candles.fetch(client, product, request)
            if "series" in datasets:
                yield from derivatives.fetch(client, product, request)
            if "trades" in datasets:
                yield from derivatives.trades(client, product, request)
