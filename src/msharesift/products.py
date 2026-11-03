"""OKX research identities and contract rules; legacy alert admission is unchanged."""

from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path

from msharesift.files import atomic_write
from msharesift.model import Product, Unavailable, _integer, _number, source_cutoff
from msharesift.okx import Budget, BudgetExceeded, PublicClient

KINDS = ("SPOT", "SWAP", "FUTURES")
CATEGORIES = {"1": "crypto", "3": "stocks", "4": "commodities",
              "5": "forex", "6": "bonds"}


@dataclass(frozen=True)
class ResearchProduct:
    product: Product
    rules: dict


@dataclass(frozen=True)
class ResearchCatalog:
    cutoff: int
    products: dict[str, ResearchProduct]
    rejected: dict[str, str]
    counts: dict[str, int]
    failures: dict[str, str]


def admit_research_product(raw, at):
    """Interpret source metadata once, independently of the old alert population."""
    kind, name = raw.get("instType"), raw.get("instId")
    category, rule = raw.get("instCategory"), raw.get("ruleType")
    if category not in CATEGORIES:
        raise Unavailable("research_unknown_asset_category")
    if kind not in KINDS or raw.get("state") != "live":
        raise Unavailable("research_product_not_live_or_supported_type")
    if rule != "normal" and not (kind == "FUTURES" and rule == "xperp"):
        raise Unavailable("research_trading_rule_not_supported")
    if not isinstance(name, str) or not name:
        raise Unavailable("metadata_unavailable:instId")
    times = [_integer(raw.get("listTime"))]
    times.extend(_integer(raw[field]) for field in ("contTdSwTime", "preMktSwTime")
                 if raw.get(field) not in (None, ""))
    if times[0] <= 0 or any(t < 0 or t > at for t in times):
        raise Unavailable("metadata_unavailable:normal_start")
    expiry = _integer(raw["expTime"]) if raw.get("expTime") not in (None, "") else None
    if (kind == "FUTURES" and expiry is None) or (expiry is not None and expiry <= at):
        raise Unavailable("research_expiry_unavailable_or_elapsed")
    base = raw.get("baseCcy") if kind == "SPOT" else name.split("-", 1)[0]
    if not isinstance(base, str) or not base:
        raise Unavailable("metadata_unavailable:base")
    contract_type, value, multiplier = None, 1.0, 1.0
    if kind == "SPOT":
        quote = raw.get("quoteCcy")
        if not isinstance(quote, str) or not quote or name != f"{base}-{quote}":
            raise Unavailable("metadata_unavailable:spot_identity")
        settlement = quote
    else:
        contract_type = raw.get("ctType")
        settlement = raw.get("settleCcy")
        value = _number(raw.get("ctVal"))
        multiplier = _number(raw["ctMult"]) if raw.get("ctMult") not in (None, "") else None
        if value <= 0 or (multiplier is not None and multiplier <= 0):
            raise Unavailable("metadata_unavailable:contract_value")
        if contract_type == "linear" and raw.get("ctValCcy") == base:
            quote = settlement
        elif contract_type == "inverse" and raw.get("ctValCcy") == "USD" and settlement == base:
            quote = "USD"
        else:
            raise Unavailable("research_contract_units_not_supported")
        if not isinstance(settlement, str) or not settlement:
            raise Unavailable("metadata_unavailable:settlement")
    optional_sizes = {}
    for source, target in (("tickSz", "tick_size"), ("lotSz", "lot_size"), ("minSz", "min_size")):
        value_size = _number(raw[source]) if raw.get(source) not in (None, "") else None
        if value_size is not None and value_size <= 0:
            raise Unavailable(f"metadata_unavailable:{source}")
        optional_sizes[target] = value_size
    rules = {"asset_class": CATEGORIES[category], "inst_category": category,
             "quote": quote, "settlement": settlement, "contract_type": contract_type,
             "contract_value": value, "multiplier": multiplier, "rule_type": rule,
             "expiry_ms": expiry, "funding": kind == "SWAP" or rule == "xperp",
             "future_settlement": raw.get("futureSettlement"),
             "index_id": raw.get("uly") or raw.get("instFamily") or None,
             "observed_at_ms": at,
             **optional_sizes}
    return ResearchProduct(Product(kind, name, base, max(times)), rules)


def same_contract_rules(left, right):
    """Joining/resuming needs equal contract meaning, not equal retrieval dates."""
    return all(left.get(key) == right.get(key)
               for key in left.keys() | right.keys() if key != "observed_at_ms")


def request_catalog(client, inst_ids=()):
    """One request owner; successful kinds survive a failed public directory."""
    cutoff = source_cutoff(client.get("/api/v5/public/time")[0]["ts"])
    catalogs, failures = {}, {}
    # A suffix is only a cheap lookup hint, never a product type/admission rule.
    # Unknown IDs fall through to every other real directory before rejection.
    hints = {"SWAP" if name.endswith("-SWAP") else "SPOT" for name in inst_ids}
    order = sorted(KINDS, key=lambda kind: kind not in hints)
    found = set()
    for kind in order:
        try:
            catalogs[kind] = client.get("/api/v5/public/instruments", {"instType": kind})
            found.update(r.get("instId") for r in catalogs[kind] if r.get("instType") == kind)
        except (Unavailable, BudgetExceeded) as exc:
            failures[kind] = str(exc)
        if inst_ids and found.issuperset(inst_ids):
            break
    return {"schema": "okx-candle-universe-v1", "cutoff_ms": cutoff,
            "catalogs": catalogs, "failures": failures}


def catalog_snapshot(root, *, max_requests=12, seconds=90, transport=None):
    """Reuse an existing snapshot, including failures; a new root requests a new view."""
    root = Path(root)
    path = root / "catalog.json"
    if path.exists():
        return json.loads(path.read_bytes())
    budget = Budget(max_requests=max_requests, seconds=seconds, reserve=min(5, seconds/4))
    with PublicClient(budget, transport=transport) as client:
        pack = request_catalog(client)
    pack["network"] = {"requests": budget.attempts, "bytes": budget.received}
    root.mkdir(parents=True, exist_ok=True)
    atomic_write(path, json.dumps(pack, ensure_ascii=False, allow_nan=False).encode())
    return pack


def prepare_catalog(pack):
    """Admit the directory once; pass this result to summary and acquisition."""
    products, rejected, counts = {}, {}, {}
    for kind, rows in pack["catalogs"].items():
        counts[kind] = len(rows)
        for index, raw in enumerate(rows):
            name = raw.get("instId") or f"{kind}:row:{index}"
            if name in products or name in rejected:
                raise Unavailable("research_duplicate_catalog_identity")
            try:
                if raw.get("instType") != kind:
                    raise Unavailable("research_catalog_type_mismatch")
                products[name] = admit_research_product(raw, pack["cutoff_ms"])
            except Unavailable as exc:
                rejected[name] = str(exc)
    failures = dict(pack.get("failures", {}))
    return ResearchCatalog(pack["cutoff_ms"], products, rejected, counts, failures)


def summarize_catalog(catalog, inst_ids=()):
    result = {"cutoff_ms": catalog.cutoff, "source_counts": catalog.counts,
              "supported": len(catalog.products), "rejected": len(catalog.rejected),
              "asset_classes": dict(Counter(v.rules["asset_class"] for v in catalog.products.values())),
              "mechanisms": dict(Counter(v.product.inst_type+":"+v.rules["rule_type"] for v in catalog.products.values())),
              "rejection_reasons": dict(Counter(catalog.rejected.values())),
              "failures": catalog.failures, "status": "partial" if catalog.failures else "complete"}
    result["unqueried_types"] = sorted(set(KINDS)-catalog.counts.keys()-catalog.failures.keys())
    if inst_ids:
        result["selected"] = {name: {"product": asdict(catalog.products[name].product),
                                     "rules": catalog.products[name].rules}
                              if name in catalog.products else {"unavailable": catalog.rejected.get(name, "not_in_supplied_catalog")}
                              for name in inst_ids}
    return result


def position_scenario(product, rules, quantity, entry, exit_price, *, side="long",
                      fee_rate=None, funding=None, exit_at=None):
    """Constant quantity, explicit price/fee/funding assumptions; no account or leverage model.

    Funding is an explicit sequence of (decimal realized/assumed rate, settlement
    mark). None means unknown for funding-bearing contracts, not a zero cost.
    PnL follows OKX face-value x size x multiplier formulas; no USD/USDT conversion.
    """
    if side not in ("long", "short") or any(not math.isfinite(v) or v <= 0 for v in (quantity, entry, exit_price)):
        raise ValueError("positive finite quantity/prices and long/short side required")
    if rules["expiry_ms"] is not None and exit_at is not None and exit_at > rules["expiry_ms"]:
        raise ValueError("scenario extends beyond contract expiry; rollover is a separate position")
    if product.inst_type == "SPOT" and side == "short":
        raise ValueError("spot short requires a separate borrowing model")
    if rules["multiplier"] is None:
        raise Unavailable("research_contract_multiplier_unavailable")
    amount = quantity * rules["contract_value"] * rules["multiplier"]
    if not math.isfinite(amount):
        raise ValueError("position amount overflow")
    sign = 1 if side == "long" else -1
    inverse = rules["contract_type"] == "inverse"
    gross = sign * amount * (1/entry - 1/exit_price) if inverse else sign * amount * (exit_price-entry)
    entry_value = amount/entry if inverse else amount*entry
    exit_value = amount/exit_price if inverse else amount*exit_price
    if fee_rate is not None and not math.isfinite(fee_rate):
        raise ValueError("finite explicit fee rate required")
    fees = None if fee_rate is None else fee_rate * (entry_value+exit_value)
    funding_cost = None if rules["funding"] else 0.0
    if funding is not None:
        if not rules["funding"] and funding:
            raise ValueError("funding is not applicable to this product")
        funding_cost = 0.0
        for rate, mark in funding:
            if not math.isfinite(rate) or not math.isfinite(mark) or mark <= 0:
                raise ValueError("finite funding rate and positive mark required")
            funding_cost += sign * (amount/mark if inverse else amount*mark) * rate
    return {"inst_id": product.inst_id, "side": side, "quantity": quantity,
            "quantity_unit": product.base if product.inst_type == "SPOT" else "contracts",
            "entry": entry, "exit": exit_price, "gross_pnl": gross,
            "fees": fees, "funding": funding_cost,
            "net_pnl": None if fees is None or funding_cost is None else gross-fees-funding_cost,
            "pnl_ccy": rules["settlement"], "entry_notional": amount if inverse else entry_value,
            "notional_ccy": rules["quote"], "expiry_ms": rules["expiry_ms"],
            "fee_rate_assumption": fee_rate,
            "funding_events_assumption": None if funding is None else len(funding)}
