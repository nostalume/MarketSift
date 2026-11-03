"""Mainline, unranked instrument census and bounded history availability probes.

Inventory is a current catalog, never a historical point-in-time universe. Public
GETs reuse the existing client/budget. No liquidity/top-N selection is applied.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from msharesift.files import atomic_write
from msharesift.model import (
    FIVE,
    HOUR,
    Series,
    Unavailable,
    admit_product,
    source_cutoff,
)
from msharesift.okx import Budget, BudgetExceeded, PublicClient

SCHEMA = "okx-candle-universe-v1"
DAY = 24 * HOUR
KINDS = ("SPOT", "SWAP")
SCENARIOS = (
    90,
    365,
    1095,
    1825,
    0,
)  # 0 = all since normal listing, metadata upper bound


def write_json(path, body):
    atomic_write(
        Path(path),
        json.dumps(body, ensure_ascii=False, indent=2, allow_nan=False).encode(),
    )


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def census_rows(catalogs, cutoff):
    rows = []
    seen = set()
    for kind in KINDS:
        for raw in catalogs[kind]:
            if not isinstance(raw, dict) or not isinstance(raw.get("instId"), str):
                raise Unavailable("invalid_catalog_identity")
            identity = (kind, raw["instId"])
            if identity in seen:
                raise Unavailable("duplicate_catalog_identity")
            seen.add(identity)
            if raw.get("instType") != kind:
                raise Unavailable("catalog_type_mismatch")
            candidate = (
                raw.get("instCategory") == "1"
                and raw.get("state") == "live"
                and raw.get("ruleType") == "normal"
            )
            candidate_start = None
            if candidate:
                try:
                    names = ("listTime", "contTdSwTime", "preMktSwTime")
                    times = [
                        int(raw[name])
                        for name in names
                        if raw.get(name) not in (None, "")
                    ]
                    if (
                        raw.get("listTime") in (None, "")
                        or not times
                        or times[0] <= 0
                        or any(t < 0 or t > cutoff for t in times)
                    ):
                        raise ValueError("unavailable lifecycle")
                    candidate_start = max(times)
                except (TypeError, ValueError, OverflowError):
                    candidate = False
            candidate_base = (
                raw.get("baseCcy", "")
                if kind == "SPOT"
                else raw.get("instFamily", "").rsplit("-", 1)[0]
            )
            product, reason = None, None
            try:
                product = admit_product(raw, cutoff, excluded_bases=())
            except Unavailable as exc:
                reason = str(exc)
            alert_reason = None
            try:
                admit_product(raw, cutoff)
            except Unavailable as exc:
                alert_reason = str(exc)
            rows.append(
                {
                    "inst_type": kind,
                    "inst_id": raw["instId"],
                    "base": product.base
                    if product
                    else (raw.get("baseCcy") or raw.get("ctValCcy") or ""),
                    "state": raw.get("state", ""),
                    "category": raw.get("instCategory", ""),
                    "quote": raw.get("quoteCcy", ""),
                    "settlement": raw.get("settleCcy", ""),
                    "list_time_raw": raw.get("listTime", ""),
                    "normal_since_ms": product.normal_since if product else None,
                    "candidate_crypto": candidate,
                    "candidate_base": candidate_base,
                    "candidate_normal_since_ms": candidate_start,
                    "adapter_status": "native_usdt"
                    if product
                    else (
                        "quote_or_contract_adapter_required"
                        if candidate
                        else "outside_current_crypto_population"
                    ),
                    "research_eligible": product is not None,
                    "research_reason": reason or "eligible_current_usdt_crypto",
                    "alert_eligible": alert_reason is None,
                    "alert_reason": alert_reason or "eligible",
                }
            )
    if not rows:
        raise Unavailable("empty_catalog")
    return pl.DataFrame(
        rows,
        schema_overrides={
            "normal_since_ms": pl.Int64,
            "candidate_normal_since_ms": pl.Int64,
        },
    ).sort("inst_type", "inst_id")


def capacity_rows(census, end, *, page_rows=300, all_quotes=False):
    """Listing-clipped upper bounds, not observed history completeness."""
    if not isinstance(page_rows, int) or not 1 <= page_rows <= 300:
        raise ValueError("page_rows must be 1..300")
    if all_quotes:
        census = census.with_columns(
            pl.col("candidate_crypto").alias("research_eligible"),
            pl.col("candidate_base").alias("base"),
            pl.col("candidate_normal_since_ms").alias("normal_since_ms"),
        )
    records = []
    for row in census.filter(pl.col("research_eligible")).to_dicts():
        for days in SCENARIOS:
            start = max(row["normal_since_ms"], end - days * DAY if days else 0)
            for name, grain in [("5m", FIVE), ("1h", HOUR)]:
                aligned = (start + grain - 1) // grain * grain
                count = max(0, (end // grain * grain - aligned) // grain)
                chunk = 30 * DAY // grain
                full, remainder = divmod(count, chunk)
                calls = full * math.ceil(chunk / page_rows) + math.ceil(
                    remainder / page_rows
                )
                records.append(
                    {
                        "inst_type": row["inst_type"],
                        "inst_id": row["inst_id"],
                        "base": row["base"],
                        "scenario_days": days,
                        "grain": name,
                        "start_ms": aligned,
                        "end_ms": end // grain * grain,
                        "expected_rows_upper": count,
                        "active_days_upper": count * grain / DAY,
                        "page_rows_assumption": page_rows,
                        "data_requests_estimate": calls,
                        "shards_estimate": math.ceil(count / chunk),
                        "history_status": "listing_bound_unverified",
                        "population": "all_quotes_crypto"
                        if all_quotes
                        else "native_usdt_crypto",
                    }
                )
    return pl.DataFrame(records).sort("scenario_days", "grain", "inst_type", "inst_id")


def inventory(root, *, end=None, max_requests=20, seconds=90, transport=None):
    root = Path(root)
    if root.exists():
        raise FileExistsError("Preserve universe snapshots; use a new directory")
    budget = Budget(
        max_requests=max_requests, seconds=seconds, reserve=min(10, seconds / 4)
    )
    started = time.monotonic()
    with PublicClient(budget, transport=transport) as client:
        clock = client.get("/api/v5/public/time")
        cutoff = source_cutoff(clock[0]["ts"])
        if end is None:
            end = cutoff // HOUR * HOUR
        if not isinstance(end, int) or not 0 < end <= cutoff or end % HOUR:
            raise ValueError("end must be completed, hourly aligned UTC")
        catalogs = {
            kind: client.get("/api/v5/public/instruments", {"instType": kind})
            for kind in KINDS
        }
    census = census_rows(catalogs, cutoff)
    if not census.filter(pl.col("research_eligible")).height:
        raise Unavailable("no_eligible_research_catalog")
    root.mkdir(parents=True)
    write_json(
        root / "catalog.json",
        {"schema": SCHEMA, "cutoff_ms": cutoff, "catalogs": catalogs},
    )
    census.write_parquet(root / "instruments.parquet", compression="zstd")
    census.write_csv(root / "instruments.csv")
    capacities = capacity_rows(census, end)
    capacities.write_parquet(root / "capacity.parquet", compression="zstd")
    capacities.write_csv(root / "capacity.csv")
    all_capacity = capacity_rows(census, end, all_quotes=True)
    all_capacity.write_parquet(root / "capacity_all_quotes.parquet", compression="zstd")
    all_capacity.write_csv(root / "capacity_all_quotes.csv")
    summary = {
        "schema": SCHEMA,
        "catalog_cutoff_ms": cutoff,
        "study_end_ms": end,
        "population": "all current normal crypto SPOT and SWAP; native USDT subset separately marked; no top-N exclusion",
        "historical_membership": "unverified; delisted products absent from current catalog",
        "counts": census.group_by("inst_type", "research_eligible", "research_reason")
        .len()
        .sort("inst_type", "research_reason")
        .to_dicts(),
        "eligible_products": census.filter(pl.col("research_eligible")).height,
        "eligible_bases": census.filter(pl.col("research_eligible"))["base"].n_unique(),
        "all_crypto_products": census.filter(pl.col("candidate_crypto")).height,
        "all_crypto_bases": census.filter(pl.col("candidate_crypto"))[
            "candidate_base"
        ].n_unique(),
        "additional_vs_alert": census.filter(
            pl.col("research_eligible") & ~pl.col("alert_eligible")
        ).height,
        "requests": budget.attempts,
        "received_bytes": budget.received,
        "seconds": time.monotonic() - started,
        "observations": budget.observations,
        "files": {
            name: sha(root / name)
            for name in [
                "catalog.json",
                "instruments.parquet",
                "instruments.csv",
                "capacity.parquet",
                "capacity.csv",
                "capacity_all_quotes.parquet",
                "capacity_all_quotes.csv",
            ]
        },
    }
    write_json(root / "manifest.json", summary)
    return summary


def probe(
    root,
    inventory_root,
    *,
    days=1825,
    max_requests=1400,
    seconds=480,
    workers=4,
    transport=None,
):
    """Two boundary pages per member, sharing one rate/request/byte budget.

    Each worker owns its connection pool. Boundary presence is not intervening
    completeness; failure cannot remove a member from the inventory.
    """
    root, inventory_root = Path(root), Path(inventory_root)
    if root.exists():
        raise FileExistsError("Preserve availability probe attempts")
    if not isinstance(days, int) or days <= 0 or workers not in (1, 2, 4):
        raise ValueError("positive days and workers 1/2/4 required")
    manifest = json.loads(
        (inventory_root / "manifest.json").read_text(encoding="utf-8")
    )
    for name, expected in manifest["files"].items():
        if sha(inventory_root / name) != expected:
            raise Unavailable("inventory_integrity_mismatch")
    census = pl.read_parquet(inventory_root / "instruments.parquet").filter(
        pl.col("research_eligible")
    )
    end = manifest["study_end_ms"]
    budget = Budget(
        max_requests=max_requests, seconds=seconds, reserve=min(20, seconds / 4)
    )
    root.mkdir(parents=True)
    contract = {
        "schema": SCHEMA,
        "inventory_manifest": str((inventory_root / "manifest.json").resolve()),
        "inventory_sha256": sha(inventory_root / "manifest.json"),
        "study_end_ms": end,
        "scenario_days": days,
        "products": census.height,
        "expected_pages": 2 * census.height,
        "workers": workers,
    }
    write_json(root / "probe-contract.json", contract)
    stop = threading.Event()
    local = threading.local()
    clients = []
    clients_lock = threading.Lock()

    def initialize():
        local.client = PublicClient(budget, transport=transport)
        with clients_lock:
            clients.append(local.client)

    def request(job):
        product, role = job
        begin = (
            (max(product["normal_since_ms"], end - days * DAY) + FIVE - 1)
            // FIVE
            * FIVE
        )
        cursor = min(end, begin + 100 * FIVE) if role == "start" else end
        record = {
            "inst_type": product["inst_type"],
            "inst_id": product["inst_id"],
            "role": role,
            "start_ms": begin,
            "end_ms": end,
            "rows": None,
            "first_ms": None,
            "last_ms": None,
            "status": "not_probed_budget" if stop.is_set() else "pending",
            "exact_boundary": False,
            "request_seconds": None,
            "request_bytes": None,
        }
        evidence = None
        if begin >= end:
            record["status"] = "not_listed_in_window"
        elif not stop.is_set():
            started = time.monotonic()
            try:
                page = local.client.get(
                    "/api/v5/market/history-candles",
                    {
                        "instId": product["inst_id"],
                        "bar": "5m",
                        "after": str(cursor),
                        "limit": "100",
                    },
                )
                series = Series.parse(page, FIVE)
                if any(int(row[0]) >= cursor for row in page):
                    raise Unavailable("probe_pagination_not_backward")
                times = sorted(ts for ts in series.bars if begin <= ts < end)
                record.update(
                    rows=len(times),
                    first_ms=times[0] if times else None,
                    last_ms=times[-1] if times else None,
                    status="invalid_rows"
                    if series.errors
                    else ("available_page" if times else "no_page_in_window"),
                    exact_boundary=(begin if role == "start" else end - FIVE) in times,
                )
                evidence = {
                    "inst_id": product["inst_id"],
                    "role": role,
                    "cursor_ms": cursor,
                    "rows": page,
                }
                # Logical payload size belongs to this page; actual wire bytes are budget-owned.
                record["request_bytes"] = len(
                    json.dumps(page, separators=(",", ":")).encode()
                )
            except BudgetExceeded as exc:
                record["status"] = "budget:" + str(exc)
                stop.set()
            except (Unavailable, OSError) as exc:
                record["status"] = "unknown:" + str(exc)
            record["request_seconds"] = time.monotonic() - started
        elif record["status"] == "pending":
            record["status"] = "not_probed_budget"
        return record, evidence

    started = time.monotonic()
    records = []
    progress_warnings = 0
    jobs = [(p, role) for p in census.to_dicts() for role in ("start", "end")]
    try:
        with (
            ThreadPoolExecutor(max_workers=workers, initializer=initialize) as pool,
            (root / "pages.jsonl").open("w", encoding="utf-8") as raw_file,
        ):
            for record, evidence in pool.map(request, jobs):
                records.append(record)
                if evidence is not None:
                    raw_file.write(json.dumps(evidence) + "\n")
                    raw_file.flush()
                if len(records) % 20 == 0 or len(records) == len(jobs):
                    try:
                        write_json(
                            root / "progress.json",
                            {
                                "attempted_pages": len(records),
                                "total_pages": len(jobs),
                                "requests": budget.attempts,
                                "budget_stopped": stop.is_set(),
                            },
                        )
                    except PermissionError:
                        # Windows readers can temporarily deny replacement of this optional view.
                        progress_warnings += 1
    finally:
        for client in clients:
            client.__exit__(None, None, None)
    frame = pl.DataFrame(
        records,
        schema_overrides={
            "rows": pl.Int64,
            "first_ms": pl.Int64,
            "last_ms": pl.Int64,
            "request_seconds": pl.Float64,
            "request_bytes": pl.Int64,
        },
    )
    frame.write_parquet(root / "availability.parquet", compression="zstd")
    frame.write_csv(root / "availability.csv")
    summary = contract | {
        "status_counts": frame.group_by("role", "status")
        .len()
        .sort("role", "status")
        .to_dicts(),
        "requests": budget.attempts,
        "retries": budget.retries,
        "received_bytes": budget.received,
        "seconds": time.monotonic() - started,
        "max_requests": max_requests,
        "max_seconds": seconds,
        "budget_stopped": stop.is_set(),
        "progress_write_warnings": progress_warnings,
        "observations": budget.observations,
        "probe_complete": all(
            r["status"]
            in ("available_page", "no_page_in_window", "not_listed_in_window")
            for r in records
        ),
        "meaning": "boundary page availability only; gaps, earliest retained history and delisted universe unverified",
        "files": {
            name: sha(root / name)
            for name in [
                "pages.jsonl",
                "availability.parquet",
                "availability.csv",
                "probe-contract.json",
            ]
        },
    }
    write_json(root / "manifest.json", summary)
    return summary
