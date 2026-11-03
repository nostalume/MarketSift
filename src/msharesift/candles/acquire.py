"""Mainline fixed-window history acquisition, with explicit universe policy."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from types import MappingProxyType

from msharesift.candles.frame import _coverage, frame_from_bars
from msharesift.candles.history import DATA_SCHEMA, digest_file
from msharesift.files import atomic_write
from msharesift.model import (
    HOUR,
    MINUTE,
    Series,
    Unavailable,
    admit_product,
    source_cutoff,
)
from msharesift.okx import Budget, BudgetExceeded, PublicClient, fetch_range
from msharesift.products import prepare_catalog, request_catalog, same_contract_rules

SOURCE_GRAINS = {"1m": MINUTE, "5m": 5 * MINUTE, "1h": HOUR}


def write_json(path, value):
    atomic_write(
        Path(path),
        json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False).encode(),
    )


def acquire(
    root,
    inst_ids,
    grain_name,
    start,
    end,
    *,
    max_requests=600,
    seconds=480,
    transport=None,
    universe="alert",
    workers=1,
    session_budget=None,
    catalog=None,
):
    """Fixed explicit instruments/window; resumptions may never change this contract."""
    root = Path(root)
    grain = SOURCE_GRAINS[grain_name]
    if (
        universe not in ("alert", "research")
        or (catalog is not None and universe != "research")
        or workers not in (1, 4)
        or not inst_ids
        or len(inst_ids) != len(set(inst_ids))
        or len(inst_ids) > 100
        or not 0 <= start < end
        or start % grain
        or end % grain
    ):
        raise ValueError("invalid fixed acquisition contract")
    contract = {
        "inst_ids": sorted(inst_ids),
        "grain": grain,
        "start": start,
        "end": end,
        "source": "mainline.PublicClient+fetch_range",
        "universe": "explicit_current_survivors"
        if universe == "alert"
        else "explicit_current_research_products",
    }
    manifest_path = root / "manifest.json"
    new_dataset = not manifest_path.exists()
    if not new_dataset:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if universe == "research" and manifest["contract"].get("universe") == "explicit_current_usdt_crypto_all_bases":
            # Resuming an old immutable dataset does not upgrade its declared scope.
            contract["universe"] = "explicit_current_usdt_crypto_all_bases"
        if manifest.get("schema") != DATA_SCHEMA or manifest["contract"] != contract:
            raise ValueError(
                "resume contract mismatch; use a distinct dataset directory"
            )
    else:
        if root.exists() and any(root.iterdir()):
            raise ValueError("refuse nonempty dataset without a manifest")
        manifest = {
            "schema": DATA_SCHEMA,
            "contract": contract,
            "products": [],
            "attempts": [],
        }
    if session_budget is None:
        budget = Budget(
            max_requests=max_requests, seconds=seconds, reserve=min(60, seconds / 4)
        )
    else:
        from msharesift.candles.session import BatchBudget

        budget = BatchBudget(session_budget, max_requests=max_requests, seconds=seconds)
    prior = {item["product"]["inst_id"]: item for item in manifest["products"]}
    with PublicClient(budget, transport=transport) as client:
        products, rules = {}, {}
        if universe == "research":
            if catalog is None:
                catalog = prepare_catalog(request_catalog(client, inst_ids))
            cutoff = catalog.cutoff
            for name in inst_ids:
                if name not in catalog.products:
                    reason = catalog.rejected.get(name, "not_in_supplied_catalog")
                    raise ValueError(f"requested research product unavailable: {name}: {reason}")
                admitted = catalog.products[name]
                products[name], rules[name] = admitted.product, admitted.rules
                if admitted.rules["expiry_ms"] is not None and end > admitted.rules["expiry_ms"]:
                    raise ValueError("requested window extends beyond product expiry")
        else:
            clock = client.get("/api/v5/public/time")
            cutoff = source_cutoff(clock[0]["ts"])
            for kind in sorted({"SWAP" if name.endswith("-SWAP") else "SPOT" for name in inst_ids}):
                for raw in client.get("/api/v5/public/instruments", {"instType": kind}):
                    if raw.get("instId") in inst_ids:
                        product = admit_product(raw, cutoff)
                        products[product.inst_id] = product
        if end > cutoff:
            raise ValueError("end must precede the completed source cutoff")
        if set(products) != set(inst_ids):
            raise ValueError("requested product unavailable under mainline admission")
        if new_dataset and rules and all(
            rules[name]["asset_class"] == "crypto" and rules[name]["quote"] == "USDT"
            and products[name].inst_type in ("SPOT", "SWAP") for name in inst_ids
        ):
            contract["universe"] = "explicit_current_usdt_crypto_all_bases"
        # Complete metadata validation before creating a new dataset.
        root.mkdir(parents=True, exist_ok=True)
        publication_lock = threading.Lock()
        published = {name: copy.deepcopy(item) for name, item in prior.items()}

        def checkpoint(item):
            # Workers own private product state; only frozen copies reach the single manifest writer.
            with publication_lock:
                published[item["product"]["inst_id"]] = copy.deepcopy(item)
                manifest["products"] = [published[name] for name in sorted(published)]
                write_json(manifest_path, manifest)

        def collect(name, worker_client):
            product = products[name]
            item = copy.deepcopy(prior.get(name))
            if item is None:
                item = {
                    "product": asdict(product),
                    "status": "partial",
                    "shards": [],
                    "errors": [],
                }
                if name in rules:
                    item["rules"] = rules[name]
            elif item["product"] != asdict(product):
                raise ValueError("resume product identity changed")
            elif "rules" in item and not same_contract_rules(item["rules"], rules[name]):
                raise ValueError("resume contract rules changed; use a distinct dataset directory")
            existing = {s["start"]: s for s in item["shards"]}
            item["errors"] = []
            chunk = 30 * 24 * HOUR
            for begin in range(start, end, chunk):
                finish = min(end, begin + chunk)
                old = existing.get(begin)
                if old is not None:
                    if (
                        old["end"] != finish
                        or digest_file(root / old["file"]) != old["sha256"]
                    ):
                        raise Unavailable("resume shard integrity mismatch")
                    if old["complete"]:
                        continue
                if begin < product.normal_since:
                    # Listing coverage is explicit; never fabricate pre-listing candles.
                    item["errors"].append(
                        {"start": begin, "end": finish, "reason": "includes_prelisting"}
                    )
                try:
                    empty = Series(grain, MappingProxyType({}), MappingProxyType({}))
                    series = fetch_range(
                        worker_client,
                        product,
                        empty,
                        begin,
                        cutoff,
                        end=finish,
                        trim=False,
                    )
                    timestamps = sorted(series.bars)
                    complete = not series.errors and timestamps == list(
                        range(begin, finish, grain)
                    )
                    frame = frame_from_bars(series.bars.values()).sort("ts")
                    quality = _coverage(frame, grain, begin, finish)
                    payload = io.BytesIO()
                    frame.write_parquet(payload, compression="zstd")
                    content = payload.getvalue()
                    filename = (
                        hashlib.sha256(
                            f"{name}:{grain}:{begin}:{finish}:".encode() + content
                        ).hexdigest()
                        + ".parquet"
                    )
                    path = root / "shards" / filename
                    path.parent.mkdir(exist_ok=True)
                    atomic_write(path, content)
                    if old is not None:
                        item["shards"].remove(old)
                    item["shards"].append(
                        {
                            "file": f"shards/{filename}",
                            "start": begin,
                            "end": finish,
                            "rows": len(timestamps),
                            "complete": complete,
                            "sha256": hashlib.sha256(content).hexdigest(),
                            "source_cutoff": cutoff,
                            "quality": quality,
                        }
                    )
                    if not complete:
                        item["errors"].append(
                            {
                                "start": begin,
                                "end": finish,
                                "reason": "source_gap_or_invalid",
                                "invalid_rows": len(series.errors),
                            }
                        )
                except (BudgetExceeded, Unavailable, OSError) as exc:
                    item["errors"].append(
                        {"start": begin, "end": finish, "reason": str(exc)}
                    )
                    checkpoint(item)
                    break
                checkpoint(item)
            expected_chunks = (end - start + chunk - 1) // chunk
            item["status"] = (
                "complete"
                if (
                    len(item["shards"]) == expected_chunks
                    and all(s["complete"] for s in item["shards"])
                )
                else "partial"
            )
            checkpoint(item)

        def work(name):
            if workers == 1:
                collect(name, client)
            else:
                # One pool per worker/product, reused for all its pages; one shared Budget.
                with PublicClient(budget, transport=transport) as worker_client:
                    collect(name, worker_client)

        if workers == 1:
            for name in sorted(inst_ids):
                work(name)
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(work, sorted(inst_ids)))
        manifest["attempts"].append(
            {
                "cutoff": cutoff,
                "requests": budget.attempts,
                "retries": budget.retries,
                "bytes": budget.received,
                "max_requests": max_requests,
                "workers": workers,
                "seconds": seconds,
            }
        )
        write_json(manifest_path, manifest)
    return {
        "products": len(manifest["products"]),
        "complete": sum(i["status"] == "complete" for i in manifest["products"]),
        "requests": budget.attempts,
        "manifest": str(manifest_path),
    }
