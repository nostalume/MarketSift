"""Frozen all-member, listing-bounded 1h plans for the canonical data owner."""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

import polars as pl

from msharesift.candles.acquire import write_json
from msharesift.candles.history import DATA_SCHEMA, digest_file
from msharesift.model import HOUR, Unavailable, admit_product


def uncovered(start, end, intervals):
    """Complement of trusted coverage; never turn partial shards into coverage."""
    cursor = start
    gaps = []
    for left, right in sorted(intervals):
        left, right = max(start, left), min(end, right)
        if right <= cursor or left >= end:
            continue
        if left > cursor:
            gaps.append((cursor, left))
        cursor = max(cursor, right)
    if cursor < end:
        gaps.append((cursor, end))
    return gaps


def full_history_plan(
    root,
    inventory_root,
    *,
    reuse_roots=(),
    max_requests=80000,
    seconds=21600,
    max_bytes=4 * 1024**3,
    workers=4,
    sample_requests=None,
    sample_seed="20261021-20min",
):
    """No HTTP: every inventory member has a receipt, including unsupported ones.

    Sources own candle cleaning. A complete source shard is reused only after
    hash and declared identity/coverage checks; collection ingestion rechecks
    its frame. Partial shards do not suppress missing-range acquisition.
    """
    from msharesift.candles.session import FullHistoryBudget

    if not (
        0 < max_requests <= FullHistoryBudget.REQUEST_LIMIT
        and 0 < seconds <= FullHistoryBudget.SECOND_LIMIT
        and 0 < max_bytes <= FullHistoryBudget.BYTE_LIMIT
        and workers in (1, 4)
    ):
        raise ValueError("invalid explicit full-history budget")
    root, inventory_root = Path(root), Path(inventory_root).resolve()
    if root.exists():
        raise FileExistsError("preserve frozen plans; use a new root")
    inventory = json.loads(
        (inventory_root / "manifest.json").read_text(encoding="utf-8")
    )
    for name, expected in inventory["files"].items():
        path = (inventory_root / name).resolve()
        if not path.is_relative_to(inventory_root) or digest_file(path) != expected:
            raise Unavailable("inventory_identity_or_hash_changed")
    end = inventory["study_end_ms"]
    cutoff = inventory["catalog_cutoff_ms"]
    if not 0 < end <= cutoff or end % HOUR:
        raise ValueError("invalid inventory cutoff")
    catalog = json.loads((inventory_root / "catalog.json").read_text(encoding="utf-8"))[
        "catalogs"
    ]
    products = {}
    for kind in ("SPOT", "SWAP"):
        for raw in catalog[kind]:
            try:
                product = admit_product(raw, cutoff, excluded_bases=())
            except Unavailable:
                continue
            products[product.inst_id] = product
    census = pl.read_csv(inventory_root / "instruments.csv")
    assert len(products) == inventory["eligible_products"]
    coverage = defaultdict(list)
    reuse = []
    seen = set()
    for source in sorted(Path(p).resolve() for p in reuse_roots):
        if source in seen:
            raise ValueError("duplicate reuse root")
        seen.add(source)
        manifest_path = source / "manifest.json"
        body = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        if body.get("schema") != DATA_SCHEMA or body["contract"]["grain"] != HOUR:
            raise ValueError("only canonical1h sources may be reused")
        used = set()
        for item in body["products"]:
            identity = item["product"]["inst_id"]
            if identity not in products:
                continue
            if item["product"] != vars(products[identity]):
                raise Unavailable("reused_product_identity_changed")
            for shard in item["shards"]:
                if not shard["complete"]:
                    continue
                path = (source / shard["file"]).resolve()
                if (
                    not path.is_relative_to(source)
                    or digest_file(path) != shard["sha256"]
                ):
                    raise Unavailable("reused_shard_hash_changed")
                a, b = shard["start"], shard["end"]
                if a % HOUR or b % HOUR or a >= b or shard["rows"] != (b - a) // HOUR:
                    raise Unavailable("invalid_reused_shard_coverage")
                coverage[identity].append((a, b))
                used.add(identity)
        if used:
            reuse.append(
                {
                    "root": str(source),
                    "manifest_sha256": digest_file(manifest_path),
                    "inst_ids": sorted(used),
                    "coverage_admission": "complete_hashed_shards_only",
                }
            )
    groups = defaultdict(list)
    members = []
    wanted = reused = estimated = 0
    for row in census.to_dicts():
        identity = row["inst_id"]
        member = dict(row)
        if identity not in products:
            member["history_plan_status"] = "unsupported_or_ineligible"
            members.append(member)
            continue
        start = math.ceil(products[identity].normal_since / HOUR) * HOUR
        if start >= end:
            member.update(
                history_plan_status="not_active_at_cutoff", start_ms=start, end_ms=end
            )
            members.append(member)
            continue
        missing = uncovered(start, end, coverage[identity])
        expected = (end - start) // HOUR
        new_rows = sum((b - a) // HOUR for a, b in missing)
        wanted += expected
        reused += expected - new_rows
        member_requests = 0
        for a, b in missing:
            groups[a, b].append(identity)
            # Same30d shards and300-row pages as acquire; boundary pages included.
            chunk = 30 * 24 * HOUR
            calls = sum(
                math.ceil((min(b, t + chunk) - t) / HOUR / 300)
                for t in range(a, b, chunk)
            )
            estimated += calls
            member_requests += calls + 3
        member.update(
            history_plan_status="reuse_complete" if not missing else "fetch_missing",
            start_ms=start,
            end_ms=end,
            expected_rows_upper=expected,
            reusable_rows=expected - new_rows,
            missing_ranges=missing,
            estimated_missing_requests=member_requests,
        )
        members.append(member)
    sample_design = None
    if sample_requests is not None:
        if not 0 < sample_requests <= max_requests or seconds > 1200:
            raise ValueError("capacity sample requires <=20minute explicit budget")
        preferred = {}
        for member in members:
            if member.get("history_plan_status") not in (
                "fetch_missing",
                "reuse_complete",
            ):
                continue
            base = member["base"]
            if base not in preferred or member["inst_type"] == "SPOT":
                preferred[base] = member
        strata = defaultdict(list)
        for base, member in preferred.items():
            days = (end - member["start_ms"]) / (24 * HOUR)
            bucket = 0 if days < 180 else 1 if days < 730 else 2 if days < 1460 else 3
            strata[bucket].append(member)
        worst = {
            k: max(m["estimated_missing_requests"] for m in v)
            for k, v in strata.items()
        }
        minimum = sum(worst.values())
        if minimum > sample_requests:
            raise ValueError(
                "budget cannot fund one probability sample per age stratum"
            )
        weights = [0.05, 0.15, 0.30, 0.50]
        active_weight = sum(weights[k] for k in strata if worst[k])
        quotas = {}
        selected = set()
        design_rows = []
        for k, candidates in sorted(strata.items()):
            quota = (
                (sample_requests - minimum) * weights[k] / active_weight
                if worst[k]
                else 0
            )
            n = (
                min(len(candidates), 1 + int(quota // worst[k]))
                if worst[k]
                else len(candidates)
            )
            quotas[k] = n
            ranked = sorted(
                candidates,
                key=lambda m: hashlib.sha256(
                    (sample_seed + ":" + m["base"]).encode()
                ).hexdigest(),
            )
            chosen = ranked[:n]
            selected.update(m["inst_id"] for m in chosen)
            design_rows.append(
                {
                    "stratum": k,
                    "population": len(candidates),
                    "sample": n,
                    "inclusion_probability": n / len(candidates),
                    "worst_member_requests": worst[k],
                }
            )
        groups = defaultdict(list)
        wanted = reused = estimated = 0
        for member in members:
            chosen = member["inst_id"] in selected
            member["primary_sample_selected"] = chosen
            if not chosen:
                continue
            k = next(k for k, v in strata.items() if member in v)
            member["primary_inclusion_probability"] = quotas[k] / len(strata[k])
            member["sample_age_stratum"] = k
            wanted += member["expected_rows_upper"]
            reused += member["reusable_rows"]
            estimated += member["estimated_missing_requests"]
            for a, b in member["missing_ranges"]:
                groups[a, b].append(member["inst_id"])
        sample_design = {
            "role": "fixed_seed_stratified_base_probability_sample",
            "seed": sample_seed,
            "preferred_product": "SPOT-USDT else linear-USDT-SWAP; one product per base",
            "age_days": [0, 180, 730, 1460, None],
            "resource_weights": weights,
            "strata": design_rows,
            "selected_products": sorted(selected),
            "request_upper_bound": estimated,
            "population_limit": "current-survivor nativeUSDT bases; existing unselected history is development evidence, not extra independent primary samples",
        }
    planned = []
    for (a, b), identities in sorted(groups.items()):
        # A small group keeps each legacy batch bounded; full-history sessions
        # parallelize groups under ONE shared rate/request/byte owner.
        for offset in range(0, len(identities), 4):
            planned.append(
                {
                    "group_id": f"g{len(planned):05d}",
                    "grain": "1h",
                    "start_ms": a,
                    "end_ms": b,
                    "inst_ids": sorted(identities[offset : offset + 4]),
                }
            )
    body = {
        "schema": "canonical-full-history-plan-v1",
        "budget_profile": "full_history",
        "inventory": {
            "root": str(inventory_root),
            "sha256": digest_file(inventory_root / "manifest.json"),
        },
        "source_grain": "1h",
        "aggregation": "complete_UTC_buckets_4h_and1d_downstream",
        "population": "all current nativeUSDT crypto products, no topN; unsupported rows retained in members.json"
        if sample_design is None
        else "capacity-bounded fixed-seed base sample; every census member retained in members.json",
        "historical_membership": "current-survivor catalog, not delisting-complete",
        "budget": {
            "spent_seconds": 0,
            "spent_attempts": 0,
            "total_acquisition_seconds": seconds,
            "global_attempt_cap": max_requests,
            "max_bytes": max_bytes,
            "workers": workers,
        },
        "remaining_groups": planned,
        "reuse": reuse,
        "sample_design": sample_design,
        "capacity": {
            "products": len(products),
            "bases": len({p.base for p in products.values()}),
            "groups": len(planned),
            "expected_rows_upper": wanted,
            "reused_rows": reused,
            "missing_rows_upper": wanted - reused,
            "estimated_requests_including_metadata": estimated
            if sample_design
            else estimated + 3 * len(planned),
            "selected_primary_bases": len(sample_design["selected_products"])
            if sample_design
            else len({p.base for p in products.values()}),
        },
    }
    root.mkdir(parents=True)
    write_json(root / "plan.json", body)
    write_json(root / "members.json", members)
    return dict(
        plan=str(root / "plan.json"),
        members=str(root / "members.json"),
        **body["capacity"],
    )
