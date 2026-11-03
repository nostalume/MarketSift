"""Compressed candle snapshot format used by the canonical dataset importer."""
from __future__ import annotations

import gzip
import hashlib
import io
import json
from dataclasses import asdict
from pathlib import Path

from msharesift.files import atomic_write
from msharesift.model import FIVE, Product, Series, Unavailable
from msharesift.okx import rows_of

SCHEMA = "okx-alert-canonical-research-v1"

def _product_name(product: Product) -> str:
    digest = hashlib.sha256(json.dumps(product.key, separators=(",", ":")).encode()).hexdigest()
    return digest + ".json.gz"

def _save_snapshot(root: Path, product: Product, start: int, end: int,
                   series: Series, *, at: int, observations: list[dict]) -> dict:
    expected = (end - start) // FIVE
    timestamps = sorted(series.bars)
    missing = expected - sum(start <= ts < end for ts in timestamps)
    rows = rows_of(series)
    quality = {
        "expected_rows": expected,
        "observed_rows": len([ts for ts in timestamps if start <= ts < end]),
        "error_rows": len(series.errors),
        "missing_rows": max(0, missing),
        "first_ts": timestamps[0] if timestamps else None,
        "last_ts": timestamps[-1] if timestamps else None,
        "complete": (len(rows) == expected and not series.errors
                      and timestamps == list(range(start, end, FIVE))),
    }
    body = {
        "schema": SCHEMA,
        "product": asdict(product),
        "window": {"start": start, "end": end, "interval": "5m"},
        "at": at,
        "quality": quality,
        "source_observations": observations,
        "rows": rows,
    }
    raw = json.dumps(body, separators=(",", ":"), allow_nan=False).encode()
    body["sha256"] = hashlib.sha256(raw).hexdigest()
    payload = gzip.compress(json.dumps(body, separators=(",", ":"), allow_nan=False).encode(), compresslevel=3, mtime=0)
    destination = root / "products" / _product_name(product)
    destination.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(destination, payload)
    return {"product": asdict(product), "snapshot": str(destination), "quality": quality,
            "source_observations": len(observations), "bytes": len(payload)}

def _load_snapshot(path: Path, *, payload: bytes | None = None) -> tuple[Product, Series, dict]:
    with gzip.open(path if payload is None else io.BytesIO(payload), "rb") as stream:
        body = json.loads(stream.read(16 * 1024 * 1024 + 1))
    if body.get("schema") != SCHEMA or not isinstance(body.get("sha256"), str):
        raise Unavailable("research_snapshot_schema")
    check = dict(body)
    saved = check.pop("sha256")
    raw = json.dumps(check, separators=(",", ":"), allow_nan=False).encode()
    if hashlib.sha256(raw).hexdigest() != saved:
        raise Unavailable("research_snapshot_hash")
    item = body["product"]
    product = Product(item["inst_type"], item["inst_id"], item["base"], int(item["normal_since"]))
    window = body["window"]
    series = Series.parse(body["rows"], FIVE)
    expected = (int(window["end"]) - int(window["start"])) // FIVE
    timestamps = sorted(series.bars)
    if (len(timestamps) != expected or timestamps != list(range(int(window["start"]), int(window["end"]), FIVE))
            or series.errors):
        raise Unavailable("research_snapshot_incomplete")
    return product, series, body
