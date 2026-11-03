"""Read canonical snapshots, preserve gaps, aggregate on UTC boundaries."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path

import polars as pl

from msharesift.candles.frame import (
    _aggregate_frame,
    _check_resolution,
    _check_window,
    _coverage,
    _merge_frame,
    aggregate_frame,
    clean_frame,
    frame_from_bars,
)
from msharesift.files import atomic_write
from msharesift.model import Bar, Product, Series, Unavailable
from msharesift.snapshot import _load_snapshot

DATA_SCHEMA = "okx-canonical-candles-v1"


def digest_file(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


@dataclass(frozen=True)
class History:
    product: Product
    grain: int
    bars: tuple[Bar, ...]
    start: int
    end: int
    provenance: tuple[dict, ...]
    canonical_frame: pl.DataFrame | None = field(
        default=None, repr=False, compare=False
    )
    rules: dict | None = field(default=None, compare=False)

    @cached_property
    def frame(self):
        return (
            self.canonical_frame
            if self.canonical_frame is not None
            else frame_from_bars(self.bars)
        )

    def resample(self, minutes):
        """Use an admitted canonical frame directly; raw Bar inputs still enter once."""
        target = minutes * 60_000
        if self.canonical_frame is None:
            return aggregate_frame(self.frame, self.grain, target, self.start, self.end)
        _check_resolution(self.grain, target)
        return _aggregate_frame(self.frame, self.grain, target, self.start, self.end)


def _resolve_snapshot(root, value):
    path = Path(value)
    if path.is_absolute():
        return path
    # Older canonical manifests store project-relative paths, rather than paths
    # relative to the manifest. Resolve without relying on the shell cwd.
    for parent in (root, *root.parents):
        candidate = parent / path
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(value)


def load_histories(root, *, inst_ids=None, start=None, end=None):
    """Read all history by default, or admit only the selected products/shards.

    A selected shard is checked in full before cropping. Unselected files are
    unopened and outside the integrity claim. Empty selections return no history;
    absent requested products remain visible in the population audit.
    """
    histories, audit, _ = _read_histories(
        root, inst_ids=inst_ids, start=start, end=end
    )
    return histories, audit


def _read_histories(root, *, inst_ids=None, start=None, end=None):
    root = Path(root).resolve()
    payload = (root / "manifest.json").read_bytes()
    manifest = json.loads(payload)
    manifest_hash = hashlib.sha256(payload).hexdigest()
    selected = None if inst_ids is None else frozenset(inst_ids)
    scoped = selected is not None or start is not None or end is not None
    histories, audit = [], []
    declared_ids = set()
    if manifest.get("schema") in (DATA_SCHEMA, "ema-research-candles-v1"):
        contract = manifest["contract"]
        declared_ids = set(contract["inst_ids"])
        grain = contract["grain"]
        begin = contract["start"] if start is None else start
        finish = contract["end"] if end is None else end
        _check_window(grain, contract["start"], contract["end"])
        if (begin, finish) != (contract["start"], contract["end"]):
            _check_window(grain, begin, finish)
        for item in manifest["products"]:
            if selected is not None and item["product"]["inst_id"] not in selected:
                continue
            frames = []
            evidence = []
            for shard in item["shards"]:
                if (start is not None or end is not None) and (
                    shard["end"] <= begin or shard["start"] >= finish
                ):
                    continue
                path = (root / shard["file"]).resolve()
                if not path.is_relative_to(root):
                    raise Unavailable("candle_shard_path_outside_dataset")
                payload = path.read_bytes()
                if hashlib.sha256(payload).hexdigest() != shard["sha256"]:
                    raise Unavailable("research_shard_hash")
                if path.suffix == ".parquet":
                    frame, quality = clean_frame(
                        pl.read_parquet(io.BytesIO(payload)),
                        grain,
                        shard["start"],
                        shard["end"],
                    )
                else:
                    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
                        raw = stream.read(16 * 1024 * 1024 + 1)
                    if len(raw) > 16 * 1024 * 1024:
                        raise Unavailable("candle_shard_size_limit")
                    body = json.loads(raw)
                    if (
                        body["product"] != item["product"]
                        or body["grain"] != manifest["contract"]["grain"]
                    ):
                        raise Unavailable("research_shard_identity")
                    series = Series.parse(body["rows"], body["grain"])
                    if series.errors:
                        raise Unavailable("research_shard_invalid_rows")
                    _check_window(grain, shard["start"], shard["end"])
                    frame = frame_from_bars(series.bars.values()).sort("ts")
                    if frame.height and (
                        frame["ts"][0] < shard["start"]
                        or frame["ts"][-1] >= shard["end"]
                    ):
                        raise Unavailable("candle_frame_invalid_rows")
                    quality = _coverage(frame, grain, shard["start"], shard["end"])
                if frame.height != shard["rows"] or (
                    shard["complete"] and quality["missing_rows"]
                ):
                    raise Unavailable("candle_shard_coverage_mismatch")
                if frame.height and (
                    frame["ts"][0] < contract["start"]
                    or frame["ts"][-1] >= contract["end"]
                ):
                    raise Unavailable("candle_frame_invalid_rows")
                if begin > shard["start"] or finish < shard["end"]:
                    frame = frame.filter((pl.col("ts") >= begin) & (pl.col("ts") < finish))
                frames.append(frame)
                evidence.append({"path": str(path), "sha256": shard["sha256"]})
            product = Product(**item["product"])
            if len(frames) > 1:
                frame, quality = _merge_frame(pl.concat(frames), grain, begin, finish)
            else:
                frame = frames[0] if frames else frame_from_bars(())
                quality = _coverage(frame, grain, begin, finish)
            bars = tuple(Bar(*row) for row in frame.iter_rows())
            declared_rows = max(
                0, min(finish, contract["end"]) - max(begin, contract["start"])
            ) // grain
            if item["status"] == "complete" and frame.height != declared_rows:
                raise Unavailable("candle_product_coverage_mismatch")
            status = (
                "complete" if quality["missing_rows"] == 0 else "partial"
            ) if scoped else item["status"]
            histories.append(
                History(
                    product,
                    contract["grain"],
                    bars,
                    begin,
                    finish,
                    tuple(evidence),
                    frame,
                    item.get("rules"),
                )
            )
            audit.append(
                {
                    "product": product.key,
                    "status": status,
                    "rows": len(bars),
                    "missing_rows": quality["missing_rows"],
                    "manifest_sha256": manifest_hash,
                    **({"source_status": item["status"], "start": begin, "end": finish,
                        "integrity_scope": "selected_shards", "shards_read": len(evidence),
                        "shards_skipped": len(item["shards"]) - len(evidence)} if scoped else {}),
                }
            )
    elif manifest.get("schema") == "okx-alert-canonical-research-v1":
        for item in manifest["products"]:
            if selected is not None and item["product"]["inst_id"] not in selected:
                continue
            if item.get("status") != "complete":
                audit.append(
                    {
                        "product": item["product"],
                        "status": item.get("status", "unknown"),
                    }
                )
                continue
            path = _resolve_snapshot(root, item["snapshot"])
            payload = path.read_bytes()
            product, series, body = _load_snapshot(path, payload=payload)
            if body["product"] != item["product"]:
                raise Unavailable("manifest_snapshot_identity")
            window = body["window"]
            begin = window["start"] if start is None else start
            finish = window["end"] if end is None else end
            _check_window(series.grain, begin, finish)
            bars = tuple(series.bars[t] for t in sorted(series.bars) if begin <= t < finish)
            frame = frame_from_bars(bars)
            histories.append(
                History(
                    product,
                    series.grain,
                    bars,
                    begin,
                    finish,
                    ({"path": str(path), "sha256": hashlib.sha256(payload).hexdigest()},),
                    frame,
                )
            )
            declared = manifest["contract"]["window"]
            audit.append(
                {
                    "product": product.key,
                    "status": "complete" if len(bars) == (finish-begin)//series.grain else "partial",
                    "rows": len(bars),
                    "missing_rows": (finish-begin)//series.grain - len(bars),
                    "manifest_sha256": manifest_hash,
                    "window_matches_manifest": all(
                        window[k] == declared[k] for k in ("start", "end")
                    ),
                    "actual_window": window,
                    **({"start": begin, "end": finish,
                        "integrity_scope": "selected_snapshots"} if scoped else {}),
                }
            )
    else:
        raise ValueError("unsupported dataset schema")
    missing_products = (declared_ids if selected is None else selected) - {
        h.product.inst_id for h in histories
    }
    if missing_products:
        audit.append(
            {"status": "partial_population", "missing_products": sorted(missing_products)}
        )
    if not histories and not scoped:
        raise ValueError("dataset has no readable products")
    # A manifest must not count the same product/grain twice as independent data.
    identities = [(h.product.key, h.grain) for h in histories]
    if len(identities) != len(set(identities)):
        raise Unavailable("duplicate_product_history")
    return histories, audit, manifest_hash


def normalize_dataset(source, destination):
    """Convert admitted legacy/canonical data to shared-window Parquet snapshots."""
    histories, audit, manifest_hash = _read_histories(source)
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(
            "normalization destination exists; preserve prior dataset"
        )
    grains = {h.grain for h in histories}
    if len(grains) != 1:
        raise ValueError("normalize one source resolution at a time")
    grain = grains.pop()
    start, end = max(h.start for h in histories), min(h.end for h in histories)
    if start >= end:
        raise ValueError("no shared window")
    manifest = {
        "schema": DATA_SCHEMA,
        "contract": {
            "inst_ids": sorted(h.product.inst_id for h in histories),
            "grain": grain,
            "start": start,
            "end": end,
            "source": "mainline.PublicClient+fetch_range",
            "universe": "explicit_current_research_products"
            if any(h.rules is not None for h in histories) else "explicit_current_survivors",
        },
        "normalization": {
            "source_manifest": str(Path(source).resolve() / "manifest.json"),
            "source_manifest_sha256": manifest_hash,
            "audit": audit,
            "window_policy": "intersection_of_declared_product_windows",
        },
        "products": [],
        "attempts": [],
    }
    destination.mkdir(parents=True)
    (destination / "shards").mkdir()
    for history in histories:
        item = {
            "product": {
                "inst_type": history.product.inst_type,
                "inst_id": history.product.inst_id,
                "base": history.product.base,
                "normal_since": history.product.normal_since,
            },
            "status": "complete",
            "shards": [],
            "errors": [],
            "provenance": history.provenance,
        }
        if history.rules is not None:
            item["rules"] = history.rules
        frame = history.frame.filter((pl.col("ts") >= start) & (pl.col("ts") < end))
        for begin in range(start, end, 30 * 24 * 3_600_000):
            finish = min(end, begin + 30 * 24 * 3_600_000)
            shard_frame = frame.filter(
                (pl.col("ts") >= begin) & (pl.col("ts") < finish)
            )
            clean = shard_frame
            quality = _coverage(clean, grain, begin, finish)
            complete = quality["missing_rows"] == 0
            if not complete:
                item["status"] = "partial"
            payload = io.BytesIO()
            clean.write_parquet(payload, compression="zstd")
            content = payload.getvalue()
            name = (
                hashlib.sha256(
                    f"{history.product.key}:{begin}:{finish}:".encode() + content
                ).hexdigest()
                + ".parquet"
            )
            path = destination / "shards" / name
            atomic_write(path, content)
            item["shards"].append(
                {
                    "file": f"shards/{name}",
                    "start": begin,
                    "end": finish,
                    "rows": clean.height,
                    "complete": complete,
                    "quality": quality,
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            )
        manifest["products"].append(item)
        atomic_write(
            destination / "manifest.json",
            json.dumps(manifest, indent=2, allow_nan=False).encode(),
        )
    return {
        "products": len(histories),
        "complete": sum(i["status"] == "complete" for i in manifest["products"]),
        "window": {"start": start, "end": end},
        "manifest": str(destination / "manifest.json"),
    }
