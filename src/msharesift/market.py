"""Canonical public trades, funding and open-interest acquisition.

This module is owned by the alert data layer.  It intentionally keeps the
research boundary separate from the production cache while reusing the same
allowlisted HTTP client, budget, product admission and source-cutoff rules.

OKX exposes paginated public trade and funding history, but its public
open-interest REST endpoint is a point-in-time snapshot rather than a
historical series.  The canonical contract records that limitation instead of
turning repeated current snapshots into fabricated history.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import time
import zipfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import httpx

from msharesift.files import atomic_write
from msharesift.model import HOUR, Product, Unavailable, admit_product, source_cutoff
from msharesift.okx import Budget, BudgetExceeded, PublicClient

SCHEMA = "okx-alert-market-history-v1"
DAY = 24 * HOUR
MAX_TARGET_DAYS = 90
DEFAULT_TARGET_DAYS = 1
PAGE_ROWS_TRADES = 100
PAGE_ROWS_FUNDING = 400
TRADE_PATH = "/api/v5/market/history-trades"
FUNDING_PATH = "/api/v5/public/funding-rate-history"
OPEN_INTEREST_PATH = "/api/v5/public/open-interest"
DATASETS = ("trades", "funding", "open_interest")
BULK_LINK_PATH = "/priapi/v5/broker/public/trade-data/download-link"
BULK_SCHEMA = "okx-alert-market-bulk-v1"
OI_SCHEMA = "okx-alert-open-interest-snapshots-v1"
BULK_MODULES = {"trades": "1", "funding": "3"}


def _product_name(product: Product) -> str:
    digest = hashlib.sha256(json.dumps(product.key, separators=(",", ":")).encode()).hexdigest()
    return digest + ".json.gz"


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise Unavailable(f"invalid_{name}")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise Unavailable(f"invalid_{name}") from exc
    if result < 0:
        raise Unavailable(f"invalid_{name}")
    return result


def _float(value: object, name: str, *, optional: bool = False) -> float | None:
    if value in (None, "") and optional:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise Unavailable(f"invalid_{name}") from exc
    if not math.isfinite(result):
        raise Unavailable(f"invalid_{name}")
    return result


def _trade(row: object, product: Product) -> tuple[int, str, dict]:
    if not isinstance(row, dict):
        raise Unavailable("invalid_trade_row")
    ts = _integer(row.get("ts"), "trade_ts")
    trade_id = row.get("tradeId")
    side = row.get("side")
    if not isinstance(trade_id, str) or not trade_id or side not in ("buy", "sell"):
        raise Unavailable("invalid_trade_identity")
    result = {
        "inst_type": product.inst_type,
        "inst_id": product.inst_id,
        "trade_id": trade_id,
        "ts": ts,
        "side": side,
        "px": _float(row.get("px"), "trade_px"),
        "sz": _float(row.get("sz"), "trade_sz"),
        "source": row.get("source", ""),
    }
    return ts, trade_id, result


def _funding(row: object, product: Product) -> tuple[int, str, dict]:
    if not isinstance(row, dict):
        raise Unavailable("invalid_funding_row")
    funding_time = _integer(row.get("fundingTime"), "funding_time")
    if row.get("instId") not in (None, product.inst_id):
        raise Unavailable("funding_product_mismatch")
    result = {
        "inst_type": product.inst_type,
        "inst_id": product.inst_id,
        "funding_time": funding_time,
        "funding_rate": _float(row.get("fundingRate"), "funding_rate", optional=True),
        "realized_rate": _float(row.get("realizedRate"), "realized_rate", optional=True),
        "formula_type": row.get("formulaType"),
        "method": row.get("method"),
    }
    return funding_time, str(funding_time), result


def _open_interest(row: object, product: Product) -> dict:
    if not isinstance(row, dict):
        raise Unavailable("invalid_open_interest_row")
    if row.get("instId") not in (None, product.inst_id):
        raise Unavailable("open_interest_product_mismatch")
    return {
        "inst_type": product.inst_type,
        "inst_id": product.inst_id,
        "ts": _integer(row.get("ts"), "open_interest_ts"),
        "oi": _float(row.get("oi"), "open_interest"),
        "oi_ccy": _float(row.get("oiCcy"), "open_interest_ccy", optional=True),
        "oi_usd": _float(row.get("oiUsd"), "open_interest_usd", optional=True),
    }


def _paginate(client: PublicClient, path: str, product: Product, start: int, end: int,
              *, time_key: str, limit: int, params: dict[str, str], normalize) -> tuple[list[dict], dict]:
    """Fetch records older than a descending timestamp cursor.

    ``complete`` is true only when the cursor reaches the requested start.  A
    source ending early is reported as partial, never padded or called complete.
    """
    cursor = end
    raw_rows: list[dict] = []
    seen: set[str] = set()
    duplicates = 0
    pages = 0
    reached_start = False
    last_cursor = None
    page_error = None
    while cursor > start:
        request = dict(params)
        request["after"] = str(cursor)
        request["limit"] = str(limit)
        try:
            page = client.get(path, request)
        except (BudgetExceeded, Unavailable) as exc:
            reason = str(exc)
            # Preserve bounded partial history for transport, deadline and
            # HTTP failures.  Shape/pagination failures remain hard errors.
            if not (isinstance(exc, BudgetExceeded) or reason.startswith((
                    "transport:", "http:", "retryable_http:", "acquisition_"))):
                raise
            page_error = reason
            break
        pages += 1
        if not page:
            break
        parsed: list[tuple[int, str, dict]] = []
        for row in page:
            parsed.append(normalize(row, product))
        timestamps = [item[0] for item in parsed]
        if any(ts >= cursor for ts in timestamps):
            raise Unavailable("pagination_not_backward")
        for ts, identity, value in parsed:
            if start <= ts < end and identity not in seen:
                raw_rows.append(value)
                seen.add(identity)
            elif start <= ts < end:
                duplicates += 1
        next_cursor = min(timestamps)
        if last_cursor is not None and next_cursor >= last_cursor:
            raise Unavailable("pagination_stalled")
        last_cursor = next_cursor
        cursor = next_cursor
        if cursor <= start:
            reached_start = True
    raw_rows.sort(key=lambda row: (row[time_key], row.get("trade_id", "")))
    quality = {
        "requested_start": start,
        "requested_end": end,
        "rows": len(raw_rows),
        "duplicates": duplicates,
        "pages": pages,
        "first_ts": raw_rows[0][time_key] if raw_rows else None,
        "last_ts": raw_rows[-1][time_key] if raw_rows else None,
        "cursor_reached_start": reached_start,
        "complete": reached_start and page_error is None,
        "stopped_before_start": bool(raw_rows) and not reached_start,
        "source_exhausted_before_start": bool(raw_rows) and not reached_start and page_error is None,
    }
    ordered_times = [row[time_key] for row in raw_rows]
    deltas = [right - left for left, right in zip(ordered_times, ordered_times[1:])]
    if deltas:
        quality["min_delta_ms"] = min(deltas)
        quality["max_delta_ms"] = max(deltas)
    if page_error is not None:
        quality["error"] = page_error
    return raw_rows, quality


def fetch_trades(client: PublicClient, product: Product, start: int, end: int) -> tuple[list[dict], dict]:
    return _paginate(client, TRADE_PATH, product, start, end, time_key="ts",
                     limit=PAGE_ROWS_TRADES, params={"instId": product.inst_id, "type": "2"},
                     normalize=_trade)


def fetch_funding(client: PublicClient, product: Product, start: int, end: int, *, rules=None) -> tuple[list[dict], dict]:
    if product.inst_type == "FUTURES" and rules is None:
        return [], {"requested_start": start, "requested_end": end, "rows": 0,
                    "complete": False, "error": "contract_rules_required_for_funding"}
    applies = rules["funding"] if rules is not None else product.inst_type == "SWAP"
    if not applies:
        return [], {"requested_start": start, "requested_end": end, "rows": 0,
                     "complete": True, "not_applicable": True}
    return _paginate(client, FUNDING_PATH, product, start, end, time_key="funding_time",
                     limit=PAGE_ROWS_FUNDING, params={"instId": product.inst_id},
                     normalize=_funding)


def fetch_open_interest(client: PublicClient, product: Product, at: int) -> tuple[list[dict], dict]:
    if product.inst_type not in ("SWAP", "FUTURES"):
        return [], {"observed_at": at, "rows": 0, "history_supported": False,
                    "complete": True, "not_applicable": True}
    page = client.get(OPEN_INTEREST_PATH, {"instType": product.inst_type, "instId": product.inst_id})
    rows = [_open_interest(row, product) for row in page]
    rows.sort(key=lambda row: row["ts"])
    return rows, {
        "observed_at": at,
        "rows": len(rows),
        "first_ts": rows[0]["ts"] if rows else None,
        "last_ts": rows[-1]["ts"] if rows else None,
        "history_supported": False,
        "complete": bool(rows),
        "point_in_time_only": True,
    }


def _write_dataset(root: Path, product: Product, dataset: str, start: int, end: int,
                   at: int, rows: list[dict], quality: dict, endpoint: str,
                   *, artifact_name: str | None = None) -> dict:
    body = {
        "schema": SCHEMA,
        "dataset": dataset,
        "product": asdict(product),
        "window": {"start": start, "end": end},
        "at": at,
        "endpoint": endpoint,
        "quality": quality,
        "rows": rows,
    }
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    body["sha256"] = hashlib.sha256(raw).hexdigest()
    payload = gzip.compress(json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(),
                            compresslevel=3, mtime=0)
    destination = root / "products" / dataset / (artifact_name or _product_name(product))
    destination.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(destination, payload)
    status = "complete" if quality.get("complete") else "partial" if rows else "failed"
    return {"dataset": dataset, "product": asdict(product), "path": str(destination),
            "status": status,
            "quality": quality, "rows": len(rows), "bytes": len(payload)}


def _load_dataset(path: Path) -> dict:
    try:
        with gzip.open(path, "rb") as stream:
            body = json.loads(stream.read(512 * 1024 * 1024 + 1))
    except (OSError, ValueError, UnicodeError) as exc:
        raise Unavailable("market_history_read") from exc
    if body.get("schema") != SCHEMA or not isinstance(body.get("sha256"), str):
        raise Unavailable("market_history_schema")
    check = dict(body)
    saved = check.pop("sha256")
    raw = json.dumps(check, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if hashlib.sha256(raw).hexdigest() != saved:
        raise Unavailable("market_history_hash")
    if body.get("dataset") not in DATASETS or not isinstance(body.get("rows"), list):
        raise Unavailable("market_history_dataset")
    return body


def verify(root: Path) -> dict:
    manifest_path = Path(root) / "manifest.json"
    if not manifest_path.exists():
        raise Unavailable("market_history_manifest_missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != SCHEMA or not isinstance(manifest.get("datasets"), list):
        raise Unavailable("market_history_manifest_schema")
    checked, failed = 0, []
    for item in manifest["datasets"]:
        path = Path(item["path"])
        try:
            body = _load_dataset(path)
            if body["dataset"] != item["dataset"] or body["product"] != item["product"]:
                raise Unavailable("market_history_identity")
            checked += len(body["rows"])
        except (OSError, ValueError, KeyError, TypeError, Unavailable) as exc:
            failed.append({"path": item.get("path"), "error": str(exc)})
    result = {"schema": SCHEMA, "datasets_checked": len(manifest["datasets"]),
              "rows_checked": checked, "failed": failed,
              "status": "complete" if not failed else "failed"}
    atomic_write(Path(root) / "verify.json", json.dumps(result, indent=2).encode())
    return result


def _discover_products(client: PublicClient, inst_ids: Iterable[str] | None,
                       per_side: int, at: int) -> list[Product]:
    wanted = set(inst_ids or ())
    candidates: list[tuple[Product, float]] = []
    for kind in ("SPOT", "SWAP"):
        raw_products = client.get("/api/v5/public/instruments", {"instType": kind})
        volumes = {row.get("instId"): float(row.get("volCcy24h") or 0)
                   for row in client.get("/api/v5/market/tickers", {"instType": kind})
                   if isinstance(row, dict)}
        for raw in raw_products:
            try:
                product = admit_product(raw, at)
            except (Unavailable, TypeError, ValueError):
                continue
            if wanted and product.inst_id not in wanted:
                continue
            volume = volumes.get(product.inst_id, 0.0)
            if wanted or (math.isfinite(volume) and volume > 0):
                candidates.append((product, volume))
    if wanted:
        missing = wanted - {product.inst_id for product, _ in candidates}
        if missing:
            raise Unavailable("product_not_found:" + ",".join(sorted(missing)))
        return [product for product, _ in sorted(candidates, key=lambda value: value[0].key)]
    selected = []
    for kind in ("SPOT", "SWAP"):
        pool = [item for item in candidates if item[0].inst_type == kind]
        selected.extend(product for product, _ in sorted(pool, key=lambda value: (-value[1], value[0].key))[:per_side])
    return selected


def fetch_market_history(root: Path, *, inst_ids: Iterable[str] | None = None,
                         per_side: int = 1, target_days: int = DEFAULT_TARGET_DAYS,
                         max_requests: int = 2400, seconds: int = 480,
                         end_ms: int | None = None,
                         datasets: Iterable[str] = DATASETS) -> dict:
    if not 1 <= per_side <= 100 or not 1 <= target_days <= MAX_TARGET_DAYS:
        raise ValueError("invalid market history bounds")
    selected_datasets = tuple(dict.fromkeys(datasets))
    if not selected_datasets or any(name not in DATASETS for name in selected_datasets):
        raise ValueError("invalid market history datasets")
    if end_ms is not None and (not isinstance(end_ms, int) or end_ms <= 0):
        raise ValueError("end_ms must be a positive millisecond timestamp")
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    budget = Budget(max_requests=max_requests, seconds=seconds)
    with PublicClient(budget) as client:
        clock = client.get("/api/v5/public/time")
        at = source_cutoff(clock[0]["ts"]) if end_ms is None else end_ms
        end = at
        start = end - target_days * DAY
        products = _discover_products(client, inst_ids, per_side, at)
        entries = []
        for product in products:
            before_product = len(budget.observations)
            try:
                if "trades" in selected_datasets:
                    before_dataset = len(budget.observations)
                    rows, quality = fetch_trades(client, product, start, end)
                    entry = _write_dataset(root, product, "trades", start, end, at, rows, quality, TRADE_PATH)
                    entry["source_observations"] = budget.observations[before_dataset:]
                    entries.append(entry)
                if "funding" in selected_datasets:
                    before_dataset = len(budget.observations)
                    rows, quality = fetch_funding(client, product, start, end)
                    entry = _write_dataset(root, product, "funding", start, end, at, rows, quality, FUNDING_PATH)
                    entry["source_observations"] = budget.observations[before_dataset:]
                    entries.append(entry)
                if "open_interest" in selected_datasets:
                    before_dataset = len(budget.observations)
                    rows, quality = fetch_open_interest(client, product, at)
                    entry = _write_dataset(root, product, "open_interest", start, end, at, rows, quality, OPEN_INTEREST_PATH)
                    entry["source_observations"] = budget.observations[before_dataset:]
                    entries.append(entry)
            except (BudgetExceeded, OSError, ValueError, Unavailable) as exc:
                entries.append({"product": asdict(product), "status": "failed", "error": str(exc),
                                "source_observations": budget.observations[before_product:]})
    manifest = {
        "schema": SCHEMA,
        "contract": {"datasets": selected_datasets, "window": {"start": start, "end": end},
                      "target_days": target_days, "historical_open_interest": False},
        "source": {"owner": "MshareSift", "budget": {
            "max_requests": budget.max_requests, "attempts": budget.attempts,
            "initial": budget.initial, "retries": budget.retries,
            "received": budget.received, "max_bytes": budget.max_bytes,
            "seconds": budget.seconds}, "products": len(products)},
        "datasets": entries,
    }
    atomic_write(root / "manifest.json", json.dumps(manifest, indent=2, allow_nan=False).encode())
    return {"schema": SCHEMA, "products": len(products),
            "datasets": len(entries),
            "complete": sum(item.get("status") == "complete" for item in entries),
            "partial": sum(item.get("status") == "partial" for item in entries),
            "failed": sum(item.get("status") == "failed" for item in entries),
            "manifest": str(root / "manifest.json"), "budget": manifest["source"]["budget"]}


def _date_ms(value: str) -> int:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("dates must use YYYY-MM-DD") from exc
    return int(parsed.timestamp() * 1000)


def _bulk_payload(product: Product, dataset: str, begin: int, end: int,
                  date_aggregation: str) -> dict:
    if dataset not in BULK_MODULES:
        raise ValueError("bulk dataset must be trades or funding")
    if product.inst_type == "SPOT":
        query = {"instIdList": [product.inst_id]}
    else:
        family = product.inst_id[:-5] if product.inst_id.endswith("-SWAP") else product.inst_id
        query = {"instFamilyList": [family]}
    return {
        "module": BULK_MODULES[dataset],
        "instType": product.inst_type,
        "instQueryParam": query,
        "dateQuery": {"dateAggrType": date_aggregation,
                       "begin": str(begin), "end": str(end)},
    }


def request_bulk_links(client: PublicClient, product: Product, dataset: str,
                       begin: int, end: int, date_aggregation: str) -> list[dict]:
    data = client.post(BULK_LINK_PATH, _bulk_payload(product, dataset, begin, end, date_aggregation))
    if not isinstance(data, dict) or not isinstance(data.get("details"), list):
        raise Unavailable("bulk_link_schema")
    links = []
    for group in data["details"]:
        if not isinstance(group, dict):
            raise Unavailable("bulk_link_group_schema")
        for item in group.get("groupDetails", []):
            if not isinstance(item, dict) or not isinstance(item.get("url"), str):
                raise Unavailable("bulk_link_item_schema")
            url = item["url"]
            if not url.startswith("https://static.okx.com/"):
                raise Unavailable("bulk_link_host")
            links.append({"url": url, "filename": item.get("filename"),
                          "date_ts": item.get("dateTs"),
                          "size_mb": item.get("sizeMB"),
                          "group": {key: group.get(key) for key in
                                     ("instFamily", "instId", "instType", "dateRangeStart", "dateRangeEnd")}})
    return links


def _download_bulk_zip(url: str, *, max_bytes: int = 256 * 1024 * 1024) -> tuple[bytes, dict]:
    started = time.monotonic()
    size = 0
    try:
        with httpx.Client(follow_redirects=False, trust_env=False,
                          headers={"User-Agent": "crypto-boom-public-shadow/1"}) as client:
            with client.stream("GET", url, timeout=httpx.Timeout(60)) as response:
                if response.status_code != 200:
                    raise Unavailable("bulk_http:" + str(response.status_code))
                payload = bytearray()
                for chunk in response.iter_bytes(chunk_size=64 * 1024):
                    size += len(chunk)
                    if size > max_bytes:
                        raise Unavailable("bulk_archive_budget")
                    payload.extend(chunk)
    except httpx.TransportError as exc:
        raise Unavailable("bulk_transport:" + type(exc).__name__) from exc
    return bytes(payload), {"url": url, "status": 200, "bytes": size,
                            "seconds": time.monotonic() - started}


def _parse_bulk_zip(payload: bytes, product: Product, dataset: str) -> tuple[list[dict], dict]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except (OSError, zipfile.BadZipFile) as exc:
        raise Unavailable("bulk_zip_schema") from exc
    files = [name for name in archive.namelist()
             if not name.endswith("/") and name.lower().endswith(".csv")]
    if len(files) != 1 or any(Path(name).name != name for name in files):
        raise Unavailable("bulk_csv_count")
    filename = files[0]
    rows = []
    seen = set()
    duplicates = 0
    with archive.open(filename, "r") as stream:
        text = io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)
        required = ({"instrument_name", "trade_id", "side", "price", "size", "created_time", "source"}
                    if dataset == "trades" else
                    {"instrument_name", "funding_rate", "funding_time"})
        if not required.issubset(set(reader.fieldnames or ())):
            raise Unavailable("bulk_csv_columns")
        for row in reader:
            if row.get("instrument_name") != product.inst_id:
                raise Unavailable("bulk_product_mismatch")
            if dataset == "trades":
                identity = row.get("trade_id")
                if not isinstance(identity, str) or not identity:
                    raise Unavailable("bulk_trade_id")
                if identity in seen:
                    duplicates += 1
                    continue
                seen.add(identity)
                side = row.get("side")
                if side not in ("buy", "sell"):
                    raise Unavailable("bulk_trade_side")
                rows.append({"inst_type": product.inst_type, "inst_id": product.inst_id,
                             "trade_id": identity, "ts": _integer(row.get("created_time"), "bulk_trade_ts"),
                             "side": side, "px": _float(row.get("price"), "bulk_trade_px"),
                             "sz": _float(row.get("size"), "bulk_trade_sz"),
                             "source": row.get("source", "")})
            else:
                funding_time = _integer(row.get("funding_time"), "bulk_funding_time")
                if funding_time in seen:
                    duplicates += 1
                    continue
                seen.add(funding_time)
                rows.append({"inst_type": product.inst_type, "inst_id": product.inst_id,
                             "funding_time": funding_time,
                             "funding_rate": _float(row.get("funding_rate"), "bulk_funding_rate"),
                             "realized_rate": None, "formula_type": None, "method": None})
    time_key = "ts" if dataset == "trades" else "funding_time"
    rows.sort(key=lambda row: (row[time_key], row.get("trade_id", "")))
    quality = {"archive_filename": filename, "csv_columns": reader.fieldnames,
               "rows": len(rows), "duplicates_removed": duplicates,
               "first_ts": rows[0][time_key] if rows else None,
               "last_ts": rows[-1][time_key] if rows else None,
               "complete": bool(rows)}
    return rows, quality


def bulk_fetch(root: Path, *, inst_id: str, dataset: str, begin: str, end: str,
               date_aggregation: str | None = None, max_archives: int = 31) -> dict:
    if dataset not in BULK_MODULES:
        raise ValueError("dataset must be trades or funding")
    begin_ms, end_ms = _date_ms(begin), _date_ms(end)
    if not begin_ms < end_ms:
        raise ValueError("begin must be before end")
    expected_aggregation = "daily" if dataset == "trades" else "monthly"
    date_aggregation = date_aggregation or expected_aggregation
    if date_aggregation != expected_aggregation:
        raise ValueError(f"{dataset} bulk data requires {expected_aggregation} aggregation")
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    budget = Budget(max_requests=2400, seconds=480)
    with PublicClient(budget) as client:
        clock = client.get("/api/v5/public/time")
        at = source_cutoff(clock[0]["ts"])
        products = _discover_products(client, [inst_id], 1, at)
        if len(products) != 1:
            raise Unavailable("bulk_product_not_found")
        product = products[0]
        links = request_bulk_links(client, product, dataset, begin_ms, end_ms, date_aggregation)
    if len(links) > max_archives:
        raise Unavailable("bulk_archive_count_budget")
    entries = []
    for link in links:
        payload, transfer = _download_bulk_zip(link["url"])
        rows, quality = _parse_bulk_zip(payload, product, dataset)
        time_key = "ts" if dataset == "trades" else "funding_time"
        archive_rows = len(rows)
        rows = [row for row in rows if begin_ms <= row[time_key] < end_ms]
        quality["archive_rows"] = archive_rows
        quality["out_of_window_rows"] = archive_rows - len(rows)
        quality["rows"] = len(rows)
        quality["first_ts"] = rows[0][time_key] if rows else None
        quality["last_ts"] = rows[-1][time_key] if rows else None
        quality["complete"] = bool(rows)
        quality.update(transfer)
        quality.update({"requested_start": begin_ms, "requested_end": end_ms,
                        "date_aggregation": date_aggregation, "source": "okx_bulk_download"})
        stem = Path(link.get("filename") or f"{dataset}-{link.get('date_ts') or 'unknown'}").stem
        artifact_name = hashlib.sha256((product.inst_id + "|" + stem).encode()).hexdigest() + ".json.gz"
        entry = _write_dataset(root, product, dataset, begin_ms, end_ms, at, rows, quality,
                               "okx://" + link["url"], artifact_name=artifact_name)
        entry["bulk_link"] = link
        entries.append(entry)
    manifest_path = root / "bulk-manifest.json"
    prior = {}
    if manifest_path.exists():
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        if prior.get("schema") != BULK_SCHEMA:
            raise Unavailable("bulk_manifest_schema")
    merged = {item.get("path"): item for item in prior.get("datasets", [])}
    merged.update({item.get("path"): item for item in entries})
    manifest = {"schema": BULK_SCHEMA,
                "contract": {"datasets": sorted({item.get("dataset") for item in merged.values()}),
                              "date_aggregation": "mixed" if len({item.get("quality", {}).get("date_aggregation") for item in merged.values()}) > 1
                              else date_aggregation},
                "source": {"owner": "MshareSift", "link_endpoint": BULK_LINK_PATH,
                           "budget": {"attempts": budget.attempts, "received": budget.received}},
                "datasets": sorted(merged.values(), key=lambda item: item.get("path", ""))}
    atomic_write(manifest_path, json.dumps(manifest, indent=2, allow_nan=False).encode())
    return {"schema": BULK_SCHEMA, "dataset": dataset, "archives": len(entries),
            "rows": sum(item.get("rows", 0) for item in entries),
            "complete": sum(item.get("status") == "complete" for item in entries),
            "manifest": str(manifest_path)}


def verify_bulk(root: Path) -> dict:
    root = Path(root)
    path = root / "bulk-manifest.json"
    if not path.exists():
        raise Unavailable("bulk_manifest_missing")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("schema") != BULK_SCHEMA or not isinstance(manifest.get("datasets"), list):
        raise Unavailable("bulk_manifest_schema")
    failed = []
    rows = 0
    for item in manifest["datasets"]:
        try:
            body = _load_dataset(Path(item["path"]))
            if body.get("schema") != SCHEMA or body.get("dataset") != item.get("dataset"):
                raise Unavailable("bulk_identity")
            rows += len(body["rows"])
        except (OSError, ValueError, KeyError, TypeError, Unavailable) as exc:
            failed.append({"path": item.get("path"), "error": str(exc)})
    result = {"schema": BULK_SCHEMA, "archives_checked": len(manifest["datasets"]),
              "rows_checked": rows, "failed": failed,
              "status": "complete" if not failed else "failed"}
    atomic_write(root / "bulk-verify.json", json.dumps(result, indent=2).encode())
    return result


def _write_oi_snapshot(root: Path, at: int, rows: list[dict], quality: dict) -> dict:
    body = {"schema": OI_SCHEMA, "observed_at": at, "quality": quality, "rows": rows}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    body["sha256"] = hashlib.sha256(raw).hexdigest()
    path = Path(root) / "snapshots" / f"{at}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(path, gzip.compress(json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(),
                                     compresslevel=3, mtime=0))
    return {"path": str(path), "observed_at": at, "rows": len(rows), "quality": quality}


def collect_oi_snapshot(root: Path, *, inst_ids: Iterable[str] | None = None,
                        per_side: int = 1, max_requests: int = 2400,
                        seconds: int = 120) -> dict:
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    budget = Budget(max_requests=max_requests, seconds=seconds)
    with PublicClient(budget) as client:
        clock = client.get("/api/v5/public/time")
        at = source_cutoff(clock[0]["ts"])
        products = [product for product in _discover_products(client, inst_ids, per_side, at)
                    if product.inst_type == "SWAP"]
        rows, failures = [], []
        for product in products:
            try:
                values, quality = fetch_open_interest(client, product, at)
                rows.extend(values)
                if not quality.get("complete"):
                    failures.append({"inst_id": product.inst_id, "quality": quality})
            except (BudgetExceeded, OSError, ValueError, Unavailable) as exc:
                failures.append({"inst_id": product.inst_id, "error": str(exc)})
    quality = {"products_requested": len(products), "products_with_rows": len({row["inst_id"] for row in rows}),
               "rows": len(rows), "failures": failures, "history_supported": False,
               "source": OPEN_INTEREST_PATH,
               "budget": {"attempts": budget.attempts, "received": budget.received}}
    entry = _write_oi_snapshot(root, at, rows, quality)
    manifest_path = root / "oi-manifest.json"
    manifest = {"schema": OI_SCHEMA, "source": {"owner": "MshareSift"}, "snapshots": []}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("snapshots", [])
    manifest["snapshots"] = [item for item in manifest["snapshots"] if item.get("path") != entry["path"]]
    manifest["snapshots"].append(entry)
    manifest["snapshots"].sort(key=lambda item: item.get("observed_at", 0))
    atomic_write(manifest_path, json.dumps(manifest, indent=2, allow_nan=False).encode())
    return {"schema": OI_SCHEMA, "snapshot": entry, "manifest": str(manifest_path)}


def oi_report(root: Path) -> dict:
    root = Path(root)
    manifest_path = root / "oi-manifest.json"
    if not manifest_path.exists():
        raise Unavailable("oi_manifest_missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    snapshots = manifest.get("snapshots", [])
    rows = []
    failed = []
    loaded = []
    for item in snapshots:
        try:
            body = _load_oi_snapshot(Path(item["path"]))
            rows.extend(body["rows"])
            loaded.append((body.get("observed_at", item.get("observed_at")), body["rows"]))
        except (OSError, ValueError, KeyError, TypeError, Unavailable) as exc:
            failed.append({"path": item.get("path"), "error": str(exc)})
    keys = [(row["inst_id"], row["ts"]) for row in rows]
    duplicate_count = len(keys) - len(set(keys))
    lags = [row["ts"] - observed_at for observed_at, snapshot_rows in loaded for row in snapshot_rows]
    result = {"schema": OI_SCHEMA, "snapshots": len(snapshots), "rows": len(rows),
              "products": len({row["inst_id"] for row in rows}),
              "duplicate_keys": duplicate_count,
              "source_ts_lag_ms": {"min": min(lags) if lags else None, "max": max(lags) if lags else None},
              "failed": failed, "history_supported": False,
              "status": "complete" if not failed and duplicate_count == 0 else "failed"}
    atomic_write(root / "oi-quality-report.json", json.dumps(result, indent=2).encode())
    return result


def _load_oi_snapshot(path: Path) -> dict:
    with gzip.open(path, "rb") as stream:
        body = json.loads(stream.read(16 * 1024 * 1024 + 1))
    if body.get("schema") != OI_SCHEMA or not isinstance(body.get("sha256"), str):
        raise Unavailable("oi_snapshot_schema")
    check = dict(body)
    saved = check.pop("sha256")
    raw = json.dumps(check, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if hashlib.sha256(raw).hexdigest() != saved:
        raise Unavailable("oi_snapshot_hash")
    return body
