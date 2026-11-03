"""Primary-owner bounded 300-row page throughput calibration."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from msharesift.candles.universe import sha, write_json
from msharesift.model import FIVE, HOUR, Series, Unavailable
from msharesift.okx import Budget, PublicClient


def benchmark_pages(root, inventory_root, *, pages=24, seconds=90, transport=None):
    root, inventory_root = Path(root), Path(inventory_root)
    if root.exists():
        raise FileExistsError("preserve throughput attempts")
    if not 8 <= pages <= 64:
        raise ValueError("pages must be 8..64")
    meta = json.loads((inventory_root / "manifest.json").read_text(encoding="utf-8"))
    if (
        sha(inventory_root / "instruments.parquet")
        != meta["files"]["instruments.parquet"]
    ):
        raise Unavailable("inventory_integrity_mismatch")
    census = pl.read_parquet(inventory_root / "instruments.parquet")
    names = (
        census.filter(
            pl.col("research_eligible")
            & pl.col("base").is_in(["BTC", "ETH", "DOGE", "SOL"])
        )["inst_id"]
        .sort()
        .to_list()
    )
    if len(names) < 4:
        raise Unavailable("benchmark metadata coverage insufficient")
    end = meta["study_end_ms"]
    records = []
    root.mkdir(parents=True)
    for workers in [1, 4]:
        budget = Budget(
            max_requests=pages + max(4, pages // 5), seconds=seconds, reserve=10
        )
        local = threading.local()
        clients = []
        lock = threading.Lock()

        def initialize(*, local=local, budget=budget, lock=lock, clients=clients):
            local.client = PublicClient(budget, transport=transport)
            with lock:
                clients.append(local.client)

        def fetch(i, *, local=local, workers=workers):
            grain = FIVE if i % 2 else HOUR
            # Distinct timestamp pages per arm; same mixture, each connection kept alive.
            cursor = end - (i // len(names) + workers * 10) * 300 * grain
            t = time.monotonic()
            page = local.client.get(
                "/api/v5/market/history-candles",
                {
                    "instId": names[i % len(names)],
                    "bar": "5m" if grain == FIVE else "1H",
                    "after": str(cursor),
                    "limit": "300",
                },
            )
            parsed = Series.parse(page, grain)
            return {
                "inst_id": names[i % len(names)],
                "source_minutes": grain // 60000,
                "rows": len(parsed.bars),
                "invalid_rows": len(parsed.errors),
                "request_seconds": time.monotonic() - t,
            }

        started = time.monotonic()
        try:
            with ThreadPoolExecutor(
                max_workers=workers, initializer=initialize
            ) as pool:
                rows = list(pool.map(fetch, range(pages)))
        finally:
            for c in clients:
                c.__exit__(None, None, None)
        duration = time.monotonic() - started
        records.append(
            {
                "workers": workers,
                "pages": pages,
                "elapsed_seconds": duration,
                "requests": budget.attempts,
                "retries": budget.retries,
                "wire_bytes": budget.received,
                "pages_per_second": pages / duration,
                "rows": sum(r["rows"] for r in rows),
                "records": rows,
                "observations": budget.observations,
            }
        )
    result = {
        "schema": "okx-candle-throughput-v1",
        "inventory_sha256": sha(inventory_root / "manifest.json"),
        "meaning": "300-row public GET/parse calibration; excludes full shard cleanup/disk and not a hard throughput guarantee",
        "arms": records,
        "speedup": records[0]["elapsed_seconds"] / records[1]["elapsed_seconds"],
    }
    write_json(root / "benchmark.json", result)
    return result
