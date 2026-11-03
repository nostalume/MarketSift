"""Canonical segment union is explicit about duplicates, conflicts and gaps."""

import json
from dataclasses import asdict

import pytest

from msharesift.candles.collection import load_collection
from msharesift.candles.frame import clean_frame, frame_from_bars
from msharesift.candles.history import DATA_SCHEMA, digest_file
from msharesift.model import FIVE, Bar, Product, Unavailable


def dataset(root, start, end, *, omitted=(), price=100, since=1):
    root.mkdir()
    frame = frame_from_bars(
        [
            Bar(t, price, price + 1, price - 1, price, 1, 1, 100)
            for t in range(start, end, FIVE)
            if t not in omitted
        ]
    )
    _, quality = clean_frame(frame, FIVE, start, end)
    frame.write_parquet(root / "shard.parquet")
    product = Product("SPOT", "X-USDT", "X", since)
    status = "partial" if omitted else "complete"
    value = {
        "schema": DATA_SCHEMA,
        "contract": {
            "inst_ids": [product.inst_id],
            "grain": FIVE,
            "start": start,
            "end": end,
        },
        "products": [
            {
                "product": asdict(product),
                "status": status,
                "shards": [
                    {
                        "file": "shard.parquet",
                        "start": start,
                        "end": end,
                        "rows": frame.height,
                        "complete": not omitted,
                        "sha256": digest_file(root / "shard.parquet"),
                        "quality": quality,
                    }
                ],
            }
        ],
    }
    (root / "manifest.json").write_text(json.dumps(value))
    return root


def test_adjacent_canonical_windows_join_and_keep_receipts(tmp_path):
    a = dataset(tmp_path / "a", 0, 3 * FIVE)
    b = dataset(tmp_path / "b", 3 * FIVE, 6 * FIVE)
    histories, audit = load_collection([b, a])
    assert len(histories) == 1 and len(histories[0].bars) == 6
    assert (histories[0].start, histories[0].end) == (0, 6 * FIVE)
    assert len(histories[0].provenance) == 2
    assert audit[-1]["status"] == "complete"


def test_exact_overlap_deduplicates_and_conflict_rejects(tmp_path):
    a = dataset(tmp_path / "a", 0, 4 * FIVE)
    b = dataset(tmp_path / "b", 2 * FIVE, 6 * FIVE)
    histories, audit = load_collection([a, b])
    assert len(histories[0].bars) == 6 and audit[-1]["duplicate_rows_removed"] == 2
    c = dataset(tmp_path / "c", 2 * FIVE, 6 * FIVE, price=101)
    with pytest.raises(Unavailable, match="conflicting_complete_candle"):
        load_collection([a, c])


def test_gap_stays_partial_and_is_not_filled(tmp_path):
    a = dataset(tmp_path / "a", 0, 2 * FIVE)
    b = dataset(tmp_path / "b", 3 * FIVE, 5 * FIVE)
    histories, audit = load_collection([a, b])
    assert len(histories[0].bars) == 4
    assert audit[-1]["missing_rows"] == 1 and audit[-1]["status"] == "partial"


def test_duplicate_roots_and_changed_identity_refused(tmp_path):
    a = dataset(tmp_path / "a", 0, 2 * FIVE)
    b = dataset(tmp_path / "b", 2 * FIVE, 4 * FIVE, since=2)
    with pytest.raises(ValueError, match="distinct"):
        load_collection([a, a])
    with pytest.raises(Unavailable, match="identity_changed"):
        load_collection([a, b])
