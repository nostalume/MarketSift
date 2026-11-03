"""Mainline history ownership, normalization and bounded public collection."""

import json

import httpx
import pytest

from msharesift.candles.acquire import acquire
from msharesift.candles.history import load_histories, normalize_dataset
from msharesift.model import FIVE, HOUR, Unavailable


def source(*, missing=False):
    calls = []

    def handle(request):
        calls.append(request)
        if request.url.path.endswith("/time"):
            data = [{"ts": str(1000 * HOUR + 2000)}]
        elif request.url.path.endswith("/instruments"):
            data = [
                {
                    "instType": "SWAP",
                    "instId": "X-USDT-SWAP",
                    "instCategory": "1",
                    "state": "live",
                    "ruleType": "normal",
                    "instFamily": "X-USDT",
                    "ctType": "linear",
                    "settleCcy": "USDT",
                    "ctValCcy": "X",
                    "ctVal": "1",
                    "listTime": "1",
                }
            ]
        else:
            grain = HOUR if request.url.params["bar"] == "1H" else FIVE
            end, limit = (
                int(request.url.params["after"]),
                int(request.url.params["limit"]),
            )
            data = [
                [str(t), "100", "102", "99", "101", "1", "1", "100", "1"]
                for t in range(
                    end - grain, max(-grain, end - (limit + 1) * grain), -grain
                )
                if not (missing and t == 5 * grain)
            ]
        return httpx.Response(200, json={"code": "0", "data": data})

    return httpx.MockTransport(handle), calls


def test_mainline_fetch_hourly_parquet_roundtrip_and_fixed_resume(tmp_path):
    transport, calls = source()
    root = tmp_path / "data"
    result = acquire(root, ["X-USDT-SWAP"], "1h", 0, 30 * HOUR, transport=transport)
    assert result["complete"] == 1
    histories, audit = load_histories(root)
    assert histories[0].grain == HOUR and len(histories[0].bars) == 30
    assert audit[0]["missing_rows"] == 0
    count = len(calls)
    acquire(root, ["X-USDT-SWAP"], "1h", 0, 30 * HOUR, transport=transport)
    assert len(calls) == count + 2  # Current clock and instrument identity only.
    with pytest.raises(ValueError, match="contract mismatch"):
        acquire(root, ["X-USDT-SWAP"], "1h", 0, 31 * HOUR, transport=transport)


def test_partial_shard_can_be_retried_without_lying_about_coverage(tmp_path):
    partial_transport, _ = source(missing=True)
    full_transport, _ = source()
    root = tmp_path / "data"
    assert (
        acquire(root, ["X-USDT-SWAP"], "5m", 0, 30 * FIVE, transport=partial_transport)[
            "complete"
        ]
        == 0
    )
    assert load_histories(root)[1][0]["missing_rows"] == 1
    assert (
        acquire(root, ["X-USDT-SWAP"], "5m", 0, 30 * FIVE, transport=full_transport)[
            "complete"
        ]
        == 1
    )
    assert load_histories(root)[1][0]["missing_rows"] == 0


def test_mainline_normalization_keeps_coverage_and_provenance(tmp_path):
    transport, _ = source()
    root, normalized = tmp_path / "source", tmp_path / "normalized"
    acquire(root, ["X-USDT-SWAP"], "5m", 0, 30 * FIVE, transport=transport)
    result = normalize_dataset(root, normalized)
    assert result["complete"] == 1
    assert load_histories(root)[0][0].bars == load_histories(normalized)[0][0].bars
    body = json.loads((normalized / "manifest.json").read_text())
    assert body["normalization"]["source_manifest_sha256"]
    with pytest.raises(FileExistsError):
        normalize_dataset(root, normalized)


def test_hash_tamper_and_shard_window_inconsistency_are_rejected(tmp_path):
    transport, _ = source()
    root = tmp_path / "source"
    acquire(root, ["X-USDT-SWAP"], "5m", 0, 30 * FIVE, transport=transport)
    body = json.loads((root / "manifest.json").read_text())
    body["products"][0]["shards"][0]["rows"] += 1
    (root / "manifest.json").write_text(json.dumps(body))
    with pytest.raises(Unavailable, match="coverage_mismatch"):
        load_histories(root)
