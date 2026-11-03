import gzip
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from msharesift import market as market_history
from msharesift import okx as data
from msharesift.model import Product

PRODUCT = Product("SWAP", "X-USDT-SWAP", "X", 1)


def response(rows):
    return httpx.Response(200, json={"code": "0", "data": rows})


def budget():
    clock = type("Clock", (), {"now": 0.0})()
    return data.Budget(clock=lambda: clock.now, sleep=lambda seconds: setattr(clock, "now", clock.now + seconds))


def test_trade_history_pagination_is_canonical_and_bounded():
    pages = {
        10_000: [{"instId": PRODUCT.inst_id, "tradeId": "3", "ts": "9_000", "side": "buy", "px": "2", "sz": "4", "source": "0"}],
        9_000: [{"instId": PRODUCT.inst_id, "tradeId": "2", "ts": "8_000", "side": "sell", "px": "1.9", "sz": "3", "source": "0"}],
        8_000: [{"instId": PRODUCT.inst_id, "tradeId": "1", "ts": "1_000", "side": "buy", "px": "1.8", "sz": "2", "source": "0"}],
    }
    def handle(request):
        return response(pages.get(int(request.url.params["after"]), []))

    with data.PublicClient(budget(), transport=httpx.MockTransport(handle)) as client:
        rows, quality = market_history.fetch_trades(client, PRODUCT, 1_000, 10_000)
    assert [row["trade_id"] for row in rows] == ["1", "2", "3"]
    assert quality["complete"] is True
    assert quality["pages"] == 3


def test_funding_history_normalizes_realized_rate():
    def handle(request):
        assert request.url.path == market_history.FUNDING_PATH
        after = int(request.url.params["after"])
        if after == 10_000:
            return response([{"instId": PRODUCT.inst_id, "fundingTime": "9_000",
                              "fundingRate": "0.001", "realizedRate": "0.0009",
                              "formulaType": "noRate", "method": "current_period"}])
        return response([])

    with data.PublicClient(budget(), transport=httpx.MockTransport(handle)) as client:
        rows, quality = market_history.fetch_funding(client, PRODUCT, 8_000, 10_000)
    assert rows[0]["funding_time"] == 9_000
    assert rows[0]["realized_rate"] == pytest.approx(0.0009)
    assert quality["complete"] is False


def test_trade_budget_keeps_explicit_partial_rows():
    def handle(request):
        return response([{"instId": PRODUCT.inst_id, "tradeId": "1", "ts": "9_000",
                          "side": "buy", "px": "2", "sz": "4", "source": "0"}])

    budget_limited = budget()
    budget_limited.max_requests = 1
    with data.PublicClient(budget_limited, transport=httpx.MockTransport(handle)) as client:
        rows, quality = market_history.fetch_trades(client, PRODUCT, 1_000, 10_000)
    assert len(rows) == 1
    assert quality["complete"] is False
    assert quality["error"] == "request_budget"


def test_open_interest_is_explicitly_point_in_time_only():
    def handle(request):
        assert request.url.path == market_history.OPEN_INTEREST_PATH
        return response([{"instId": PRODUCT.inst_id, "ts": "9_000", "oi": "10",
                          "oiCcy": "5", "oiUsd": "20"}])

    with data.PublicClient(budget(), transport=httpx.MockTransport(handle)) as client:
        rows, quality = market_history.fetch_open_interest(client, PRODUCT, 10_000)
    assert rows[0]["oi_usd"] == 20.0
    assert quality["history_supported"] is False
    assert quality["point_in_time_only"] is True


def test_dataset_hash_and_manifest_verification(tmp_path: Path):
    item = market_history._write_dataset(
        tmp_path, PRODUCT, "open_interest", 0, 10_000, 10_000,
        [{"inst_type": "SWAP", "inst_id": PRODUCT.inst_id, "ts": 9_000,
          "oi": 10.0, "oi_ccy": 5.0, "oi_usd": 20.0}],
        {"complete": True, "history_supported": False}, market_history.OPEN_INTEREST_PATH)
    manifest = {"schema": market_history.SCHEMA, "datasets": [item]}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert market_history.verify(tmp_path)["status"] == "complete"
    path = Path(item["path"])
    with gzip.open(path, "rb") as stream:
        body = json.loads(stream.read())
    body["rows"][0]["oi"] = 11.0
    path.write_bytes(gzip.compress(json.dumps(body).encode(), mtime=0))
    assert market_history.verify(tmp_path)["status"] == "failed"


def test_bulk_csv_parser_accepts_official_trade_schema():
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "X-USDT-SWAP-trades-2026-10-15.csv",
            "instrument_name,trade_id,side,price,size,created_time,source\n"
            "X-USDT-SWAP,1,buy,2.0,3,1000,0\n",
        )
    rows, quality = market_history._parse_bulk_zip(payload.getvalue(), PRODUCT, "trades")
    assert rows[0]["trade_id"] == "1"
    assert rows[0]["px"] == 2.0
    assert quality["complete"] is True


def test_bulk_csv_parser_accepts_official_funding_schema():
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "X-USDT-SWAP-fundingrates-2026-09.csv",
            "instrument_name,funding_rate,funding_time\n"
            "X-USDT-SWAP,0.001,1000\n",
        )
    rows, _ = market_history._parse_bulk_zip(payload.getvalue(), PRODUCT, "funding")
    assert rows[0]["funding_rate"] == pytest.approx(0.001)
    assert rows[0]["realized_rate"] is None


def test_oi_snapshot_and_quality_report(tmp_path: Path):
    item = market_history._write_oi_snapshot(
        tmp_path, 10_000,
        [{"inst_type": "SWAP", "inst_id": PRODUCT.inst_id, "ts": 10_100,
          "oi": 10.0, "oi_ccy": 5.0, "oi_usd": 20.0}],
        {"products_requested": 1, "products_with_rows": 1, "rows": 1},
    )
    (tmp_path / "oi-manifest.json").write_text(
        json.dumps({"schema": market_history.OI_SCHEMA, "snapshots": [item]}), encoding="utf-8")
    result = market_history.oi_report(tmp_path)
    assert result["status"] == "complete"
    assert result["rows"] == 1
    assert result["source_ts_lag_ms"]["min"] == 100
