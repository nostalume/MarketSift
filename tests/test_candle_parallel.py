"""Parallel history acquisition preserves canonical identity and one shared budget."""

import json

import httpx

from msharesift.candles.acquire import acquire
from msharesift.candles.history import load_histories
from msharesift.model import FIVE, HOUR


def server():
    calls = []
    products = [
        {
            "instType": "SPOT",
            "instId": base + "-USDT",
            "baseCcy": base,
            "quoteCcy": "USDT",
            "instCategory": "1",
            "state": "live",
            "ruleType": "normal",
            "listTime": "1",
        }
        for base in ["A", "B", "C", "D"]
    ]

    def handle(request):
        calls.append(request)
        if request.url.path.endswith("/time"):
            data = [{"ts": str(1000 * HOUR + 2000)}]
        elif request.url.path.endswith("/instruments"):
            data = products
        else:
            end = int(request.url.params["after"])
            limit = int(request.url.params["limit"])
            data = [
                [str(t), "100", "102", "99", "101", "1", "1", "100", "1"]
                for t in range(end - FIVE, max(-FIVE, end - (limit + 1) * FIVE), -FIVE)
            ]
        return httpx.Response(200, json={"code": "0", "data": data})

    return httpx.MockTransport(handle), calls


def test_parallel_matches_serial_and_resume_can_change_workers(tmp_path):
    ids = [x + "-USDT" for x in "ABCD"]
    transport, calls = server()
    a = acquire(
        tmp_path / "serial", ids, "5m", 0, 20 * FIVE, transport=transport, workers=1
    )
    b = acquire(
        tmp_path / "parallel", ids, "5m", 0, 20 * FIVE, transport=transport, workers=4
    )
    assert a["complete"] == b["complete"] == 4
    h1, _ = load_histories(tmp_path / "serial")
    h2, _ = load_histories(tmp_path / "parallel")
    assert [(h.product.key, h.bars) for h in h1] == [
        (h.product.key, h.bars) for h in h2
    ]
    count = len(calls)
    c = acquire(
        tmp_path / "parallel", ids, "5m", 0, 20 * FIVE, transport=transport, workers=1
    )
    assert c["complete"] == 4 and len(calls) == count + 2


def test_parallel_uses_one_budget_and_private_clients(tmp_path, monkeypatch):
    import msharesift.candles.acquire as owner

    real = owner.PublicClient
    owners = []

    def factory(budget, **kwargs):
        client = real(budget, **kwargs)
        owners.append((id(budget), id(client.client)))
        return client

    monkeypatch.setattr(owner, "PublicClient", factory)
    transport, _ = server()
    acquire(
        tmp_path / "data",
        [x + "-USDT" for x in "ABCD"],
        "5m",
        0,
        20 * FIVE,
        transport=transport,
        workers=4,
    )
    assert len({b for b, _ in owners}) == 1
    assert (
        len({c for _, c in owners}) == 5
    )  # Metadata owner + one independent pool per product.


def test_shared_request_cap_preserves_partial_members(tmp_path):
    transport, calls = server()
    result = acquire(
        tmp_path / "data",
        [x + "-USDT" for x in "ABCD"],
        "5m",
        0,
        20 * FIVE,
        transport=transport,
        workers=4,
        max_requests=3,
    )
    assert result["requests"] == len(calls) == 3
    manifest = json.loads((tmp_path / "data/manifest.json").read_text())
    assert len(manifest["products"]) == 4
    assert result["complete"] == 1
    assert sum(p["status"] == "partial" for p in manifest["products"]) == 3
