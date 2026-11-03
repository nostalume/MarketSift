"""All-member planning and one durable, bounded full-history request owner."""

import json

import httpx
import pytest

from msharesift.candles.acquire import acquire
from msharesift.candles.plan import full_history_plan, uncovered
from msharesift.candles.session import FullHistoryBudget, SessionBudget, acquire_session
from msharesift.candles.universe import inventory
from msharesift.model import HOUR, Unavailable
from msharesift.okx import Budget


def server(count=2):
    calls = []
    products = [
        {
            "instType": "SPOT",
            "instId": f"X{i}-USDT",
            "baseCcy": f"X{i}",
            "quoteCcy": "USDT",
            "instCategory": "1",
            "state": "live",
            "ruleType": "normal",
            "listTime": "1",
        }
        for i in range(count)
    ]

    def handle(request):
        calls.append(request)
        path = request.url.path
        if path.endswith("/time"):
            data = [{"ts": str(50 * HOUR + 2000)}]
        elif path.endswith("/instruments"):
            data = products if request.url.params["instType"] == "SPOT" else []
        else:
            assert request.url.params["bar"] == "1H"
            cursor = int(request.url.params["after"])
            limit = int(request.url.params["limit"])
            data = [
                [str(t), "100", "102", "99", "101", "1", "1", "100", "1"]
                for t in range(
                    cursor - HOUR, max(-HOUR, cursor - (limit + 1) * HOUR), -HOUR
                )
            ]
        return httpx.Response(200, json={"code": "0", "data": data})

    return httpx.MockTransport(handle), calls


def test_ordinary_budget_caps_unchanged():
    with pytest.raises(ValueError):
        Budget(seconds=481)
    with pytest.raises(ValueError):
        SessionBudget(max_requests=4001)
    with pytest.raises(ValueError):
        SessionBudget(max_bytes=129 * 1024**2)
    b = FullHistoryBudget(max_requests=80000, seconds=21600, max_bytes=4 * 1024**3)
    assert b.max_requests == 80000


def test_interval_union_preserves_uncovered_edges():
    assert uncovered(10, 100, [(0, 30), (20, 40), (60, 70), (70, 90), (200, 300)]) == [
        (40, 60),
        (90, 100),
    ]


def test_full_census_not_cut_at100_and_no_five_minute(tmp_path):
    t, calls = server(151)
    inventory(tmp_path / "inventory", end=20 * HOUR, transport=t)
    report = full_history_plan(
        tmp_path / "plan", tmp_path / "inventory", max_requests=1000, seconds=100
    )
    plan = json.loads((tmp_path / "plan/plan.json").read_text())
    ids = [i for g in plan["remaining_groups"] for i in g["inst_ids"]]
    assert len(set(ids)) == report["products"] == 151
    assert all(
        g["grain"] == "1h" and g["start_ms"] == HOUR for g in plan["remaining_groups"]
    )
    assert len(calls) == 3  # planning itself performs no market I/O


def test_complete_source_shards_reused_without_hiding_partial_gaps(tmp_path):
    t, _ = server(1)
    inventory(tmp_path / "inventory", end=20 * HOUR, transport=t)
    acquire(
        tmp_path / "reuse",
        ["X0-USDT"],
        "1h",
        HOUR,
        10 * HOUR,
        transport=t,
        universe="research",
    )
    body = json.loads((tmp_path / "reuse/manifest.json").read_text())
    body["products"][0]["status"] = "partial"
    (tmp_path / "reuse/manifest.json").write_text(json.dumps(body))
    report = full_history_plan(
        tmp_path / "plan",
        tmp_path / "inventory",
        reuse_roots=[tmp_path / "reuse"],
        max_requests=20,
        seconds=100,
    )
    p = json.loads((tmp_path / "plan/plan.json").read_text())
    assert report["reused_rows"] == 9 and report["missing_rows_upper"] == 10
    assert p["remaining_groups"][0]["start_ms"] == 10 * HOUR
    result = acquire_session(
        tmp_path / "session", tmp_path / "plan/plan.json", transport=t
    )
    assert result["status"] == "complete"


def test_parallel_groups_share_quota_and_resume_cannot_reset(tmp_path):
    t, calls = server(5)
    inventory(tmp_path / "inventory", end=20 * HOUR, transport=t)
    full_history_plan(
        tmp_path / "plan", tmp_path / "inventory", max_requests=5, seconds=100
    )
    before = len(calls)
    result = acquire_session(
        tmp_path / "session", tmp_path / "plan/plan.json", transport=t
    )
    assert result["spent_attempts"] == len(calls) - before == 5
    result2 = acquire_session(
        tmp_path / "session", tmp_path / "plan/plan.json", transport=t
    )
    assert result2["status"] == "budget_exhausted"
    assert len(calls) - before == 5


def test_full_complete_resume_is_network_free_and_hash_bound(tmp_path):
    t, calls = server(2)
    inventory(tmp_path / "inventory", end=20 * HOUR, transport=t)
    full_history_plan(
        tmp_path / "plan", tmp_path / "inventory", max_requests=30, seconds=100
    )
    result = acquire_session(
        tmp_path / "session", tmp_path / "plan/plan.json", transport=t
    )
    assert result["status"] == "complete"
    count = len(calls)
    acquire_session(tmp_path / "session", tmp_path / "plan/plan.json", transport=t)
    assert len(calls) == count
    p = tmp_path / "inventory/manifest.json"
    p.write_bytes(p.read_bytes() + b" ")
    with pytest.raises(Unavailable, match="frozen_inventory_changed"):
        acquire_session(tmp_path / "session", tmp_path / "plan/plan.json", transport=t)
    assert len(calls) == count


def test_capacity_sample_has_known_inclusion_probability_and_hard_cost_bound(tmp_path):
    t, calls = server(151)
    inventory(tmp_path / "inventory", end=20 * HOUR, transport=t)
    full_history_plan(
        tmp_path / "sample",
        tmp_path / "inventory",
        max_requests=20,
        seconds=100,
        sample_requests=20,
    )
    plan = json.loads((tmp_path / "sample/plan.json").read_text())
    design = plan["sample_design"]
    assert design["request_upper_bound"] <= 20
    assert 0 < len(design["selected_products"]) < 151
    stratum = design["strata"][0]
    assert stratum["inclusion_probability"] == stratum["sample"] / 151
    full_history_plan(
        tmp_path / "sample2",
        tmp_path / "inventory",
        max_requests=20,
        seconds=100,
        sample_requests=20,
    )
    other = json.loads((tmp_path / "sample2/plan.json").read_text())
    assert design["selected_products"] == other["sample_design"]["selected_products"]
    assert len(calls) == 3


def test_capacity_sample_rejects_long_or_unfunded_envelopes(tmp_path):
    t, _ = server(2)
    inventory(tmp_path / "inventory", end=20 * HOUR, transport=t)
    with pytest.raises(ValueError, match="20minute"):
        full_history_plan(
            tmp_path / "sample",
            tmp_path / "inventory",
            seconds=1201,
            sample_requests=20,
        )
    with pytest.raises(ValueError, match="one probability sample"):
        full_history_plan(
            tmp_path / "sample",
            tmp_path / "inventory",
            max_requests=2,
            seconds=100,
            sample_requests=2,
        )
