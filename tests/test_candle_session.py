"""Cross-batch quotas include metadata/retries and cannot reset on resumption."""

import json

import pytest
from test_candle_parallel import server

from msharesift.candles.session import BatchBudget, SessionBudget, acquire_session
from msharesift.model import FIVE, Unavailable
from msharesift.okx import Budget, BudgetExceeded


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def plan(path, *, attempts=20, seconds=1200):
    value = {
        "budget": {
            "spent_seconds": 0,
            "spent_attempts": 0,
            "total_acquisition_seconds": seconds,
            "global_attempt_cap": attempts,
            "workers": 4,
        },
        "remaining_groups": [
            {
                "group_id": name,
                "grain": "5m",
                "start_ms": FIVE,
                "end_ms": 20 * FIVE,
                "inst_ids": [base + "-USDT"],
            }
            for name, base in [("g00", "A"), ("g01", "B")]
        ],
    }
    path.write_text(json.dumps(value))
    return path


def test_one_request_cap_includes_both_batches_and_resume(tmp_path):
    transport, calls = server()
    clock = Clock()
    source = plan(tmp_path / "plan.json", attempts=5)
    result = acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    assert len(calls) == result["spent_attempts"] == 5
    assert [g["status"] for g in result["groups"]] == ["complete", "partial"]
    again = acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    assert len(calls) == again["spent_attempts"] == 5
    assert again["status"] == "budget_exhausted"


def test_complete_session_resumes_without_any_new_requests(tmp_path):
    transport, calls = server()
    clock = Clock()
    source = plan(tmp_path / "plan.json")
    result = acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    assert result["status"] == "complete"
    assert result["spent_attempts"] == len(calls) == 6
    acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    assert len(calls) == 6
    value = json.loads(source.read_text())
    value["budget"]["global_attempt_cap"] = 21
    source.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="plan changed"):
        acquire_session(tmp_path / "data", source)


def test_batch_switch_preserves_global_clock_rates_and_retries():
    clock = Clock()
    session = SessionBudget(
        seconds=20, reserve=2, max_requests=10, clock=clock, sleep=clock.sleep
    )
    a = BatchBudget(session, max_requests=5, seconds=15)
    a.admit("/api/v5/market/history-candles")
    b = BatchBudget(session, max_requests=5, seconds=15)
    b.admit("/api/v5/market/history-candles", retry=True)
    assert clock.now == 0.125
    assert session.attempts == 2 and session.retries == 1
    clock.now = 18
    with pytest.raises(BudgetExceeded, match="deadline"):
        b.admit("/api/v5/public/time")
    assert session.attempts == 2


def test_base_budget_limits_and_batch_limits_remain_in_force():
    with pytest.raises(ValueError):
        Budget(seconds=1200, max_requests=4000)
    clock = Clock()
    session = SessionBudget(
        seconds=1200, max_requests=4000, clock=clock, sleep=clock.sleep
    )
    batch = BatchBudget(session, max_requests=1, seconds=480)
    batch.admit("/api/v5/public/time")
    with pytest.raises(BudgetExceeded, match="batch_request"):
        batch.admit("/api/v5/public/time")
    assert session.attempts == 1


def test_unclean_exit_cannot_silently_reset_time(tmp_path):
    transport, _ = server()
    clock = Clock()
    source = plan(tmp_path / "plan.json")
    acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    receipt = tmp_path / "data/session.json"
    value = json.loads(receipt.read_text())
    value["status"] = "running"
    receipt.write_text(json.dumps(value))
    with pytest.raises(Unavailable, match="unclean_exit"):
        acquire_session(tmp_path / "data", source)


def test_resumed_time_budget_does_not_restart_twenty_minutes(tmp_path):
    transport, calls = server()
    clock = Clock()
    source = plan(tmp_path / "plan.json")
    acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    receipt = tmp_path / "data/session.json"
    ledger = json.loads(receipt.read_text())
    ledger["status"] = "partial"
    ledger["spent_seconds"] = 1200
    ledger["groups"][1]["status"] = "deferred"
    receipt.write_text(json.dumps(ledger))
    result = acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    assert result["status"] == "budget_exhausted"
    assert len(calls) == 6
    assert result["spent_seconds"] == 1200


def test_resumed_byte_budget_cannot_reset(tmp_path):
    transport, calls = server()
    clock = Clock()
    source = plan(tmp_path / "plan.json")
    acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    receipt = tmp_path / "data/session.json"
    ledger = json.loads(receipt.read_text())
    ledger["status"] = "partial"
    ledger["received_bytes"] = 128 * 1024 * 1024
    ledger["groups"][1]["status"] = "deferred"
    receipt.write_text(json.dumps(ledger))
    result = acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    assert result["status"] == "budget_exhausted"
    assert len(calls) == 6


def test_complete_session_rechecks_shard_integrity_without_network(tmp_path):
    transport, calls = server()
    clock = Clock()
    source = plan(tmp_path / "plan.json")
    acquire_session(
        tmp_path / "data", source, transport=transport, clock=clock, sleep=clock.sleep
    )
    shard = next((tmp_path / "data/g00/shards").glob("*.parquet"))
    shard.write_bytes(shard.read_bytes() + b"corrupt")
    with pytest.raises(Unavailable, match="research_shard_hash"):
        acquire_session(tmp_path / "data", source, transport=transport)
    assert len(calls) == 6


def test_competing_session_owner_cannot_duplicate_global_quota(tmp_path):
    from msharesift.files import WriterLock

    source = plan(tmp_path / "plan.json")
    root = tmp_path / "data"
    with (
        WriterLock(root.with_suffix(".owner")).writer(),
        pytest.raises(Unavailable, match="writer_lock_present"),
    ):
        acquire_session(root, source)
    assert not (root / "session.json").exists()
