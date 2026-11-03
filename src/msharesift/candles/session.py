"""One persisted acquisition budget across fixed-window canonical batches."""

from __future__ import annotations

import copy
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from msharesift.candles.acquire import SOURCE_GRAINS, acquire, write_json
from msharesift.candles.history import digest_file, load_histories
from msharesift.files import WriterLock
from msharesift.model import Unavailable
from msharesift.okx import Budget, BudgetExceeded


class SessionBudget(Budget):
    """Explicit offline session; ordinary request owners keep their 480s limits."""

    REQUEST_LIMIT = 4000
    SECOND_LIMIT = 1200

    def __init__(self, *, checkpoint=None, **kwargs):
        self.checkpoint = checkpoint
        self.persistence_lock = threading.Lock()
        super().__init__(**kwargs)

    def admit(self, path, retry=False):
        # Serialize reservation AND durable recording before any worker sends I/O.
        with self.persistence_lock:
            super().admit(path, retry)
            if self.checkpoint is not None:
                self.checkpoint()


class FullHistoryBudget(SessionBudget):
    """Explicit all-inventory profile; ordinary sessions retain their old caps."""

    REQUEST_LIMIT = 100000
    SECOND_LIMIT = 43200
    BYTE_LIMIT = 8 * 1024**3


def budget_type(plan):
    profile = plan.get("budget_profile", "standard")
    if profile == "standard":
        return SessionBudget
    if profile == "full_history" and plan.get("schema") == "canonical-full-history-plan-v1":
        return FullHistoryBudget
    raise ValueError("unknown acquisition budget profile")


class BatchBudget(Budget):
    """A bounded batch borrows the session's rates, retry state and total quotas."""

    def __init__(self, session, *, max_requests, seconds):
        self.session = session
        super().__init__(
            max_requests=max_requests,
            seconds=seconds,
            reserve=min(60, seconds / 4),
            clock=session.clock,
            sleep=session.sleep,
        )

    def remaining(self):
        return min(super().remaining(), self.session.remaining())

    def admit(self, path, retry=False):
        with self.lock:
            if self.attempts >= self.max_requests:
                raise BudgetExceeded("batch_request_budget")
            if self.remaining() <= 0:
                raise BudgetExceeded("batch_acquisition_deadline")
            self.session.admit(path, retry)
            self.attempts += 1
            self.initial += int(not retry)
            self.retries += int(retry)

    def charge(self, amount):
        self.session.charge(amount)
        super().charge(amount)

    def backoff(self, seconds):
        self.session.backoff(seconds)

    def observe(self, record):
        self.session.observe(record)
        super().observe(record)


def _validate(plan):
    budget = plan["budget"]
    limits = budget_type(plan)
    spent = budget["spent_seconds"]
    attempts = budget["spent_attempts"]
    if not (
        0 <= spent < budget["total_acquisition_seconds"] <= limits.SECOND_LIMIT
        and 0 <= attempts < budget["global_attempt_cap"] <= limits.REQUEST_LIMIT
        and budget["workers"] in (1, 4)
        and 0 < budget.get("max_bytes", 128 * 1024**2) <= limits.BYTE_LIMIT
    ):
        raise ValueError("invalid session budget")
    identities = set()
    for group in plan["remaining_groups"]:
        grain = SOURCE_GRAINS[group["grain"]]
        ids = group["inst_ids"]
        if not (
            group["group_id"].isalnum()
            and group["group_id"] not in identities
            and 0 < len(ids) <= 100
            and len(ids) == len(set(ids))
            and 0 <= group["start_ms"] < group["end_ms"]
            and group["start_ms"] % grain == group["end_ms"] % grain == 0
        ):
            raise ValueError("invalid fixed group contract")
        identities.add(group["group_id"])


def _acquire_session(
    root, plan_path, *, transport=None, clock=time.monotonic, sleep=time.sleep
):
    """Resume only the same plan, retaining spent time/requests and every member.

    A process killed while marked running is refused on restart: elapsed time
    cannot be recovered safely from a stale checkpoint. It needs audited recovery,
    rather than silently receiving another twenty minutes.
    """
    root, plan_path = Path(root), Path(plan_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8-sig"))
    _validate(plan)
    plan_hash = digest_file(plan_path)
    full_history = budget_type(plan) is FullHistoryBudget
    if full_history:
        inventory = plan["inventory"]
        if digest_file(Path(inventory["root"]) / "manifest.json") != inventory["sha256"]:
            raise Unavailable("frozen_inventory_changed")
        if any(g["grain"] != "1h" for g in plan["remaining_groups"]):
            raise ValueError("full-history profile admits1h source only")
    ledger_path = root / "session.json"
    if ledger_path.exists():
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        if ledger["plan_sha256"] != plan_hash:
            raise ValueError("session plan changed")
        if ledger["status"] == "running":
            raise Unavailable("session_unclean_exit_requires_audited_recovery")
    else:
        if root.exists() and any(root.iterdir()):
            raise ValueError("refuse nonempty session without ledger")
        root.mkdir(parents=True, exist_ok=True)
        ledger = {
            "schema": "canonical-candle-session-v1",
            "plan_sha256": plan_hash,
            "status": "pending",
            "spent_seconds": plan["budget"]["spent_seconds"],
            "spent_attempts": plan["budget"]["spent_attempts"],
            "received_bytes": 0,
            "retries": 0,
            "groups": [
                {
                    "group_id": g["group_id"],
                    "status": "deferred",
                    "inst_ids": g["inst_ids"],
                }
                for g in plan["remaining_groups"]
            ],
            "reuse": copy.deepcopy(plan.get("reuse", [])),
        }
    # Validate reused source receipts without copying/cleaning in research.
    for reuse in ledger["reuse"]:
        if (
            digest_file(Path(reuse["root"]) / "manifest.json")
            != reuse["manifest_sha256"]
        ):
            raise Unavailable("reused_manifest_changed")
        _, audit = load_histories(reuse["root"])
        if not (full_history and reuse.get("coverage_admission") == "complete_hashed_shards_only") and any(row.get("status") != "complete" for row in audit):
            raise Unavailable("reused_dataset_incomplete")
    for group, state in zip(plan["remaining_groups"], ledger["groups"], strict=True):
        if state["status"] != "complete":
            continue
        histories, audit = load_histories(root / group["group_id"])
        if (
            {h.product.inst_id for h in histories} != set(group["inst_ids"])
            or any(
                h.start != group["start_ms"]
                or h.end != group["end_ms"]
                or h.grain != SOURCE_GRAINS[group["grain"]]
                for h in histories
            )
            or any(
                row.get("status") != "complete" or row.get("missing_rows", 0)
                for row in audit
            )
        ):
            raise Unavailable("completed_group_integrity_or_contract_changed")
    if ledger["status"] == "complete":
        return ledger
    b = plan["budget"]
    previous_seconds, previous_attempts = (
        ledger["spent_seconds"],
        ledger["spent_attempts"],
    )
    previous_bytes, previous_retries = ledger["received_bytes"], ledger["retries"]
    byte_cap = b.get("max_bytes", 128 * 1024**2)
    if (
        previous_seconds >= b["total_acquisition_seconds"]
        or previous_attempts >= b["global_attempt_cap"]
        or previous_bytes >= byte_cap
    ):
        ledger["status"] = "budget_exhausted"
        write_json(ledger_path, ledger)
        return ledger
    session = budget_type(plan)(
        max_requests=b["global_attempt_cap"] - previous_attempts,
        max_bytes=byte_cap - previous_bytes,
        seconds=b["total_acquisition_seconds"] - previous_seconds,
        reserve=min(60, (b["total_acquisition_seconds"] - previous_seconds) / 4),
        clock=clock,
        sleep=sleep,
    )

    publication_lock = threading.RLock()

    def checkpoint():
        with publication_lock:
            checkpoint_locked()

    def checkpoint_locked():
        ledger.update(
            spent_seconds=previous_seconds + clock() - session.started,
            spent_attempts=previous_attempts + session.attempts,
            received_bytes=previous_bytes + session.received,
            retries=previous_retries + session.retries,
        )
        write_json(ledger_path, ledger)

    session.checkpoint = checkpoint
    ledger["status"] = "running"
    checkpoint()
    try:
        def run_group(pair):
            group, state = pair
            if state["status"] == "complete":
                return
            with publication_lock:
                if session.remaining() <= 0 or session.attempts >= session.max_requests:
                    ledger["stop_reason"] = "global_budget_exhausted"
                    return
                state["status"] = "running"
                checkpoint()
            try:
                result = acquire(
                    root / group["group_id"],
                    group["inst_ids"],
                    group["grain"],
                    group["start_ms"],
                    group["end_ms"],
                    max_requests=2400,
                    seconds=min(480, max(1, session.remaining())),
                    universe="research",
                    workers=1 if full_history else b["workers"],
                    transport=transport,
                    session_budget=session,
                )
                with publication_lock:
                    state.update(
                        status="complete" if result["complete"] == len(group["inst_ids"]) else "partial",
                        result=result,
                    )
            except (BudgetExceeded, Unavailable) as exc:
                with publication_lock:
                    state.update(status="partial", reason=str(exc))
                    if isinstance(exc, BudgetExceeded):
                        ledger["stop_reason"] = str(exc)
            with publication_lock:
                checkpoint()
                if full_history:
                    print(json.dumps({"group": group["group_id"], "status": state["status"], "requests": ledger["spent_attempts"], "seconds": round(ledger["spent_seconds"], 1)}), flush=True)

        pairs = list(zip(plan["remaining_groups"], ledger["groups"], strict=True))
        if full_history:
            # Group-level concurrency avoids serial singleton listing-date groups.
            # The shared SessionBudget still owns every request/rate/retry/byte.
            with ThreadPoolExecutor(max_workers=b["workers"]) as pool:
                list(pool.map(run_group, pairs))
        else:
            for pair in pairs:
                run_group(pair)
                if ledger.get("stop_reason"):
                    break
        ledger["status"] = (
            "complete"
            if all(g["status"] == "complete" for g in ledger["groups"])
            else "partial"
        )
    except BaseException:
        ledger["status"] = "interrupted"
        raise
    finally:
        checkpoint()
    return ledger


def acquire_session(
    root, plan_path, *, transport=None, clock=time.monotonic, sleep=time.sleep
):
    """One exclusive local owner; a competing process cannot duplicate quotas."""
    with WriterLock(Path(root).with_suffix(".owner")).writer():
        return _acquire_session(
            root, plan_path, transport=transport, clock=clock, sleep=sleep
        )
