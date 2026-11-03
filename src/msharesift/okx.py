"""Bounded public OKX requests and incremental complete-candle acquisition."""
from __future__ import annotations

import json
import math
import socket
import threading
import time
from dataclasses import dataclass, field
from types import MappingProxyType

import httpx

from msharesift.model import FIVE, HOUR, MINUTE, Product, Series, Unavailable

PATHS = frozenset({"/api/v5/public/time", "/api/v5/public/instruments",
                   "/api/v5/public/open-interest", "/api/v5/public/funding-rate-history",
                   "/api/v5/public/mark-price", "/api/v5/market/index-tickers",
                   "/api/v5/market/tickers", "/api/v5/market/candles",
                   "/api/v5/market/history-candles", "/api/v5/market/history-trades",
                   "/priapi/v5/broker/public/trade-data/download-link"})

GRAINS = {"minute": MINUTE, "five": FIVE, "hourly": HOUR}

RETENTION = {MINUTE: 24 * HOUR + 6 * MINUTE, FIVE: 8 * 24 * HOUR + 3 * FIVE, HOUR: 192 * HOUR}

class BudgetExceeded(RuntimeError):
    pass

def _transport_reason(exc: BaseException) -> str:
    """Keep the httpx exception name while exposing the actionable class."""
    cause = exc
    seen = set()
    kind = "connection"
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        if isinstance(cause, socket.gaierror):
            kind = "dns"
            break
        if isinstance(cause, (httpx.TimeoutException, TimeoutError)):
            kind = "timeout"
        cause = cause.__cause__ or cause.__context__
    return f"transport:{type(exc).__name__}:{kind}"

@dataclass
class Budget:
    """One shared admission/rate/byte owner for initial requests AND retries.

    Deadline is cooperative, not a hard process watchdog (notably DNS).
    All locks are released before network I/O; response chunks are accounted.
    """
    REQUEST_LIMIT = 2400
    SECOND_LIMIT = 480
    BYTE_LIMIT = 128 * 1024 * 1024

    max_requests: int = 2400
    max_bytes: int = 128 * 1024 * 1024
    max_response: int = 4 * 1024 * 1024
    seconds: float = 480
    reserve: float = 60
    clock: object = time.monotonic
    sleep: object = time.sleep
    attempts: int = 0
    initial: int = 0
    retries: int = 0
    received: int = 0
    observations: list[dict] = field(default_factory=list)

    def __post_init__(self):
        if not (0 < self.max_requests <= self.REQUEST_LIMIT and 0 < self.max_bytes <= self.BYTE_LIMIT
                and 0 < self.max_response <= 4 * 1024 * 1024 and 0 <= self.reserve < self.seconds <= self.SECOND_LIMIT):
            raise ValueError("Invalid bounded budget")
        self.started = self.clock()
        self.last_success_at = None
        self.lock = threading.Lock()
        self.next_market = self.next_public = self.backoff_until = self.started

    def remaining(self):
        return self.started + self.seconds - self.reserve - self.clock()

    def admit(self, path: str, retry: bool = False):
        if path not in PATHS:
            raise ValueError("Only allowlisted public endpoints")
        market = "/market/" in path
        with self.lock:
            if self.received >= self.max_bytes:
                raise BudgetExceeded("byte_budget")
            if self.attempts >= self.max_requests:
                raise BudgetExceeded("request_budget")
            if retry and self.retries >= max(2, self.initial // 5):
                raise BudgetExceeded("retry_budget")
            due = max(self.backoff_until, self.next_market if market else self.next_public)
            wait = max(0, due - self.clock())
            if self.remaining() <= wait:
                raise BudgetExceeded("acquisition_deadline")
            self.sleep(wait)
            if self.remaining() <= 0:
                raise BudgetExceeded("acquisition_deadline")
            now = self.clock()
            if market:
                self.next_market = now + .125
            else:
                self.next_public = now + .25
            self.attempts += 1
            self.retries += int(retry)
            self.initial += int(not retry)

    def charge(self, amount: int):
        with self.lock:
            self.received += amount
            if self.received > self.max_bytes:
                raise BudgetExceeded("byte_budget")
        if self.remaining() <= 0:
            raise BudgetExceeded("acquisition_deadline")

    def backoff(self, seconds: float):
        with self.lock:
            self.backoff_until = max(self.backoff_until, self.clock() + seconds)

    def observe(self, record: dict):
        with self.lock:
            self.observations.append(record)
            if record['status'] == 200 and record['failure'] is None:
                self.last_success_at = self.clock()

class PublicClient:
    """A single worker owns this HTTP/1.1 connection pool for its entire batch."""
    def __init__(self, budget: Budget, *, transport=None):
        self.budget = budget
        self.client = httpx.Client(
            base_url="https://www.okx.com", transport=transport, follow_redirects=False,
            trust_env=False, http2=False, limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
            headers={"User-Agent": "crypto-boom-public-shadow/1", "Accept-Encoding": "identity"})

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.client.close()

    def get(self, path: str, params: dict | None = None) -> list:
        if path not in PATHS:
            raise ValueError("Only allowlisted public GET")
        for attempt in range(3):
            self.budget.admit(path, retry=attempt > 0)
            started = self.budget.clock()
            size, status, failure = 0, None, None
            retry_delay = None
            try:
                timeout = httpx.Timeout(max(.001, min(5, self.budget.remaining())))
                with self.client.stream("GET", path, params=params, timeout=timeout) as response:
                    status = response.status_code
                    if response.headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
                        raise Unavailable("unexpected_content_encoding")
                    payload = bytearray()
                    for chunk in response.iter_bytes(chunk_size=16384):
                        size += len(chunk)
                        self.budget.charge(len(chunk))
                        if size > self.budget.max_response:
                            raise BudgetExceeded("response_budget")
                        payload.extend(chunk)
                    if status == 429 or status in (500, 502, 503, 504):
                        retry_delay = 2 ** attempt
                        header = response.headers.get("Retry-After")
                        if header:
                            try:
                                retry_delay = max(retry_delay, float(header))
                            except ValueError:
                                pass
                        if not math.isfinite(retry_delay) or retry_delay < 0:
                            retry_delay = 2 ** attempt
                        self.budget.backoff(retry_delay)
                        raise Unavailable("retryable_http:" + str(status))
                    if status != 200:
                        raise Unavailable("http:" + str(status))
                    try:
                        body = json.loads(payload)
                    except (ValueError, UnicodeError) as exc:
                        raise Unavailable("invalid_json") from exc
                    if not isinstance(body, dict) or body.get("code") != "0" or not isinstance(body.get("data"), list):
                        raise Unavailable("okx_business_or_schema_error")
                    return body["data"]
            except httpx.TransportError as exc:
                failure = _transport_reason(exc)
                retry_delay = 2 ** attempt
                if attempt == 2:
                    raise Unavailable(failure) from exc
                # A broken connection belongs to this worker, not every public request.
                self.budget.sleep(min(retry_delay, max(0, self.budget.remaining())))
            except (Unavailable, BudgetExceeded) as exc:
                failure = str(exc)
                if retry_delay is None or attempt == 2 or isinstance(exc, BudgetExceeded):
                    raise
            finally:
                self.budget.observe({"path": path, "status": status, "bytes": size,
                                     "seconds": self.budget.clock() - started, "failure": failure})
        raise AssertionError("Unreachable")

    def post(self, path: str, payload: dict) -> dict:
        """Bounded public JSON POST for the official bulk-data link endpoint."""
        if path not in PATHS or not isinstance(payload, dict):
            raise ValueError("Only allowlisted public POST")
        for attempt in range(3):
            self.budget.admit(path, retry=attempt > 0)
            started = self.budget.clock()
            size, status, failure = 0, None, None
            retry_delay = None
            try:
                timeout = httpx.Timeout(max(.001, min(5, self.budget.remaining())))
                with self.client.stream("POST", path, json=payload, timeout=timeout) as response:
                    status = response.status_code
                    if response.headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
                        raise Unavailable("unexpected_content_encoding")
                    body_bytes = bytearray()
                    for chunk in response.iter_bytes(chunk_size=16384):
                        size += len(chunk)
                        self.budget.charge(len(chunk))
                        if size > self.budget.max_response:
                            raise BudgetExceeded("response_budget")
                        body_bytes.extend(chunk)
                    if status == 429 or status in (500, 502, 503, 504):
                        retry_delay = 2 ** attempt
                        header = response.headers.get("Retry-After")
                        if header:
                            try:
                                retry_delay = max(retry_delay, float(header))
                            except ValueError:
                                pass
                        self.budget.backoff(retry_delay)
                        raise Unavailable("retryable_http:" + str(status))
                    if status != 200:
                        raise Unavailable("http:" + str(status))
                    try:
                        body = json.loads(body_bytes)
                    except (ValueError, UnicodeError) as exc:
                        raise Unavailable("invalid_json") from exc
                    if not isinstance(body, dict) or body.get("code") != "0" or not isinstance(body.get("data"), dict):
                        raise Unavailable("okx_business_or_schema_error")
                    return body["data"]
            except httpx.TransportError as exc:
                failure = _transport_reason(exc)
                retry_delay = 2 ** attempt
                if attempt == 2:
                    raise Unavailable(failure) from exc
                self.budget.sleep(min(retry_delay, max(0, self.budget.remaining())))
            except (Unavailable, BudgetExceeded) as exc:
                failure = str(exc)
                if retry_delay is None or attempt == 2 or isinstance(exc, BudgetExceeded):
                    raise
            finally:
                self.budget.observe({"path": path, "status": status, "bytes": size,
                                     "seconds": self.budget.clock() - started, "failure": failure})
        raise AssertionError("Unreachable")

def rows_of(series: Series) -> list[list]:
    return [[b.ts, b.open, b.high, b.low, b.close, b.volume, b.volume_ccy, b.quote, "1"]
            for _, b in sorted(series.bars.items())]

def _trim_series(series: Series, at: int) -> Series:
    """Crop already-admitted candles without interpreting their values again."""
    lower = (at // series.grain) * series.grain - RETENTION[series.grain]
    errors = {t: e for t, e in series.errors.items() if lower <= t and t + series.grain <= at}
    bars = {t: b for t, b in series.bars.items()
            if lower <= t and t + series.grain <= at and t not in errors}
    return Series(series.grain, MappingProxyType(bars), MappingProxyType(errors))

def merge_series(old: Series, rows: list[list], at: int, *, trim: bool = True) -> Series:
    merged = Series.parse(rows_of(old) + rows, old.grain)
    errors = dict(old.errors)
    errors.update(merged.errors)  # A conflict stays unavailable until it ages out.
    result = Series(old.grain, merged.bars, MappingProxyType(errors))
    return _trim_series(result, at) if trim else result

def fetch_range(client: PublicClient, product: Product, old: Series, start: int, at: int,
                *, end: int | None = None, retain_partial=None, trim: bool = True) -> Series:
    """Fetch [start, end), keeping source cutoff/retention at the real ``at``.

    On interrupted acquisition, an opted-in recovery consumer receives validated
    partial history once; the original failure still propagates. This is cache
    progress, never evidence that the range or replay is complete.
    """
    grain = old.grain
    end = (at // grain) * grain if end is None else end
    if (start < 0 or start % grain or at % MINUTE or start >= end
            or end % grain or end > at):
        raise ValueError("Invalid candle range")
    name = {MINUTE: "1m", FIVE: "5m", HOUR: "1H"}[grain]
    cursor = end
    received = []
    try:
        while cursor > start:
            path = "/api/v5/market/candles" if start >= at - 1440 * grain else "/api/v5/market/history-candles"
            limit = min(300, max(2, math.ceil((cursor - start) / grain)))
            page = client.get(path, {"instId": product.inst_id, "bar": name, "after": str(cursor), "limit": str(limit)})
            if not page:
                break
            Series.parse(page, grain)
            # Include confirm=0 timestamps for cursor progress, never cache them.
            timestamps = [int(row[0]) for row in page]
            if any(t >= cursor for t in timestamps):
                raise Unavailable("pagination_not_backward")
            received.extend(row for row in page if start <= int(row[0]) and int(row[0]) + grain <= end)
            cursor = min(timestamps)
            if len(page) < limit:
                break  # Source shortage is a gap, never invented history.
    except (Unavailable, BudgetExceeded):
        if received and retain_partial is not None:
            partial = merge_series(old, received, at, trim=trim)
            if partial.bars != old.bars or partial.errors != old.errors:
                retain_partial(partial)
        raise
    return merge_series(old, received, at, trim=trim)
