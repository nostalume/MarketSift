"""Public acquisition budgets and pagination; fixture HTTP only."""
import gzip
from types import MappingProxyType

import httpx
import pytest
from test_model import PRODUCT, T, row

from msharesift import model as trial
from msharesift import okx as data


class Clock:
    def __init__(self):
        self.now = 0.
    def clock(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds

def budget(**kwargs):
    clock = Clock()
    return data.Budget(clock=clock.clock, sleep=clock.sleep, **kwargs), clock

def empty(grain):
    return trial.Series(grain, MappingProxyType({}), MappingProxyType({}))

def response(rows):
    return httpx.Response(200, json={"code": "0", "data": rows})

def test_rate_and_global_retry_bound():
    b, clock = budget()
    for _ in range(16):
        b.admit("/api/v5/market/candles")
    assert clock.now == 15 / 8
    b.admit("/api/v5/market/history-candles")
    assert clock.now == 2.
    for _ in range(3):
        b.admit("/api/v5/market/history-candles", retry=True)
    with pytest.raises(data.BudgetExceeded, match="retry_budget"):
        b.admit("/api/v5/market/candles", retry=True)
    assert b.retries <= b.initial // 5

def test_budget_refuses_after_deadline_or_count():
    b, clock = budget(max_requests=1)
    b.admit("/api/v5/public/time")
    with pytest.raises(data.BudgetExceeded, match="request_budget"):
        b.admit("/api/v5/public/time")
    b, clock = budget(seconds=2, reserve=1)
    clock.now = 1
    with pytest.raises(data.BudgetExceeded, match="deadline"):
        b.admit("/api/v5/public/time")

@pytest.mark.parametrize("status", [301, 403, 451])
def test_no_redirect_auth_or_region_retry(status):
    b, _ = budget()
    with data.PublicClient(b, transport=httpx.MockTransport(lambda r: httpx.Response(status))) as client:
        with pytest.raises(trial.Unavailable, match="http"):
            client.get("/api/v5/public/time")
    assert b.attempts == 1 and client.client.is_closed

def test_allowlist_rejects_account_before_effect():
    b, _ = budget()
    with data.PublicClient(b) as client:
        with pytest.raises(ValueError):
            client.get("/api/v5/account/balance")
    assert b.attempts == 0

def test_429_shared_backoff_and_transport_retry():
    b, clock = budget()
    attempts = []
    def handle(request):
        attempts.append(clock.now)
        if len(attempts) == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        if len(attempts) == 2:
            raise httpx.ReadError("fixture")
        return response([])
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        assert client.get("/api/v5/market/candles") == []
    assert attempts[1] - attempts[0] >= 3 and attempts[2] - attempts[1] >= 2
    assert b.retries == 2 and len(b.observations) == 3

def test_startup_transport_retry_is_local_and_remains_bounded():
    b, clock = budget()
    calls = []
    def handle(request):
        calls.append(clock.now)
        raise httpx.ConnectError('fixture offline', request=request)
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(trial.Unavailable, match='transport:ConnectError'):
            client.get('/api/v5/public/time')
    assert len(calls) == 3 and b.retries == 2 and b.attempts == 3
    assert b.backoff_until == b.started  # Connection failure must not pause other workers.
    with pytest.raises(data.BudgetExceeded, match='retry_budget'):
        b.admit('/api/v5/public/time', retry=True)

@pytest.mark.parametrize("kwargs,reason", [({"max_response": 10}, "response_budget"), ({"max_bytes": 10}, "byte_budget")])
def test_response_and_total_byte_caps(kwargs, reason):
    b, _ = budget(**kwargs)
    with data.PublicClient(b, transport=httpx.MockTransport(lambda r: response(["a" * 100]))) as client:
        with pytest.raises(data.BudgetExceeded, match=reason):
            client.get("/api/v5/public/time")
    assert client.client.is_closed

def test_native_pagination_and_missing_confirm():
    at = 2000 * trial.MINUTE
    rows = [row(at - i * trial.MINUTE, confirm="0" if i == 1 else "1") for i in range(1, 1502)]
    requests = []
    def handle(request):
        requests.append(str(request.url))
        end, limit = int(request.url.params["after"]), int(request.url.params["limit"])
        return response([r for r in rows if int(r[0]) < end][:limit])
    b, _ = budget()
    with data.PublicClient(b, transport=httpx.MockTransport(handle)) as client:
        got = data.fetch_range(client, PRODUCT, empty(trial.MINUTE), at - 1500 * trial.MINUTE, at)
    assert len(got.bars) == 1445 and at - trial.MINUTE not in got.bars
    assert len(requests) == 5 and all("history-candles" in u for u in requests)

def test_stalled_pagination_fails_not_infinite():
    b, _ = budget()
    with data.PublicClient(b, transport=httpx.MockTransport(lambda r: response([row(T)]))) as client:
        with pytest.raises(trial.Unavailable, match="pagination"):
            data.fetch_range(client, PRODUCT, empty(trial.MINUTE), T - trial.MINUTE, T)
    assert b.attempts == 1

def test_unrequested_compression_is_rejected_before_decompression():
    b,_=budget()
    compressed=gzip.compress(b'x'*(4*1024*1024+1))
    with data.PublicClient(b,transport=httpx.MockTransport(lambda r:httpx.Response(200,headers={'Content-Encoding':'gzip'},stream=httpx.ByteStream(compressed)))) as client:
        with pytest.raises(trial.Unavailable,match='encoding'):
            client.get('/api/v5/public/time')
    assert b.received == 0 and b.attempts == 1
