"""Native candle pagination/decoding; retain forming bars and native volume units."""

from datetime import timedelta

from ...time import instant, stamp
from .. import http
from ..common import from_ms, milliseconds, number, source_read
from . import get

INTERVALS = {
    "1m": ("1m", 60),
    "3m": ("3m", 180),
    "5m": ("5m", 300),
    "15m": ("15m", 900),
    "30m": ("30m", 1800),
    "1h": ("1H", 3600),
    "2h": ("2H", 7200),
    "4h": ("4H", 14400),
    "6h": ("6Hutc", 21600),
    "12h": ("12Hutc", 43200),
    "1d": ("1Dutc", 86400),
    "1w": ("1Wutc", 604800),
}
PATHS = {
    "trade": "/api/v5/market/history-candles",
    "mark": "/api/v5/market/history-mark-price-candles",
    "index": "/api/v5/market/history-index-candles",
}


def fetch(client, product, request):
    interval, basis = request.get("interval", "5m"), request.get("basis", "trade")
    native_interval, seconds = INTERVALS[interval]
    since, until = milliseconds(request["since"]), milliseconds(request["until"])
    rules = product["rules"]
    identifier = product["symbol"] if basis != "index" else rules.get("underlying")
    cursor = until
    while True:
        try:
            response, raw = get(
                client,
                PATHS[basis],
                {
                    "instId": identifier,
                    "bar": native_interval,
                    "after": str(cursor),
                    "limit": "100",
                },
            )
            rows, issues, times = [], [], []
            for values in raw:
                try:
                    ts = int(values[0])
                    times.append(ts)
                    if not since <= ts < until:
                        continue
                    end = instant(from_ms(ts)) + timedelta(seconds=seconds)
                    prices = [number(v) for v in values[1:5]]
                    volume = number(values[5]) if basis == "trade" else None
                    amount = (
                        number(values[7])
                        if basis == "trade" and len(values) >= 9
                        else None
                    )
                    rows.append(
                        dict(
                            source="okx",
                            instrument=product["instrument"],
                            interval=interval,
                            basis=basis,
                            start=from_ms(ts),
                            end=stamp(end),
                            session=instant(from_ms(ts)).date().isoformat(),
                            observed_at=response.received_at,
                            requested_at=response.requested_at,
                            complete=str(values[-1]) == "1",
                            open=prices[0],
                            high=prices[1],
                            low=prices[2],
                            close=prices[3],
                            volume=volume,
                            amount=amount,
                            units={
                                "price": rules.get("quote"),
                                "amount": rules.get("quote"),
                                "volume": rules.get("base")
                                if product["kind"] == "SPOT"
                                else "contracts",
                            },
                            metadata={
                                "volume_ccy": number(values[6])
                                if basis == "trade" and len(values) >= 9
                                else None,
                                "volume_ccy_unit": rules.get("base"),
                                "native_confirm": values[-1],
                                "session_role": "source_utc",
                                "native_instrument": identifier,
                            },
                        )
                    )
                except (ValueError, TypeError, IndexError) as exc:
                    issues.append(str(exc))
            oldest = min(times) if times else None
            stopped = not raw or oldest is None or oldest <= since or oldest >= cursor
            reason = (
                "requested boundary reached"
                if oldest is not None and oldest <= since
                else "source exhausted"
                if not raw
                else "pagination stalled"
                if stopped
                else "next native page"
            )
            yield dict(
                bars=rows,
                read=source_read(
                    "okx",
                    response,
                    request=request,
                    detail=reason,
                    status="partial"
                    if issues or (stopped and reason != "requested boundary reached")
                    else "ok",
                    issues=issues,
                    first_time=from_ms(min(times)) if times else None,
                    last_time=from_ms(max(times)) if times else None,
                ),
            )
            if stopped:
                return
            cursor = oldest
        except http.FetchError as exc:
            yield {"read": dict(source="okx", **exc.read)}
            return
        except (ValueError, KeyError, TypeError) as exc:
            yield {
                "read": dict(
                    source="okx",
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="failed",
                    detail=str(exc),
                )
            }
            return
