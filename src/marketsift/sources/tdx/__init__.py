"""Explicit five-minute TDX acquisition, with no database or budget owner."""

from datetime import datetime, timedelta, timezone

from ...time import instant, stamp
from . import wire

CHINA = timezone(timedelta(hours=8))
HOSTS = [
    "43.139.18.171:7709",
    "110.41.147.114:7709",
    "111.229.247.189:7709",
    "62.234.50.143:7709",
    "124.223.163.242:7709",
    "159.75.29.111:7709",
    "119.97.185.59:7709",
    "123.60.70.228:7709",
]
UNITS = {"price": "CNY", "amount": "CNY", "volume": "TDX source units"}


def kind(code):
    """Vendor range bookkeeping, separate from wire-layout detection or listing status."""
    market, symbol = code[:2].lower(), code[2:]
    if any(
        market == m and symbol.startswith(p)
        for m, p in (
            ("sh", "000"),
            ("sh", "88"),
            ("sh", "999"),
            ("sz", "399"),
            ("sz", "395"),
            ("bj", "899"),
        )
    ):
        return "index"
    if market in {"sh", "sz"} and symbol[:2] in {
        "50",
        "51",
        "52",
        "56",
        "58",
        "15",
        "16",
        "18",
    }:
        return "fund"
    if (
        (market == "sh" and symbol.startswith(("60", "68")))
        or (market == "sz" and symbol.startswith(("00", "30")))
        or market == "bj"
    ):
        return "stock"
    return "security"


def session_role(end):
    minute = end.astimezone(CHINA).hour * 60 + end.astimezone(CHINA).minute
    return (
        "regular"
        if minute in range(575, 691, 5) or minute in range(785, 901, 5)
        else "other"
    )


def instrument(code):
    code = code.upper()
    prefix, symbol = code[:2].lower(), code[2:]
    if prefix not in wire.MARKETS:
        raise ValueError("TDX instruments need an exchange prefix, e.g. SZ002946")
    wire.wire_code(symbol)
    return {
        "instrument": code,
        "venue": {"sh": "SSE", "sz": "SZSE", "bj": "BSE"}[prefix],
        "symbol": symbol,
        "kind": kind(code),
        "aliases": [code, symbol],
    }


def row(code, bar, requested, received):
    end = datetime.fromisoformat(bar.stamp).replace(tzinfo=CHINA)
    return {
        "source": "tdx",
        "instrument": code,
        "interval": "5m",
        "start": stamp(end - timedelta(minutes=5)),
        "end": stamp(end),
        "session": end.date().isoformat(),
        "observed_at": received,
        "requested_at": requested,
        "complete": end <= instant(requested),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "amount": bar.amount,
        "units": UNITS,
        "metadata": {"up": bar.up, "down": bar.down, "session_role": session_role(end)},
    }


def fetch(request, *, timeout=15):
    instruments = request.get("instruments", ())
    hosts = request.get("options", {}).get("hosts", HOSTS)
    since, until = instant(request["since"]), instant(request["until"])
    references = [instrument(code) for code in dict.fromkeys(instruments)]
    if not references:
        raise ValueError("select TDX instruments, e.g. --instrument SZ002946")
    requested = stamp()
    try:
        session = wire.open_session(list(hosts), timeout)
    except (OSError, wire.TdxError) as exc:
        yield {
            "read": {
                "source": "tdx",
                "requested_at": requested,
                "received_at": stamp(),
                "since": stamp(since),
                "until": stamp(until),
                "status": "failed",
                "detail": str(exc),
                "instruments": [r["instrument"] for r in references],
            }
        }
        return
    with session:
        for reference in references:
            code = reference["instrument"]
            start, oldest = 0, None
            while True:
                requested = stamp()
                read = {
                    "source": "tdx",
                    "requested_at": requested,
                    "since": stamp(since),
                    "until": stamp(until),
                    "instruments": [code],
                }
                try:
                    page = wire.bars(
                        session,
                        wire.MARKETS[code[:2].lower()],
                        code[2:],
                        wire.CATEGORY["5m"],
                        start,
                        800,
                    )
                    received = stamp()
                    rows = [row(code, b, requested, received) for b in page]
                    earliest = min((b["start"] for b in rows), default=None)
                    repeated = (
                        earliest is not None
                        and oldest is not None
                        and earliest >= oldest
                    )
                    boundary = earliest is not None and instant(earliest) <= since
                    selected = [b for b in rows if since <= instant(b["start"]) < until]
                    read.update(
                        received_at=received,
                        status="partial" if repeated else "ok",
                        detail=f"{session.host}:{session.port}; page offset {start}; "
                        + (
                            "repeated boundary; continuity unknown"
                            if repeated
                            else "requested boundary reached"
                            if boundary
                            else "source exhausted"
                            if not page
                            else "public five-minute history"
                        ),
                        first_time=earliest,
                        last_time=max((b["end"] for b in rows), default=None),
                    )
                    yield {"instruments": [reference], "bars": selected, "read": read}
                    if not page or boundary or repeated:
                        break
                    oldest = earliest
                    start += len(page)
                except (OSError, wire.TdxError, ValueError) as exc:
                    read.update(
                        received_at=stamp(),
                        status="failed",
                        detail=f"{code}: {type(exc).__name__}: {exc}",
                    )
                    yield {"read": read}
                    break
