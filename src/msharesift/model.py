"""Public product identity and complete-candle representation."""
from __future__ import annotations

import math
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

MINUTE = 60_000

FIVE = 5 * MINUTE

HOUR = 60 * MINUTE

WEEK = 7 * 24 * HOUR

EXCLUDED = frozenset({"BTC", "ETH", "BNB", "USDT", "USDC", "FDUSD", "TUSD",
                      "USDP", "DAI", "BUSD", "EUR", "TRY", "BRL"})

class Unavailable(ValueError):
    """An input cannot support the requested data operation."""

def _integer(value: object) -> int:
    if type(value) is int:
        return value
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise Unavailable("invalid_integer")
    try:
        return int(value)
    except ValueError as exc:
        raise Unavailable("invalid_integer") from exc

def _number(value: object) -> float:
    # Already parsed exact floats dominate cache merge/save. Subclasses retain
    # the original conversion path, including their __float__ behavior.
    if type(value) is float:
        if not math.isfinite(value):
            raise Unavailable("nonfinite_number")
        return value
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise Unavailable("invalid_number")
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise Unavailable("invalid_number") from exc
    if not math.isfinite(result):
        raise Unavailable("nonfinite_number")
    return result

def source_cutoff(server_ms: int) -> int:
    return ((_integer(server_ms) - 2000) // MINUTE) * MINUTE

@dataclass(frozen=True)
class Product:
    inst_type: str
    inst_id: str
    base: str
    normal_since: int

    @property
    def key(self) -> tuple[str, str, str]:
        return ("okx", self.inst_type, self.inst_id)

def admit_product(raw: Mapping[str, object], at: int, *, excluded_bases=EXCLUDED) -> Product:
    """Admit public metadata; unavailable fields never become a guessed pool."""
    category = raw.get("instCategory")
    if category in ("3", "4", "5", "6"):
        raise Unavailable("excluded_noncrypto")
    if category != "1":
        raise Unavailable("metadata_unavailable:instCategory")
    kind, inst_id = raw.get("instType"), raw.get("instId")
    if kind not in ("SPOT", "SWAP") or raw.get("state") != "live" or raw.get("ruleType") != "normal":
        raise Unavailable("excluded_product_state_or_type")
    if not isinstance(inst_id, str) or not inst_id:
        raise Unavailable("metadata_unavailable:instId")
    if kind == "SPOT":
        base = raw.get("baseCcy")
        if raw.get("quoteCcy") != "USDT" or not isinstance(base, str) or not base or inst_id != base + "-USDT":
            raise Unavailable("excluded_spot_quote_or_identity")
    else:
        family = raw.get("instFamily")
        if raw.get("ctType") != "linear" or raw.get("settleCcy") != "USDT" or not isinstance(family, str) or not family.endswith("-USDT") or inst_id != family + "-SWAP":
            raise Unavailable("excluded_swap_settlement_or_identity")
        base = family[:-5]
        if not base or raw.get("ctValCcy") != base or _number(raw.get("ctVal")) <= 0:
            raise Unavailable("metadata_unavailable:contract_value")
    if base in excluded_bases:
        raise Unavailable("excluded_base")
    times = [_integer(raw.get("listTime"))]
    for name in ("contTdSwTime", "preMktSwTime"):
        value = raw.get(name)
        if value not in (None, ""):
            times.append(_integer(value))
    if times[0] <= 0 or any(value < 0 or value > at for value in times):
        raise Unavailable("metadata_unavailable:normal_start")
    return Product(kind, inst_id, base, max(times))

@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    volume_ccy: float
    quote: float

@dataclass(frozen=True)
class Series:
    grain: int
    bars: Mapping[int, Bar]
    errors: Mapping[int, str]

    @classmethod
    def parse(cls, rows: list[list[object]], grain: int) -> Series:
        if grain not in (MINUTE, FIVE, HOUR):
            raise ValueError("Unsupported candle grain")
        bars: dict[int, Bar] = {}
        errors: dict[int, str] = {}
        for row in rows:
            if not isinstance(row, list) or len(row) != 9:
                raise Unavailable("invalid_candle_shape")
            ts = _integer(row[0])
            if ts < 0 or ts % grain:
                raise Unavailable("invalid_candle_alignment")
            if row[8] == "0":
                continue
            if row[8] != "1":
                errors[ts] = "invalid_confirm"
                bars.pop(ts, None)
                continue
            try:
                o, h, l, c, v, vc, q = map(_number, row[1:8])
                # Finite inputs: positive low plus both endpoint bounds imply
                # all positive OHLC and high >= low, without repeated min/max.
                if l <= 0 or o < l or c < l or o > h or c > h or v < 0 or vc < 0 or q < 0:
                    raise Unavailable("invalid_ohlc_or_volume")
                bar = Bar(ts, o, h, l, c, v, vc, q)
                if ts in bars and bars[ts] != bar:
                    raise Unavailable("conflicting_complete_candle")
                if ts not in errors:
                    bars[ts] = bar
            except Unavailable as exc:
                errors[ts] = str(exc)
                bars.pop(ts, None)
        return cls(grain, MappingProxyType(bars), MappingProxyType(errors))

    def window(self, start: int, count: int) -> list[Bar]:
        result = []
        for ts in range(start, start + count * self.grain, self.grain):
            if ts in self.errors:
                raise Unavailable(self.errors[ts])
            if ts not in self.bars:
                raise Unavailable("missing_complete_candle")
            result.append(self.bars[ts])
        return result
