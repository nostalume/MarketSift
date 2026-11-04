"""Native TDX framing and bar decoding, reused from AshareSift."""

from __future__ import annotations

import socket
import struct
import time
import zlib
from dataclasses import dataclass

MARKETS = {"sz": 0, "sh": 1, "bj": 2}
CATEGORY = {
    "5m": 0,
    "15m": 1,
    "30m": 2,
    "60m": 3,
    "day": 9,
    "week": 5,
    "month": 6,
    "1m": 8,
}
BARS_PER_DAY = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}
HEADER_LEN = 16
SETUP = bytes.fromhex("0c0218930001030003000d0001")


class TdxError(RuntimeError):
    """The protocol did not behave as measured."""


class NoReachableHost(TdxError):
    """Every configured host failed to complete the handshake."""


@dataclass(frozen=True)
class Bar:
    stamp: str
    open: float
    close: float
    high: float
    low: float
    volume: float
    amount: float
    up: int = 0
    down: int = 0


def decode_price(data: bytes, pos: int) -> tuple[int, int]:
    """Variable-length signed integer: 6 payload bits, 0x80 continues, 0x40 is the sign."""
    shift, byte = (6, data[pos])
    value, negative = (byte & 63, bool(byte & 64))
    if byte & 128:
        while True:
            pos += 1
            byte = data[pos]
            value += (byte & 127) << shift
            shift += 7
            if not byte & 128:
                break
    return (-value if negative else value, pos + 1)


def decode_volume(raw: int) -> float:
    """The vendor's own float encoding for volume and turnover."""
    if raw == 0:
        return 0.0
    logpoint = raw >> 24
    high, middle, low = (raw >> 16 & 255, raw >> 8 & 255, raw & 255)
    e1, e2, e3, e4 = (
        logpoint * 2 - 127,
        logpoint * 2 - 134,
        logpoint * 2 - 142,
        logpoint * 2 - 150,
    )
    scale = 2.0 ** abs(e1)
    if e1 < 0:
        scale = 1.0 / scale
    if high > 128:
        part = 2.0**e2 * 128.0 + (high & 127) * 2.0 ** (e2 + 1)
    else:
        part = 2.0**e2 * high if e2 >= 0 else 1 / 2.0**e2 * high
    third, fourth = (2.0**e3 * middle, 2.0**e4 * low)
    if high & 128:
        third, fourth = (third * 2.0, fourth * 2.0)
    return scale + part + third + fourth


def decode_stamp(category: int, data: bytes, pos: int) -> str:
    """Intraday periods pack the date and the minute count; daily and above carry yyyymmdd."""
    if category < 4 or category in (7, 8):
        packed, minutes = struct.unpack_from("<HH", data, pos)
        return f"{(packed >> 11) + 2004:04d}-{packed % 2048 // 100:02d}-{packed % 2048 % 100:02d} {minutes // 60:02d}:{minutes % 60:02d}"
    (packed,) = struct.unpack_from("<I", data, pos)
    return f"{packed // 10000:04d}-{packed % 10000 // 100:02d}-{packed % 100:02d}"


def _read_bars(category: int, body: bytes, index: bool) -> tuple[list[Bar], int]:
    """Decode ``body`` on one assumed layout, returning the bars and the bytes consumed."""
    (count,) = struct.unpack_from("<H", body, 0)
    pos, base, out = (2, 0, [])
    try:
        for _ in range(count):
            stamp = decode_stamp(category, body, pos)
            pos += 4
            open_diff, pos = decode_price(body, pos)
            close_diff, pos = decode_price(body, pos)
            high_diff, pos = decode_price(body, pos)
            low_diff, pos = decode_price(body, pos)
            volume_raw, amount_raw = struct.unpack_from("<II", body, pos)
            pos += 8
            up = down = 0
            if index:
                up, down = struct.unpack_from("<HH", body, pos)
                pos += 4
            opened = base + open_diff
            out.append(
                Bar(
                    stamp=stamp,
                    open=opened / 1000,
                    close=(opened + close_diff) / 1000,
                    high=(opened + high_diff) / 1000,
                    low=(opened + low_diff) / 1000,
                    volume=decode_volume(volume_raw),
                    amount=decode_volume(amount_raw),
                    up=up,
                    down=down,
                )
            )
            base = opened + close_diff
    except (struct.error, IndexError) as exc:
        raise TdxError(
            f"reply declares {count} bars but the body ran out after {len(out)}: {exc}"
        ) from exc
    return (out, pos)


def decode_bars(category: int, body: bytes, index: bool | None = None) -> list[Bar]:
    """Prices are stored as differences from the previous close, in thousandths."""
    if index is not None:
        return _read_bars(category, body, index)[0]
    for guess in (False, True):
        try:
            bars, consumed = _read_bars(category, body, guess)
        except TdxError:
            continue
        if consumed == len(body):
            return bars
    raise TdxError(
        f"neither record layout consumes the {len(body)}-byte reply exactly; it is not a bars reply this client can read"
    )


class Session:
    """One connection with one handshake. Frames are read as units, never partially."""

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 15.0,
        *,
        deadline: float | None = None,
    ) -> None:
        self.host, self.port = (host, port)
        self.timeout = timeout
        self.deadline = deadline
        self.sock = socket.create_connection((host, port), timeout=self._remaining())
        try:
            self.server = self.call(SETUP)
        except BaseException:
            self.sock.close()
            raise

    def _remaining(self) -> float:
        """Optional operation deadline, including partial receives, not only idle time."""
        timeout = (
            self.timeout
            if self.deadline is None
            else min(self.timeout, self.deadline - time.monotonic())
        )
        if timeout <= 0:
            raise TimeoutError("quote operation deadline exceeded")
        return timeout

    def close(self) -> None:
        self.sock.close()

    def __enter__(self) -> Session:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def _read(self, size: int) -> bytes:
        chunks, remaining = ([], size)
        while remaining > 0:
            self.sock.settimeout(self._remaining())
            chunk = self.sock.recv(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def call(self, request: bytes) -> bytes:
        self.sock.settimeout(self._remaining())
        self.sock.sendall(request)
        header = self._read(HEADER_LEN)
        if len(header) != HEADER_LEN:
            raise TdxError(f"{self.host}: header {len(header)}/{HEADER_LEN}")
        _first, _second, _third, wire, plain = struct.unpack("<IIIHH", header)
        body = self._read(wire)
        if len(body) != wire:
            raise TdxError(f"{self.host}: body {len(body)}/{wire}")
        return zlib.decompress(body) if wire != plain else body


def open_session(hosts: list[str], timeout: float = 15.0) -> Session:
    """First host that completes the handshake. Hosts are ``ip:port`` strings."""
    failures = []
    for entry in hosts:
        address, _, port = entry.partition(":")
        try:
            return Session(address, int(port or 7709), timeout)
        except (OSError, TdxError) as exc:
            failures.append(f"{entry} {type(exc).__name__}")
    raise NoReachableHost("; ".join(failures) or "no hosts configured")


def wire_code(code: str) -> bytes:
    """The protocol's six-byte code field. The market is a *separate* field, not a prefix."""
    encoded = code.encode("ascii")
    if len(encoded) != 6:
        raise ValueError(f"the wire wants six digits, not {code!r}")
    return encoded


def bars_request(
    category: int, market: int, code: str, start: int, count: int
) -> bytes:
    return struct.pack(
        "<HIHHHH6sHHHHIIH",
        268,
        16868360,
        28,
        28,
        1325,
        market,
        wire_code(code),
        category,
        1,
        start,
        count,
        0,
        0,
        0,
    )


def bars(
    session: Session,
    market: int,
    code: str,
    category: int,
    start: int,
    count: int,
    index: bool | None = None,
) -> list[Bar]:
    """One page of bars. ``index`` defaults to unset, which lets the reply declare its layout."""
    body = session.call(bars_request(category, market, code, start, count))
    return decode_bars(category, body, index=index)
