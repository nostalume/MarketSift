"""HTTP acquisition and timing, shared by source adapters; no persistence."""

from dataclasses import dataclass

import httpx

from ..time import stamp


@dataclass(frozen=True)
class Download:
    url: str
    status_code: int
    headers: dict
    body: bytes
    requested_at: str
    received_at: str


class FetchError(Exception):
    def __init__(self, url, requested_at, reason, *, status=None, retry_after=None):
        super().__init__(f"{url}: {reason}")
        self.read = {
            "requested_at": requested_at,
            "received_at": stamp(),
            "status": "failed",
            "detail": str(self),
            "http_status": status,
            "retry_after": retry_after,
        }


def open_client(*, timeout=15, transport=None):
    return httpx.Client(
        timeout=timeout,
        transport=transport,
        follow_redirects=True,
        headers={"User-Agent": "MarketSift/0.1 (public market research)"},
    )


def request(client, method, url, **kwargs):
    requested = stamp()
    try:
        response = client.request(method, url, **kwargs)
    except httpx.HTTPError as exc:
        raise FetchError(url, requested, f"{type(exc).__name__}: {exc}") from exc
    received = stamp()
    if not response.is_success:
        raise FetchError(
            str(response.url),
            requested,
            f"HTTP {response.status_code}",
            status=response.status_code,
            retry_after=response.headers.get("retry-after"),
        )
    return Download(
        str(response.url),
        response.status_code,
        dict(response.headers),
        response.content,
        requested,
        received,
    )


def save(client, url, path):
    """Stream a requested native archive to a temporary ordinary file."""
    requested = stamp()
    try:
        with client.stream("GET", url) as response:
            if not response.is_success:
                raise FetchError(
                    url,
                    requested,
                    f"HTTP {response.status_code}",
                    status=response.status_code,
                    retry_after=response.headers.get("retry-after"),
                )
            with path.open("wb") as output:
                for chunk in response.iter_bytes():
                    output.write(chunk)
            return Download(
                str(response.url),
                response.status_code,
                dict(response.headers),
                b"",
                requested,
                stamp(),
            )
    except httpx.HTTPError as exc:
        raise FetchError(url, requested, f"{type(exc).__name__}: {exc}") from exc
