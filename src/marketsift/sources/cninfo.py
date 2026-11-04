"""Current official names, with explicit keys or recognized exchange code ranges."""

import json

from ..time import stamp
from . import http, tdx

URL = "https://www.cninfo.com.cn/new/data/szse_stock.json"


def fetch(request, *, timeout=15, transport=None):
    references = [tdx.instrument(c) for c in request.get("instruments", ())]
    requested = stamp()
    read = {
        "source": "cninfo",
        "requested_at": requested,
        "instruments": [r["instrument"] for r in references],
    }
    try:
        with http.open_client(timeout=timeout, transport=transport) as client:
            response = http.request(client, "GET", URL)
            read.update(requested_at=response.requested_at)
            rows = json.loads(response.body)["stockList"]
        keys = {}
        for ref in references:
            if ref["kind"] == "stock":
                keys.setdefault(ref["symbol"], []).append(ref)
        matched, ambiguous = [], []
        for row in rows:
            if row.get("category") != "A股":
                continue
            candidates = keys.get(row.get("code"), [])
            if not references:
                code = row.get("code", "")
                prefix = (
                    "SH"
                    if code.startswith(("60", "68"))
                    else "SZ"
                    if code.startswith(("00", "30"))
                    else "BJ"
                    if code.startswith(("43", "83", "87", "92"))
                    else None
                )
                if prefix:
                    candidates = [tdx.instrument(prefix + code)]
            if len(candidates) == 1:
                ref = dict(candidates[0])
                aliases = [v for v in (row.get("zwjc"), row.get("pinyin")) if v]
                ref.update(
                    name=row.get("zwjc"),
                    aliases=aliases,
                    metadata={
                        "venue_scope": "code-range convention, not listing-status certification",
                        "native": row,
                    },
                )
                matched.append(ref)
            elif len(candidates) > 1:
                ambiguous.append(row.get("code"))
        read.update(
            received_at=response.received_at,
            status="ok",
            detail=f"{URL}; {len(matched)} current name matches; ambiguous keys {ambiguous}; not a tradability or historical-name list",
        )
        yield {"instruments": matched, "read": read}
    except http.FetchError as exc:
        yield {"read": dict(read, **exc.read)}
    except (ValueError, KeyError, TypeError) as exc:
        read.update(
            received_at=stamp(), status="failed", detail=f"{type(exc).__name__}: {exc}"
        )
        yield {"read": read}
