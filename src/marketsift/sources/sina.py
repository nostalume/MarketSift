"""Sina's public chronological feed, reusing the existing pagination semantics."""

import json
from datetime import datetime, timedelta, timezone

from ..time import instant, stamp
from . import html, http

URL = "https://zhibo.sina.com.cn/api/zhibo/feed"
CHINA = timezone(timedelta(hours=8))


def parse_rows(rows, observed_at, since, until):
    documents, times, issues = [], [], []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            issues.append(f"row {index}: source item is not an object")
            continue
        label = row.get("create_time") or ""
        try:
            published = datetime.fromisoformat(label)
            if published.tzinfo is None:
                published = published.replace(tzinfo=CHINA)
        except (ValueError, TypeError):
            published = None
        if published:
            times.append(published)
            if not since <= published < until:
                continue
        try:
            content = html.clean_html(row.get("rich_text") or "", base_url=URL)
        except TypeError:
            issues.append(f"row {index}: source body cannot be read as text")
            continue
        text = content["text"]
        if not (text or row.get("title") or content["attachments"]):
            continue
        known_identity = row.get("id") is not None
        native = str(row["id"]) if known_identity else None
        url = "https://wap.cj.sina.cn/pc/7x24/" + native if known_identity else URL
        documents.append(
            {
                "source": "sina",
                "native_id": native,
                "url": url,
                "title": row.get("title"),
                "text": text,
                "raw_text": row.get("rich_text"),
                "author": None,
                "published_at": stamp(published) if published else None,
                "published_label": label,
                "observed_at": observed_at,
                "attachments": content["attachments"],
                "links": content["links"],
                "metadata": {} if known_identity else {"native_id_missing": True},
            }
        )
    return documents, times, issues


def fetch(request, *, timeout=15, transport=None):
    since, until = instant(request["since"]), instant(request["until"])
    page, seen = 1, set()
    with http.open_client(timeout=timeout, transport=transport) as client:
        while True:
            requested = stamp()
            read = {
                "source": "sina",
                "requested_at": requested,
                "since": stamp(since),
                "until": stamp(until),
            }
            try:
                response = http.request(
                    client,
                    "GET",
                    URL,
                    params={"zhibo_id": 152, "page_size": 100, "page": page},
                )
                received = response.received_at
                read["requested_at"] = response.requested_at
                rows = json.loads(response.body)["result"]["data"]["feed"]["list"]
                documents, times, issues = parse_rows(rows, received, since, until)
                identities = {str(r.get("id")) for r in rows if isinstance(r, dict)}
                repeated = bool(rows) and not identities.difference(seen)
                seen.update(identities)
                boundary = bool(times) and all(t < since for t in times)
                stop = not rows or repeated or boundary
                read.update(
                    received_at=received,
                    status="partial" if repeated or issues else "ok",
                    detail=f"page {page}; "
                    + (
                        "repeated page: continuity unknown"
                        if repeated
                        else "requested boundary reached"
                        if boundary
                        else "source returned empty page"
                        if not rows
                        else "dynamic public feed; site completeness unknown"
                    ),
                    first_time=stamp(min(times)) if times else None,
                    last_time=stamp(max(times)) if times else None,
                )
                if issues:
                    read["detail"] += "; " + "; ".join(issues)
                yield {"documents": documents, "read": read}
                if stop:
                    return
            except http.FetchError as exc:
                yield {"read": dict(read, **exc.read)}
                return
            except (ValueError, KeyError, TypeError) as exc:
                read.update(
                    received_at=stamp(),
                    status="failed",
                    detail=f"page {page}: {type(exc).__name__}: {exc}",
                )
                yield {"read": read}
                return
            page += 1
