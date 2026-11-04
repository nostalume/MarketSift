"""Eastmoney stock-board listings and native JSON article bodies."""

import json
import re
from urllib.parse import urljoin

from . import html, pages
from .common import china_time, json_binding


def parse(response):
    tree = html.parse_html(response.body)
    url = response.url
    article = re.search(r"/news,(\d{6}),(\d+)\.html", url)
    listing = re.search(r"/list,(\d{6})\.html", url)
    docs = []
    links = []
    issues = []
    if article:
        board, native = article.groups()
        row = json_binding(tree, "post_article")
        if str(row["post_id"]) != native or row["post_guba"]["stockbar_code"] != board:
            raise ValueError("returned another article/board")
        label = row.get("post_publish_time")
        content = html.clean_html(row["post_content"], base_url=url)
        docs.append(
            pages.document(
                "guba",
                native,
                url,
                content,
                response.received_at,
                title=row.get("post_title"),
                author=(row.get("post_user") or {}).get("user_nickname"),
                published_at=china_time(label),
                published_label=label,
                raw_text=row["post_content"],
                metadata={
                    "board_code": board,
                    "last_activity": row.get("post_last_time"),
                    "scope": "public article; comments not acquired",
                    "display_metrics": {
                        k: row.get(k)
                        for k in (
                            "post_click_count",
                            "post_comment_count",
                            "post_like_count",
                        )
                    },
                },
            )
        )
    elif listing:
        board = listing[1]
        data = json_binding(tree, "article_list")
        if data.get("bar_code") != board:
            raise ValueError("returned another board")
        anchors = {
            a.attrs["data-postid"]: urljoin(url, a.attrs.get("href", ""))
            for a in tree.select("a[data-postid]")
        }
        for row in data["re"]:
            native = str(row["post_id"])
            target = anchors.get(native)
            if not target or not re.search(
                r"/news,\d{6}," + native + r"\.html", target
            ):
                issues.append("unsupported detail link " + native)
            else:
                links.append(target)
            title = row.get("post_title") or ""
            docs.append(
                pages.document(
                    "guba",
                    native,
                    target or url,
                    {
                        "text": title,
                        "links": [target] if target else [],
                        "attachments": [],
                    },
                    response.received_at,
                    title=title,
                    published_at=china_time(row.get("post_publish_time")),
                    published_label=row.get("post_publish_time"),
                    raw_text=json.dumps(row, ensure_ascii=False),
                    metadata={
                        "board_code": board,
                        "scope": "listing title only",
                        "last_activity": row.get("post_last_time"),
                        "detail_supported": target in links,
                    },
                )
            )
    else:
        raise ValueError("use a supported stock-board listing or article URL")
    return docs, list(dict.fromkeys(links)), issues


def fetch(request, *, timeout=15, transport=None):
    defaults = [
        "https://guba.eastmoney.com/list," + c[2:] + ".html"
        for c in request.get("instruments", ())
        if c.startswith(("SH", "SZ", "BJ"))
    ]
    yield from pages.articles(
        request, parse, defaults, timeout=timeout, transport=transport
    )
