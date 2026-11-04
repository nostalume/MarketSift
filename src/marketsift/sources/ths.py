"""THS finite public advisor feed, without asserting investor representativeness."""

from . import html, pages
from .common import china_time


def parse(response):
    tree = html.parse_html(response.body)
    docs = []
    issues = []
    for entry in tree.select("[data-pid]"):
        root = entry.select_one(".word-content")
        if root is None:
            issues.append("missing feed body")
            continue
        author = entry.select_one(".broker-name")
        clock = entry.select_one(".feed-item-timeline-time")
        label = (
            (entry.attrs.get("data-date") or "")
            + " "
            + (clock.text if clock is not None else "")
        )
        docs.append(
            pages.document(
                "ths",
                entry.attrs["data-pid"],
                response.url,
                html.clean_node(root, base_url=response.url),
                response.received_at,
                author=author.text if author is not None else None,
                published_at=china_time(label),
                published_label=label,
                raw_text=root.serialize(),
                metadata={
                    "scope": "public advisor feed snippet; permalink/comments unknown",
                    "publisher_role": "unverified advisor display identity",
                },
            )
        )
    if not docs:
        raise ValueError("supported public feed not exposed")
    return docs, [], issues


def fetch(request, *, timeout=15, transport=None):
    yield from pages.articles(
        request,
        parse,
        ["https://t.10jqka.com.cn/"],
        timeout=timeout,
        transport=transport,
    )
