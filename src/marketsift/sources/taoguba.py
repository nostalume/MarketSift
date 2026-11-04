"""Public Taoguba article and HTML replies; homepage discovery is explicit."""

import re
from urllib.parse import urljoin, urlsplit

from . import html, pages
from .common import china_time

HOME = "https://www.tgb.cn/"


def identity(url):
    parts = urlsplit(url)
    match = re.fullmatch(r"/a/([A-Za-z0-9]+)(?:-1)?", parts.path)
    return match[1] if parts.hostname == "www.tgb.cn" and match else None


def parse(response):
    tree = html.parse_html(response.body)
    native = identity(response.url)
    if native is None:
        links = list(
            dict.fromkeys(
                urljoin(response.url, a.attrs["href"]).split("#")[0]
                for a in tree.select("a[href]")
                if identity(urljoin(response.url, a.attrs["href"]))
            )
        )
        return (
            [],
            links,
            []
            if links
            else ["no supported static article links; page is not article coverage"],
        )
    meta, root, header = (
        tree.select_one("#gioMsg"),
        tree.select_one("#first"),
        tree.select_one(".article-data"),
    )
    if meta is None or root is None or header is None:
        raise ValueError(
            "article structure unavailable; possible shell/challenge/layout change"
        )
    canonical = tree.select_one('link[rel="canonical"]')
    if canonical is not None and identity(canonical.attrs.get("href", "")) != native:
        raise ValueError("returned another article")
    docs = []
    issues = []

    def add(node, key, author, title, label, parent=None):
        docs.append(
            pages.document(
                "taoguba",
                key,
                response.url,
                html.clean_node(node, base_url=response.url),
                response.received_at,
                author=author,
                title=title,
                published_label=label,
                published_at=china_time(label),
                raw_text=node.serialize(),
                metadata={
                    "parent_native_id": parent,
                    "kind": "reply" if parent else "post",
                    "scope": "returned HTML page; later replies unknown",
                },
            )
        )

    add(
        root, native, meta.attrs.get("username"), meta.attrs.get("subject"), header.text
    )
    for reply in tree.select('div[id^="reply_"]'):
        body = reply.select_one(".comment-data-text")
        author = reply.select_one(".user-name")
        clock = reply.select_one(".pcyclspan")
        match = (
            re.fullmatch(r"reply(\d+)", body.attrs.get("id", ""))
            if body is not None
            else None
        )
        if not match:
            issues.append("reply with missing native identity")
            continue
        add(
            body,
            match[1],
            author.text if author is not None else None,
            None,
            clock.text if clock is not None else None,
            native,
        )
    if not docs[0]["text"] and not docs[0]["attachments"]:
        raise ValueError("empty article body")
    return docs, [], issues


def fetch(request, *, timeout=15, transport=None):
    yield from pages.articles(
        request, parse, [HOME], timeout=timeout, transport=transport
    )
