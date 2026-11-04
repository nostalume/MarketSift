"""Jiuyangongshe public listing and literal embedded article/reply payloads."""

import json
import re
from urllib.parse import urljoin, urlsplit

from . import html, pages
from .common import china_time

HOME = "https://www.jiuyangongshe.com/study_publish"


def identity(url):
    parts = urlsplit(url)
    match = re.fullmatch(r"/a/([A-Za-z0-9]+)", parts.path)
    return match[1] if parts.hostname == "www.jiuyangongshe.com" and match else None


def literal(block, key, bindings):
    match = next(
        (m for m in re.finditer(rf'"(?:\\.|[^"\\])*"|(\b{key}:)', block) if m[1]), None
    )
    if not match:
        raise ValueError("missing native field " + key)
    rest = block[match.end() :]
    if rest.startswith('"'):
        return json.JSONDecoder().raw_decode(rest)[0]
    alias = re.match(r"\w+", rest)
    return bindings.get(alias[0]) if alias else None


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
        return [], links, [] if links else ["no supported static article links"]
    script = next(
        (s.text for s in tree.select("script") if "window.__NUXT__=" in s.text), ""
    )
    params = re.search(r"window\.__NUXT__=\(function\(([^)]*)\)", script)
    if not params or not script.rstrip().endswith("));"):
        raise ValueError("supported embedded article unavailable")
    arguments = script.rsplit("}(", 1)[1].rstrip()[:-3]
    tokens = re.findall(r'"(?:\\.|[^"\\])*"|[^,]+', arguments)
    bindings = dict(
        zip(
            params[1].split(","),
            (json.loads(t) if t.startswith('"') else None for t in tokens),
            strict=True,
        )
    )
    if literal(script, "article_id", bindings) != native:
        raise ValueError("returned another article")
    docs = []
    issues = []

    def add(block, key, parent=None):
        markup = literal(block, "content", bindings)
        label = literal(block, "create_time", bindings)
        docs.append(
            pages.document(
                "jiuyangongshe",
                key,
                response.url,
                html.clean_html(markup, base_url=response.url),
                response.received_at,
                title=None if parent else literal(block, "title", bindings),
                author=literal(block, "nickname", bindings),
                published_at=china_time(label),
                published_label=label,
                raw_text=markup,
                metadata={
                    "parent_native_id": parent,
                    "kind": "reply" if parent else "post",
                    "scope": "embedded public body; later replies unknown",
                },
            )
        )

    add(script, native)
    replies = re.split(r'article_comment_id:"([^"]+)"', script)
    for key, block in zip(replies[1::2], replies[2::2]):
        try:
            add(block, key, native)
        except (ValueError, TypeError) as exc:
            issues.append(str(exc))
    if not docs[0]["text"] and not docs[0]["attachments"]:
        raise ValueError("empty article body")
    return docs, [], issues


def fetch(request, *, timeout=15, transport=None):
    yield from pages.articles(
        request, parse, [HOME], timeout=timeout, transport=transport
    )
