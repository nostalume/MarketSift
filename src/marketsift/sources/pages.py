"""Explicit HTML articles and Discourse topics using the common HTML implementation."""

import json
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlsplit

from ..time import instant, stamp
from . import html, http
from .common import china_time, published, source_read

FORUMS = {"gov.uniswap.org", "ethresear.ch", "governance.aave.com"}


def official(response, *, source, selector, article_pattern):
    tree = html.parse_html(response.body)
    if not re.search(article_pattern, response.url):
        links = list(
            dict.fromkeys(
                urljoin(response.url, a.attrs["href"])
                for a in tree.select("a[href]")
                if re.search(article_pattern, urljoin(response.url, a.attrs["href"]))
            )
        )
        return [], links, [] if links else ["no supported official article links"]
    root = tree.select_one(selector)
    if root is None:
        raise ValueError("official article body unavailable")

    def meta(name):
        node = tree.select_one('meta[name="' + name + '"]')
        return node.attrs.get("content") if node is not None else None

    label = meta("PubDate")
    content = html.clean_node(root, base_url=response.url)
    if not content["text"]:
        raise ValueError("empty official body")
    tables = []
    for table in root.select("table"):
        tables.append(
            [
                [
                    dict(
                        text=cell.text.strip(),
                        rowspan=cell.attrs.get("rowspan", "1"),
                        colspan=cell.attrs.get("colspan", "1"),
                    )
                    for cell in row.select("td,th")
                ]
                for row in table.select("tr")
            ]
        )
    item = document(
        source,
        response.url,
        response.url,
        content,
        response.received_at,
        title=meta("ArticleTitle"),
        author=meta("Author"),
        published_label=label,
        published_at=china_time(label),
        raw_text=root.serialize(),
        metadata={
            "scope": "official selected article body; attachments not downloaded",
            "publication_precision": (
                "second" if re.search(r"\d{2}:\d{2}:\d{2}", label or "") else "minute"
            )
            if china_time(label)
            else "source label only",
            "native_tables": tables,
        },
    )
    # These official providers publish mainland-China local dates. Preserve
    # their uncertainty interval instead of manufacturing a release instant.
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", label or ""):
        day = datetime.fromisoformat(label + "T00:00:00+08:00")
        item["metadata"].update(
            publication_precision="day",
            publication_bounds={
                "since": stamp(day),
                "until": stamp(day + timedelta(days=1)),
            },
        )
    return [item], [], []


def articles(request, parse, defaults=(), *, timeout=15, transport=None):
    """Shared I/O traversal; provider parse owns identity, discovery and content."""
    pending = list(request.get("urls") or defaults)
    if not pending:
        raise ValueError(request["source"] + " needs explicit article/list URLs")
    seen = set()
    with http.open_client(timeout=timeout, transport=transport) as client:
        for url in pending:
            if url in seen:
                continue
            seen.add(url)
            try:
                response = http.request(client, "GET", url)
                docs, links, issues = parse(response)
                selected = []
                for doc in docs:
                    when = doc.get("published_at")
                    if when and (
                        (
                            request.get("since")
                            and instant(when) < instant(request["since"])
                        )
                        or (
                            request.get("until")
                            and instant(when) >= instant(request["until"])
                        )
                    ):
                        continue
                    selected.append(doc)
                following = request.get("options", {}).get("follow_links", True)
                yield dict(
                    documents=selected,
                    read=source_read(
                        request["source"],
                        response,
                        detail="returned page only; known links and body/reply scope retained, not whole-site coverage",
                        request=request,
                        status="partial" if issues else "ok",
                        issues=issues,
                        discovered_urls=links,
                        deferred_urls=[] if following else links,
                    ),
                )
                if following:
                    pending.extend(links)
            except http.FetchError as exc:
                yield {"read": dict(source=request["source"], **exc.read)}
            except (ValueError, KeyError, TypeError, IndexError, UnicodeError) as exc:
                yield {
                    "read": dict(
                        source=request["source"],
                        requested_at=stamp(),
                        received_at=stamp(),
                        status="failed",
                        detail=str(exc),
                        url=url,
                    )
                }


def document(source, native, url, content, observed, **fields):
    return dict(
        source=source,
        native_id=native,
        url=url,
        text=content["text"],
        links=content["links"],
        attachments=content["attachments"],
        observed_at=observed,
        **fields,
    )


def fetch(request, *, timeout=15, transport=None):
    options = request.get("options", {})
    urls = request.get("urls", ())
    if not urls:
        raise ValueError("page/discourse needs explicit urls")
    with http.open_client(timeout=timeout, transport=transport) as client:
        for url in urls:
            parts = urlsplit(url)
            topic = next(
                (
                    p.removesuffix(".json")
                    for p in parts.path.split("/")[2:]
                    if p.removesuffix(".json").isdigit()
                ),
                None,
            )
            is_forum = (
                parts.hostname in FORUMS
                and parts.path.startswith("/t/")
                and topic is not None
            )
            target = (
                f"{parts.scheme}://{parts.netloc}/t/{topic}.json" if is_forum else url
            )
            try:
                response = http.request(client, "GET", target)
                if is_forum:
                    data = json.loads(response.body)
                    if str(data["id"]) != topic:
                        raise ValueError("forum returned another topic")
                    topic_url = f"{parts.scheme}://{parts.netloc}/t/{topic}"
                    posts = data["post_stream"]["posts"]
                    known = data["post_stream"].get("stream", [])
                    got = {p["id"] for p in posts}
                    remaining = [n for n in known if n not in got]
                    yield topic_batch(
                        data,
                        posts,
                        response,
                        request["source"],
                        topic_url,
                        remaining,
                        continuing=options.get("all_posts", False),
                    )
                    if options.get("all_posts"):
                        for begin in range(0, len(remaining), 20):
                            response = http.request(
                                client,
                                "GET",
                                f"{parts.scheme}://{parts.netloc}/t/{topic}/posts.json",
                                params=[
                                    ("post_ids[]", n)
                                    for n in remaining[begin : begin + 20]
                                ],
                            )
                            extra = json.loads(response.body)["post_stream"]["posts"]
                            yield topic_batch(
                                data,
                                extra,
                                response,
                                request["source"],
                                topic_url,
                                remaining[begin + 20 :],
                                continuing=True,
                            )
                    continue
                tree = html.parse_html(response.body)
                selector = options.get("selector") or (
                    {"cointelegraph.com": '[data-testid="post__body"]'}.get(
                        parts.hostname
                    )
                )
                if not selector and len(tree.select("article")) > 1:
                    raise ValueError(
                        "multiple article roots; supply an explicit source body selector"
                    )
                root = (
                    tree.select_one(selector)
                    if selector
                    else tree.select_one("article")
                    or tree.select_one("main")
                    or tree.select_one("body")
                )
                if root is None:
                    raise ValueError(
                        "requested body unavailable; possible layout/shell/challenge"
                    )
                content = html.clean_node(
                    root, base_url=response.url, exclude=options.get("exclude", ())
                )
                if not content["text"] and not content["attachments"]:
                    raise ValueError(
                        "no readable body; script shell is not article coverage"
                    )

                def meta(key):
                    node = tree.select_one(f'meta[property="{key}"],meta[name="{key}"]')
                    return node.attrs.get("content") if node is not None else None

                label = meta("article:published_time")
                for node in tree.select('script[type="application/ld+json"]'):
                    try:
                        structured = json.loads(node.text)
                    except ValueError:
                        continue
                    if (
                        isinstance(structured, dict)
                        and structured.get("@type") == "NewsArticle"
                    ):
                        label = label or structured.get("datePublished")
                title = tree.select_one("title")
                item = document(
                    request["source"],
                    response.url,
                    response.url,
                    content,
                    response.received_at,
                    title=title.text if title is not None else None,
                    author=meta("author"),
                    published_at=published(label),
                    published_label=label,
                    raw_text=root.serialize(),
                    metadata={
                        "scope": "explicit selector"
                        if selector
                        else root.tag + "; may include navigation",
                        "publisher_domain": parts.hostname,
                    },
                )
                yield dict(
                    documents=[item],
                    read=source_read(
                        request["source"], response, detail=item["metadata"]["scope"]
                    ),
                )
            except http.FetchError as exc:
                yield {"read": dict(source=request["source"], **exc.read)}
            except (ValueError, KeyError, TypeError) as exc:
                yield {
                    "read": dict(
                        source=request["source"],
                        requested_at=stamp(),
                        received_at=stamp(),
                        status="failed",
                        detail=str(exc),
                    )
                }


def topic_batch(data, posts, response, source, url, remaining, *, continuing=False):
    documents = []
    for post in posts:
        link = url.rstrip("/") + "/" + str(post["post_number"])
        content = html.clean_html(post["cooked"], base_url=url)
        documents.append(
            document(
                source,
                f"{data['id']}:{post['id']}",
                link,
                content,
                response.received_at,
                title=data["title"],
                author=post.get("username"),
                published_at=published(post.get("created_at")),
                published_label=post.get("created_at"),
                raw_text=post["cooked"],
                metadata={
                    "topic_id": data["id"],
                    "post_number": post["post_number"],
                    "reply_to_post_number": post.get("reply_to_post_number"),
                    "updated_at": post.get("updated_at"),
                    "publisher_domain": urlsplit(url).hostname,
                },
            )
        )
    return dict(
        documents=documents,
        read=source_read(
            source,
            response,
            detail="public topic posts; remaining IDs are discoveries, not read content",
            status="partial" if remaining and not continuing else "ok",
            supplied_posts=len(posts),
            source_posts_count=data.get("posts_count"),
            remaining_post_ids=remaining,
        ),
    )
