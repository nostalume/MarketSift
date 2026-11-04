"""Public RSS/Atom, preserving native dates, full content versus summaries."""

from copy import deepcopy
from html import escape
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

from ..time import instant, stamp
from . import html, http
from .common import published, source_read

FEEDS = {
    "bea": ("official", "https://apps.bea.gov/rss/rss.xml"),
    "bls-releases": ("official", "https://www.bls.gov/feed/bls_latest.rss"),
    "coindesk": ("news", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    "cointelegraph": ("news", "https://cointelegraph.com/rss"),
    "blockworks": ("news", "https://blockworks.com/feed"),
    "ethereum-research": ("forum", "https://ethresear.ch/latest.rss"),
    "uniswap": ("forum", "https://gov.uniswap.org/latest.rss"),
    "reddit-cryptocurrency": (
        "forum",
        "https://www.reddit.com/r/CryptoCurrency/new/.rss",
    ),
    "hayes": ("kol", "https://cryptohayes.substack.com/feed"),
    "lynalden": ("kol", "https://www.lynalden.com/feed/"),
    "bitcoinmarkets": ("forum", "https://www.reddit.com/r/BitcoinMarkets/new/.rss"),
    "conks": ("kol", "https://www.conks.plumbing/feed"),
    "cryptoslate": ("news", "https://cryptoslate.com/feed/"),
    "coinmetrics": ("research", "https://coinmetrics.substack.com/feed"),
    "aave": ("forum", "https://governance.aave.com/latest.rss"),
    "apricitas": ("research", "https://www.apricitas.io/feed"),
}
FEEDS["fed-policy"] = (
    "official",
    "https://www.federalreserve.gov/feeds/press_monetary.xml",
)


def embedded(node):
    if node is None:
        return ""
    copied = deepcopy(node)
    for item in copied.iter():
        if isinstance(item.tag, str):
            item.tag = item.tag.rsplit("}", 1)[-1]
    return (copied.text or "") + "".join(
        ET.tostring(c, encoding="unicode") for c in copied
    )


def parse(payload, url, source, observed_at, request):
    root = ET.fromstring(payload)
    ns = "{http://www.w3.org/2005/Atom}"
    if root.tag == "rss":
        entries, prefix = root.findall("./channel/item"), ""
    elif root.tag == ns + "feed":
        entries, prefix = root.findall(ns + "entry"), ns
    else:
        raise ValueError("expected RSS/Atom, not HTML shell or challenge")
    records, issues = [], []
    for index, entry in enumerate(entries):
        try:
            if prefix:
                link = next(
                    (
                        n.get("href")
                        for n in entry.findall(ns + "link")
                        if n.get("rel", "alternate") == "alternate"
                    ),
                    None,
                )
                body = embedded(entry.find(ns + "content"))
                summary = embedded(entry.find(ns + "summary"))
                label = entry.findtext(ns + "published")
                updated = entry.findtext(ns + "updated")
                author = entry.findtext(ns + "author/" + ns + "name")
                native = entry.findtext(ns + "id")
                content_node = entry.find(ns + "content")
                summary_node = entry.find(ns + "summary")
                if (
                    summary_node is not None
                    and summary_node.get("type", "text") == "text"
                ):
                    summary = escape(summary)
                if (
                    content_node is not None
                    and content_node.get("type", "text") == "text"
                ):
                    body = escape(body)
            else:
                link = entry.findtext("link")
                body = (
                    entry.findtext("{http://purl.org/rss/1.0/modules/content/}encoded")
                    or ""
                )
                summary = entry.findtext("description") or ""
                label = entry.findtext("pubDate")
                updated = None
                author = entry.findtext("author") or entry.findtext(
                    "{http://purl.org/dc/elements/1.1/}creator"
                )
                native = entry.findtext("guid")
            published_at = published(label)
            if published_at and (
                (
                    request.get("since")
                    and instant(published_at) < instant(request["since"])
                )
                or (
                    request.get("until")
                    and instant(published_at) >= instant(request["until"])
                )
            ):
                continue
            link = urljoin(url, link) if link else url
            content = html.clean_html(body, base_url=link)
            scope = "feed_content"
            if not content["text"] and not content["attachments"]:
                content = html.clean_html(summary, base_url=link)
                scope = "feed_summary" if content["text"] else "feed_metadata"
            identity = native or (link if link != url else None)
            if not identity:
                issues.append(
                    f"entry {index}: native identity unknown; local ID retained"
                )
            records.append(
                dict(
                    source=source,
                    native_id=(url + "#" + identity)
                    if identity and source == "feed"
                    else identity,
                    url=link,
                    title=entry.findtext(prefix + "title"),
                    author=author,
                    text=content["text"],
                    raw_text=body or summary,
                    attachments=content["attachments"],
                    links=content["links"],
                    published_at=published_at,
                    published_label=label,
                    observed_at=observed_at,
                    metadata={
                        "scope": scope,
                        "feed_url": url,
                        "updated_at": published(updated),
                        "updated_label": updated,
                        "native_id_missing": identity is None,
                    },
                )
            )
        except (ValueError, TypeError) as exc:
            issues.append(f"entry {index}: {exc}")
    return records, issues, len(entries)


def fetch(request, *, timeout=15, transport=None):
    source = request["source"]
    urls = request.get("urls") or ([FEEDS[source][1]] if source in FEEDS else [])
    if not urls:
        raise ValueError("feed source needs explicit urls")
    with http.open_client(timeout=timeout, transport=transport) as client:
        for url in urls:
            try:
                response = http.request(client, "GET", url)
                documents, issues, total = parse(
                    response.body, response.url, source, response.received_at, request
                )
                yield dict(
                    documents=documents,
                    read=source_read(
                        source,
                        response,
                        request=request,
                        detail="current feed only, not a historical archive or representative sentiment sample",
                        status="partial" if issues else "ok",
                        issues=issues,
                        returned_entries=total,
                    ),
                )
            except http.FetchError as exc:
                yield {"read": dict(source=source, **exc.read)}
            except (ValueError, ET.ParseError) as exc:
                yield {
                    "read": dict(
                        source=source,
                        requested_at=stamp(),
                        received_at=stamp(),
                        status="failed",
                        detail=str(exc),
                    )
                }
