"""One HTML parser and source-bound cleaning; no article scoring or I/O."""

from copy import deepcopy
from html import escape
from urllib.parse import urljoin, urlsplit

import turbohtml

from ..types import Content


def parse_html(payload, *, encoding=None):
    if encoding and isinstance(payload, bytes):
        payload = payload.decode(encoding)
    return turbohtml.parse(payload)


def _table_tag(node, content):
    # Pipe tables cannot retain merged headers. Keep compact structural HTML.
    if node.tag in {"td", "th", "caption"} and not node.select("a,img,table"):
        content = escape(node.text)
    attrs = "".join(
        f' {key}="{escape(node.attrs[key], quote=True)}"'
        for key in ("rowspan", "colspan")
        if key in node.attrs
    )
    return f"\n<{node.tag}{attrs}>{content}</{node.tag}>\n"


MARKDOWN = turbohtml.Markdown(
    converters={
        tag: _table_tag
        for tag in ("table", "thead", "tbody", "tfoot", "tr", "td", "th", "caption")
    }
)


def _web_url(base, reference):
    url = urljoin(base, reference)
    return url if urlsplit(url).scheme in {"http", "https"} else None


def clean_node(node, *, base_url, exclude=()) -> Content:
    cleaned = deepcopy(node)
    cleaned.remove(",".join(("script", "style", *exclude)))
    cleaned.resolve_links(base_url)
    links, attachments = [], []
    for anchor in cleaned.select("a[href]"):
        if url := _web_url(base_url, anchor.attrs["href"]):
            links.append(url)
        else:
            anchor.attrs.pop("href", None)
    images = list(cleaned.select("img"))
    if getattr(cleaned, "tag", None) == "img":
        images.insert(0, cleaned)
    for image in images:
        reference = (
            image.attrs.get("data-original")
            or image.attrs.get("src2")
            or image.attrs.get("src")
        )
        if reference and (url := _web_url(base_url, reference)):
            image.attrs["src"] = url
            attachments.append(url)
    return {
        "text": cleaned.to_markdown(MARKDOWN).strip(),
        "links": list(dict.fromkeys(links)),
        "attachments": list(dict.fromkeys(attachments)),
    }


def clean_html(markup, *, base_url, selector=None, exclude=()) -> Content:
    tree = parse_html(markup)
    node = tree.select_one(selector) if selector else tree
    if node is None:
        return {
            "text": "",
            "links": [],
            "attachments": [],
            "issues": [f"No body matches {selector!r}"],
        }
    return clean_node(node, base_url=base_url, exclude=exclude)
