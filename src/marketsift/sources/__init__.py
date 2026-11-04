"""Static source capabilities and acquisition implementations."""

from . import (
    bls,
    cninfo,
    etf,
    feeds,
    fred,
    guba,
    jiuyangongshe,
    legacy,
    nbs,
    nyfed,
    ofr,
    okx,
    pages,
    pbc,
    sec,
    sina,
    taoguba,
    tdx,
    tencent,
    ths,
    treasury,
)
from .okx.candles import INTERVALS

SOURCES = {
    "cninfo": {
        "connected": True,
        "scope": "current official names/pinyin; all recognized code ranges or explicit stock keys, not listing status or historical validity",
    },
    "sina": {
        "connected": True,
        "scope": "public chronological feed; finite returned pages, not certified whole-site coverage",
    },
    "tdx": {
        "connected": True,
        "scope": "explicit exchange-qualified instruments; native five-minute bars; volume retains source units",
    },
    "sqlite": {
        "connected": True,
        "scope": "read-only selected history from an existing Ashare SQLite database; original availability unknown",
    },
    "archive": {
        "connected": True,
        "scope": "read existing plain material, minute snapshot or name reference; no package admission",
    },
    "xueqiu": {
        "connected": False,
        "scope": "previous public request returned a page shell without research text",
    },
}

for name, fetch, datasets, options in (
    ("sina", sina.fetch, ("info",), ()),
    ("tdx", tdx.fetch, ("prices",), ("hosts",)),
    ("cninfo", cninfo.fetch, ("instruments",), ()),
    ("sqlite", legacy.fetch, ("prices",), ("input",)),
    ("archive", legacy.fetch, ("info", "prices", "instruments"), ("input",)),
):
    SOURCES[name].update(
        fetch=fetch, datasets=datasets, defaults=datasets, options=options
    )
    SOURCES[name]["requires_range"] = name in {"sina", "tdx", "sqlite"}
    SOURCES[name]["filters"] = (
        ["since", "until"] if name == "sina" else ["instruments", "since", "until"]
    )
SOURCES["cninfo"]["filters"] = ["instruments"]
SOURCES["cninfo"]["instrument_prefixes"] = ["SH", "SZ", "BJ"]
SOURCES["tdx"].update(intervals=["5m"], bases=["trade"])


def _register(name, fetch, datasets, *, defaults=None, options=(), filters=(), **scope):
    SOURCES[name] = dict(
        connected=True,
        fetch=fetch,
        datasets=datasets,
        defaults=defaults or datasets,
        options=options,
        filters=filters,
        **scope,
    )


_register(
    "okx",
    okx.fetch,
    ("instruments", "prices", "series", "trades"),
    defaults=("instruments",),
    filters=("instruments", "since", "until"),
    options=("inst_types", "metrics", "archive", "archive_urls", "archive_date_aggr"),
    intervals=list(INTERVALS),
    bases=["trade", "mark", "index"],
    scope="Public product rules, native candles, funding events, current OI/tickers and trades; REST and native bulk downloads have independent availability",
)
for name, fetch, required in (
    ("fred", fred.fetch, True),
    ("ofr", ofr.fetch, False),
    ("bls", bls.fetch, True),
):
    _register(
        name,
        fetch,
        ("series",),
        filters=("series_ids", "since", "until"),
        requires_range=required,
        range_clock="date",
        scope="Native statistical dates and units; current retrieval revision, not a historical vintage",
    )
SOURCES["fred"]["known_series"] = {
    k: dict(zip(("name", "unit", "frequency", "measure"), v))
    for k, v in fred.SERIES.items()
}
for name, fetch, required in (
    ("treasury", treasury.fetch, True),
    ("nyfed", nyfed.fetch, False),
):
    _register(
        name,
        fetch,
        ("info", "series"),
        filters=("since", "until"),
        requires_range=required,
        range_clock="date",
        scope="Native official rows and scalar fields; unknown scaling remains unknown",
    )
for name, datasets in (("sec-filings", ("info",)), ("sec-concept", ("series",))):
    _register(
        name,
        sec.fetch,
        datasets,
        filters=("since", "until"),
        options=("cik", "concept", "user_agent"),
        range_clock="date",
        scope="Recent submissions index or unit/accession-specific XBRL facts; no restatement merging or full filing-body claim",
    )
for name, (category, url) in feeds.FEEDS.items():
    _register(
        name,
        feeds.fetch,
        ("info",),
        filters=("since", "until"),
        category=category,
        url=url,
        scope="Native finite RSS/Atom feed; full content, summary and unknown publication times distinguished",
    )
_register(
    "feed",
    feeds.fetch,
    ("info",),
    filters=("urls", "since", "until"),
    scope="Explicit RSS/Atom URLs, without a whole-site coverage claim",
)
for name in ("page", "discourse"):
    _register(
        name,
        pages.fetch,
        ("info",),
        filters=("urls",),
        options=("selector", "exclude", "all_posts"),
        scope="Explicit article HTML or known public Discourse topic/posts; turbohtml cleaning, retained links and image references",
    )

for name, provider in (
    ("jiuyangongshe", jiuyangongshe),
    ("guba", guba),
    ("taoguba", taoguba),
    ("ths", ths),
    ("pbc", pbc),
    ("nbs", nbs),
):
    _register(
        name,
        provider.fetch,
        ("info",),
        filters=(
            "urls",
            "since",
            "until",
            *(("instruments",) if name == "guba" else ()),
        ),
        options=("follow_links",),
        scope="Public returned listing/article/replies; all known detail links followed unless explicitly disabled; finite discovery, images not OCR",
    )
for name in ("sse-etf", "szse-etf"):
    _register(
        name,
        etf.fetch,
        ("series",),
        filters=("instruments", "since", "until"),
        requires_range=True,
        range_clock="date",
        scope="Official daily ETF shares, native units/revisions; not national-team holdings or net inflow",
    )
_register(
    "tencent",
    tencent.fetch,
    ("series",),
    filters=("instruments",),
    scope="Explicit current quote snapshots; not historical five-minute bars or independent verification",
)
