"""Comparable denominators, explicit relationships and source-native coverage."""

import json

import httpx
import pytest
from test_continuous import T, batch, facts

from marketsift import api, information, store
from marketsift.sources import etf, guba, http, nbs, pages
from marketsift.time import stamp


def test_guba_unread_discoveries_are_kept_without_false_mentions(tmp_path):
    payload = {
        "bar_code": "002946",
        "re": [
            dict(
                post_id=1,
                post_title="未知详情但保留发现",
                post_publish_time="2026-11-03 10:00:00",
            )
        ],
    }
    raw = ("<script>var article_list=" + json.dumps(payload) + ";</script>").encode()
    response = http.Download(
        "https://guba.eastmoney.com/list,002946.html", 200, {}, raw, stamp(T), stamp(T)
    )
    docs, links, issues = guba.parse(response)
    assert len(docs) == 1 and issues and not links
    assert docs[0]["metadata"]["scope"] == "listing title only"
    assert not docs[0]["metadata"]["detail_supported"]


@pytest.mark.parametrize("source", ["sse-etf", "szse-etf"])
def test_etf_native_units_pagination_and_empty_not_zero(tmp_path, source):
    def respond(r):
        if source == "sse-etf":
            return httpx.Response(
                200,
                json={
                    "pageHelp": {
                        "data": [
                            dict(
                                STAT_DATE="2026-11-02",
                                SEC_CODE="510300",
                                SEC_NAME="沪深300ETF",
                                TOT_VOL="1,234",
                            )
                        ],
                        "pageCount": 1,
                    }
                },
            )
        page = int(r.url.params["PAGENO"])
        return httpx.Response(
            200,
            json=[
                {
                    "metadata": {"pagecount": 2, "cols": {"current_size": "万份"}},
                    "data": [
                        dict(
                            size_date="2026-11-02",
                            fund_code="15991" + str(page),
                            security_short_name="ETF",
                            current_size="1,234",
                        )
                    ],
                }
            ],
        )

    db = tmp_path / "etf.sqlite"
    for b in etf.fetch(
        dict(source=source, since="2026-11-02", until="2026-11-03"),
        transport=httpx.MockTransport(respond),
    ):
        store.ingest(db, b)
    rows = facts(db, "series")["rows"]
    assert len(rows) == (1 if source == "sse-etf" else 2)
    assert rows[0]["value"] == 1234
    assert rows[0]["unit"] == (None if source == "sse-etf" else "10,000 shares")


def test_nbs_tables_keep_native_cells_and_coarse_publication(tmp_path):
    raw = '<meta name="PubDate" content="2026-11-03"><meta name="ArticleTitle" content="统计"><div class="detail-text-content"><div class="txt-content"><table><tr><th colspan="2">单位：%</th></tr><tr><td>指标</td><td>50.1</td></tr></table></div></div>'.encode()
    response = http.Download(
        "https://www.stats.gov.cn/sj/zxfb/202611/t20261103_123.html",
        200,
        {},
        raw,
        stamp(T),
        stamp(T),
    )
    docs, _, _ = nbs.parse(response)
    assert docs[0]["published_at"] is None
    assert docs[0]["metadata"]["native_tables"][0][0][0]["colspan"] == "2"
    db = tmp_path / "nbs.sqlite"
    store.ingest(db, batch(docs))
    tables = api.query(
        {"d": {"dataset": "documents"}}, emit={"d": ["id", "metadata"]}, database=db
    )["results"]["d"]["rows"]
    assert tables[0]["metadata"]["native_tables"][0][1][1]["text"] == "50.1"
    assert "native_tables" not in facts(db, "documents")["rows"][0]["metadata"]


def test_page_chooses_target_body_not_recommendation_cards(tmp_path):
    raw = '<h1>Target</h1><div data-testid="post__body">Original target text</div><article>Unrelated recommendation</article><article>More recommendations</article>'
    batches = list(
        pages.fetch(
            dict(source="page", urls=["https://cointelegraph.com/news/target"]),
            transport=httpx.MockTransport(lambda r: httpx.Response(200, text=raw)),
        )
    )
    assert batches[0]["documents"][0]["text"] == "Original target text"
    batches = list(
        pages.fetch(
            dict(source="page", urls=["https://example.test/news/target"]),
            transport=httpx.MockTransport(lambda r: httpx.Response(200, text=raw)),
        )
    )
    assert batches[0]["read"]["status"] == "failed"


def test_single_pass_literals_keep_longest_matches_boundaries_and_original_text():
    refs = [
        dict(
            instrument="one", symbol="BTC", name="甲公司", aliases=["甲", "Alpha Ltd"]
        ),
        dict(instrument="two", symbol="ALP", name="Alpha", aliases=["甲公司"]),
    ]
    text = "Alpha Ltd与甲公司，BTC；xBTC不能命中，İBC也不是BTC。"
    row = information.associate([dict(text=text, metadata={})], refs)[0]
    assert row["text"] == text
    assert row["mentions"] == {
        "Alpha Ltd": ["one"],
        "甲公司": ["one", "two"],
        "BTC": ["one"],
    }
