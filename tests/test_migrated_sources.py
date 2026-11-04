"""Source semantics and continuous readback across the migrated boundaries."""

import io
import sqlite3
import zipfile

import httpx
import pytest
from test_continuous import facts

from marketsift import api, store
from marketsift.sources import feeds, fred, okx, pages, sec, treasury

T = "2026-11-03T08:00:00Z"
P = dict(
    instId="BTC-USDT-SWAP",
    instType="SWAP",
    ctType="linear",
    ctVal="0.01",
    ctValCcy="BTC",
    settleCcy="USDT",
    ctMult="1",
    instCategory="1",
    state="live",
    uly="BTC-USDT",
)


def save(db, batches):
    return [store.ingest(db, b) for b in batches]


def test_populated_schema_one_migration_preserves_ids_and_enforces_refs(tmp_path):
    db = tmp_path / "old.sqlite"
    with sqlite3.connect(db) as c:
        c.executescript(store.DDL)
        c.executescript(
            "ALTER TABLE bar ADD COLUMN basis TEXT NOT NULL DEFAULT 'trade'; ALTER TABLE bar ADD COLUMN read_id INTEGER REFERENCES source_read(id); ALTER TABLE document_version ADD COLUMN read_id INTEGER REFERENCES source_read(id); ALTER TABLE source_read ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'; PRAGMA user_version=1;"
        )
        c.execute(
            "INSERT INTO instrument(instrument,name) VALUES('SZ000001','old name')"
        )
        c.execute("INSERT INTO document VALUES(7,'old','native','https://test')")
        c.execute(
            "INSERT INTO document_version(id,document_id,observed_at,text,attachments,links,metadata) VALUES(19,7,?,'正文','[]','[]','{}')",
            (T,),
        )
    with store.connect(db, write=True) as c:
        assert c.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert (
            c.execute(
                "SELECT document_id FROM document_version WHERE id=19"
            ).fetchone()[0]
            == 7
        )
        assert store.lookup(c)[0]["name"] == "old name"
        with pytest.raises(sqlite3.IntegrityError):
            c.execute(
                "INSERT INTO document_version(document_id,observed_at,text,attachments,links,metadata) VALUES(99,?,'bad','[]','[]','{}')",
                (T,),
            )
    with store.connect(db) as c:
        assert c.execute("PRAGMA user_version").fetchone()[0] == 2
        assert not c.execute("PRAGMA foreign_key_check").fetchall()


def test_fred_revisions_periods_missingness_and_asof(tmp_path):
    db = tmp_path / "facts.sqlite"
    req = dict(
        source="fred",
        datasets=["series"],
        series_ids=["CPIAUCSL"],
        since="2026-09-01",
        until="2026-11-01",
    )

    def download(value):
        return httpx.MockTransport(
            lambda r: httpx.Response(
                200,
                text=f"observation_date,CPIAUCSL\n2026-09-01,{value}\n2026-10-01,.\n",
            )
        )

    batches = list(fred.fetch(req, transport=download("321")))
    for b in batches:
        b["read"]["received_at"] = T
        for v in b["series_values"]:
            v["observed_at"] = T
    save(db, batches)
    assert save(db, batches)[0]["series_values_added"] == 0
    before = facts(db, "series", as_of=T)
    assert before["rows"][0]["period_end"] == "2026-10-01"
    assert before["rows"][1]["state"] == "missing"
    batches[0]["series_values"][0].update(value=322, observed_at="2026-11-03T09:00:00Z")
    batches[0]["read"]["received_at"] = "2026-11-03T09:00:00Z"
    save(db, batches)
    assert facts(db, "series", as_of=T)["rows"][0]["value"] == 321
    assert facts(db, "series")["rows"][0]["value"] == 322
    assert len(facts(db, "series", time={"axis": "published"})["unplaced"]) == 2


def test_okx_units_confirmations_funding_and_trades_share_context(tmp_path):
    since, until = "2026-11-03T00:00:00Z", "2026-11-03T00:10:00Z"
    from marketsift.sources.common import milliseconds

    ts = milliseconds(since)

    def respond(r):
        path = r.url.path
        if path.endswith("/instruments"):
            data = [P]
        elif path.endswith("/history-candles"):
            data = [[str(ts), "10", "12", "9", "11", "2", "0.02", "0.22", "0"]]
        elif path.endswith("/open-interest"):
            data = [dict(ts=str(ts), oi="20", oiCcy="0.2", oiUsd="200")]
        elif path.endswith("/funding-rate-history"):
            data = [
                dict(fundingTime=str(ts), fundingRate="0.0001", realizedRate="0.00009")
            ]
        elif path.endswith("/history-trades"):
            data = [dict(ts=str(ts), tradeId="a", px="11", sz="2", side="buy")]
        else:
            raise AssertionError(path)
        return httpx.Response(200, json=dict(code="0", data=data))

    req = dict(
        source="okx",
        datasets=["prices", "series", "trades"],
        instruments=["OKX:SWAP:BTC-USDT-SWAP"],
        since=since,
        until=until,
    )
    db = tmp_path / "okx.sqlite"
    batches = list(okx.fetch(req, transport=httpx.MockTransport(respond)))
    save(db, batches)
    views = api.query(
        {
            "p": {"dataset": "bars"},
            "s": {"dataset": "series"},
            "t": {"dataset": "trades"},
            "r": {"dataset": "references"},
        },
        database=db,
    )["results"]
    assert views["r"]["rows"][0]["rules"]["contract_value"] == 0.01
    assert views["p"]["rows"][0]["complete"] is False
    values = {r["series"]: r for r in views["s"]["rows"]}
    assert values["okx:BTC-USDT-SWAP:open_interest_contracts"]["unit"] == "contracts"
    assert values["okx:BTC-USDT-SWAP:funding_rate"]["value"] == 0.0001
    assert views["t"]["rows"][0]["units"]["size"] == "contracts"
    assert all(
        r["trade_versions_added"] == 0 and r["bar_versions_added"] == 0
        for r in save(db, batches)
    )
    equity = okx.reference(
        dict(
            P,
            instType="FUTURES",
            instId="ABC-USDT",
            instCategory="3",
            ruleType="xperp",
            state="suspend",
        ),
        T,
    )
    assert equity["rules"]["asset_class"] == "stocks" and equity["rules"]["funding"]


@pytest.mark.parametrize("funding", [False, True])
def test_native_okx_archive_uses_same_store_without_packages(tmp_path, funding):
    from marketsift.sources.common import milliseconds

    ts = milliseconds("2026-11-03T00:00:00Z")
    data = io.BytesIO()
    csv = (
        f"instrument_name,funding_time,funding_rate\nBTC-USDT-SWAP,{ts},.0001\n"
        if funding
        else f"instrument_name,trade_id,created_time,price,size,side,source\nBTC-USDT-SWAP,a,{ts},11,2,buy,0\n"
    )
    with zipfile.ZipFile(data, "w") as z:
        z.writestr("native.csv", csv)

    def respond(r):
        if r.url.path.endswith("/instruments"):
            return httpx.Response(200, json=dict(code="0", data=[P]))
        if r.method == "POST":
            return httpx.Response(
                200,
                json=dict(
                    code="0",
                    data={
                        "details": [
                            {
                                "groupDetails": [
                                    {"url": "https://static.okx.com/native.zip"}
                                ]
                            }
                        ]
                    },
                ),
            )
        return httpx.Response(200, content=data.getvalue())

    req = dict(
        source="okx",
        datasets=["series" if funding else "trades"],
        instruments=["OKX:SWAP:BTC-USDT-SWAP"],
        since="2026-11-03T00:00:00Z",
        until="2026-11-03T01:00:00Z",
        options={"archive": True, "metrics": ["funding_history"]},
    )
    db = tmp_path / "archive.sqlite"
    save(db, okx.fetch(req, transport=httpx.MockTransport(respond)))
    assert (
        len(facts(db, "series")["rows"] if funding else facts(db, "trades")["rows"])
        == 1
    )


def test_atom_and_discourse_keep_text_images_dates_and_all_replies(tmp_path):
    payload = b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>x</id><title>T</title><updated>2026-11-03T01:00:00Z</updated><content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml"><p>Chinese &#20013;&#25991;</p><img src="/a.png"/></div></content></entry></feed>'
    records, issues, _ = feeds.parse(payload, "https://test/feed", "feed", T, {})
    assert not issues and records[0]["published_at"] is None
    assert "中文" in records[0]["text"] and records[0]["attachments"] == [
        "https://test/a.png"
    ]
    post = lambda i: dict(
        id=i, post_number=i, created_at=T, username="u", cooked=f"<p>reply {i}</p>"
    )

    def respond(r):
        if r.url.path.endswith("/posts.json"):
            data = {"post_stream": {"posts": [post(2)]}}
        else:
            data = dict(
                id=123, title="topic", post_stream=dict(posts=[post(1)], stream=[1, 2])
            )
        return httpx.Response(200, json=data)

    req = dict(
        source="discourse",
        urls=["https://gov.uniswap.org/t/topic/123"],
        options={"all_posts": True},
    )
    batches = list(pages.fetch(req, transport=httpx.MockTransport(respond)))
    assert all(b["read"]["status"] == "ok" for b in batches)
    db = tmp_path / "posts.sqlite"
    save(db, batches)
    assert len(facts(db, "documents")["rows"]) == 2
    related = api.query(
        {
            "seed": {
                "dataset": "documents",
                "ids": [facts(db, "documents")["rows"][0]["id"]],
            }
        },
        relations={"t": {"kind": "thread", "input": "seed"}},
        database=db,
    )
    assert len(related["results"]["t"]["rows"]) == 1


def test_treasury_preserves_raw_accounts_and_unknown_scaling(tmp_path):
    def respond(r):
        page = r.url.params["page[number]"]
        return httpx.Response(
            200,
            json={
                "meta": {
                    "total-pages": 2,
                    "dataTypes": {"amount": "CURRENCY"},
                    "labels": {"amount": "Amount"},
                },
                "data": [
                    dict(record_date="2026-11-02", account_type=page, amount="10")
                ],
            },
        )

    batches = list(
        treasury.fetch(
            dict(datasets=["info", "series"], since="2026-11-01", until="2026-11-03"),
            transport=httpx.MockTransport(respond),
        )
    )
    db = tmp_path / "treasury.sqlite"
    save(db, batches)
    assert len(facts(db, "documents")["rows"]) == 2
    assert all(v["unit"] is None for v in facts(db, "series")["rows"])


def test_sec_accessions_preserved_for_same_statistical_period(tmp_path):
    data = dict(
        cik=320193,
        taxonomy="us-gaap",
        tag="Assets",
        units={
            "USD": [
                dict(end="2026-09-30", val=10, accn="a", filed="2026-10-01"),
                dict(end="2026-09-30", val=11, accn="b", filed="2026-11-01"),
            ]
        },
    )
    batches = sec.fetch(
        dict(source="sec-concept", options={"cik": "320193", "concept": "Assets"}),
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=data)),
    )
    db = tmp_path / "sec.sqlite"
    save(db, batches)
    values = facts(db, "series")["rows"]
    assert len(values) == 2 and len({v["native_id"] for v in values}) == 2
    assert all(v["published_at"] is None for v in values)


def test_series_ties_definition_units_and_trade_correction(tmp_path):
    db = tmp_path / "versions.sqlite"
    definition = dict(series="test:rate", source="test", unit="percent")
    point = dict(
        series="test:rate", period_kind="date", period_start="2026-11-01", value=1
    )
    tick = dict(
        source="test",
        instrument="OKX:SPOT:BTC-USDT",
        native_id="a",
        ts=T,
        price=10,
        size=1,
        observed_at=T,
    )
    receipt = dict(source="test", requested_at=T, received_at=T, status="ok")
    store.ingest(
        db,
        dict(
            series=[definition],
            series_values=[point, dict(point, metadata={"additional": "reference"})],
            trades=[tick],
            read=receipt,
        ),
    )
    assert len(facts(db, "series")["rows"]) == 1
    store.ingest(
        db,
        dict(series=[definition], series_values=[dict(point, value=2)], read=receipt),
    )
    assert all(v["conflict"] for v in facts(db, "series")["rows"])
    later = "2026-11-03T09:00:00Z"
    store.ingest(
        db,
        dict(
            series=[dict(definition, unit="fraction")],
            series_values=[dict(point, value=0.01)],
            trades=[dict(tick, ts=later, observed_at=later)],
            read=dict(receipt, received_at=later),
        ),
    )
    assert {
        s["unit"] for s in facts(db, "series_definitions", versions="all")["rows"]
    } == {
        "percent",
        "fraction",
    }
    assert not facts(db, "trades", time={"axis": "event", "until": later})["rows"]
    assert (
        len(
            facts(db, "trades", as_of=T, time={"axis": "event", "until": later})["rows"]
        )
        == 1
    )
    assert facts(db, "series", time={"axis": "period", "since": T})["unplaced"]
