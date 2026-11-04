"""Observable continuous storage, source boundaries and agent-facing queries."""

import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import httpx

from marketsift import api, store


def facts(database, dataset, *, as_of=None, **spec):
    return api.query(
        {"x": dict(dataset=dataset, **spec)}, database=database, as_of=as_of
    )["results"]["x"]


from marketsift.sources import cninfo, legacy, sina, tdx

TZ = timezone(timedelta(hours=8))
T = datetime(2026, 11, 3, 10, tzinfo=TZ)


def batch(documents=(), bars=(), instruments=(), at=T):
    return {
        "documents": list(documents),
        "bars": list(bars),
        "instruments": list(instruments),
        "read": {
            "source": "example",
            "requested_at": at,
            "received_at": at,
            "status": "ok",
            "since": T - timedelta(hours=1),
            "until": T + timedelta(hours=1),
        },
    }


def document(text="新乳业的新信息", at=T):
    return {
        "source": "example",
        "native_id": "1",
        "url": "https://example.test/1",
        "text": text,
        "observed_at": at,
        "published_at": T - timedelta(minutes=30),
    }


def bar(code="SZ002946", at=T, complete=True):
    return {
        "source": "example",
        "instrument": code,
        "interval": "5m",
        "start": T - timedelta(minutes=5),
        "end": T,
        "session": "2026-11-03",
        "observed_at": at,
        "requested_at": at,
        "complete": complete,
        "open": 16.0,
        "high": 16.3,
        "low": 15.9,
        "close": 16.2,
        "amount": 1620.0,
        "volume": 100.0,
        "units": {"amount": "CNY"},
    }


def test_bulk_local_history_preserves_all_instruments_and_rows(tmp_path):
    old = tmp_path / "old.sqlite"
    with sqlite3.connect(old) as con:
        con.execute(
            "CREATE TABLE minute5(code TEXT,trade_date INTEGER,bar_time INTEGER,open REAL,high REAL,low REAL,close REAL,amount REAL,volume REAL)"
        )
        rows = [
            (f"sh{600000 + i}", 20261102, minute, 10, 11, 9, 10.5, 105, 10)
            for i in range(136)
            for minute in [*range(575, 691, 5), *range(785, 901, 5)]
        ]
        rows.append(("sh600000", 20261102, 780, 10, 10, 10, 10, 0, 0))
        con.executemany("INSERT INTO minute5 VALUES(?,?,?,?,?,?,?,?,?)", rows)
    original = old.read_bytes()
    db = tmp_path / "continuous.sqlite"
    for item in legacy.sqlite_history(
        old, "2026-11-02T09:30+08:00", "2026-11-02T15:01+08:00"
    ):
        store.ingest(db, item)
    with store.connect(db) as con:
        selected = facts(db, "bars")["rows"]
        regular = [r for r in selected if r["metadata"].get("session_role") != "other"]
        result = api.query(
            {"p": {"dataset": "bars", "ids": [r["id"] for r in regular]}},
            calculations={"s": {"kind": "summarize", "input": "p", "paths": True}},
            database=db,
        )["results"]["s"]
        assert len(selected) == 6529
        assert len(selected) - len(regular) == 1
        assert len(result["rows"]) == 136
        assert sum(r["amount"] for r in result["rows"]) == 6528 * 105
        assert len(store.lookup(con)) == 136
        assert all(r["bars"] == 48 for r in result["rows"])
    assert old.read_bytes() == original


def test_official_names_join_known_stock_keys_without_relabeling_an_index(tmp_path):
    refs = [tdx.instrument(c) for c in ("SZ000001", "SH000001", "SZ000568")]
    rows = [
        {"code": "000001", "category": "A股", "zwjc": "平安银行", "pinyin": "payh"},
        {"code": "000568", "category": "A股", "zwjc": "泸州老窖", "pinyin": "lzlj"},
    ]
    transport = httpx.MockTransport(
        lambda r: httpx.Response(200, json={"stockList": rows})
    )
    db = tmp_path / "continuous.sqlite"
    store.ingest(db, batch(instruments=refs))
    for result in cninfo.fetch(
        {"instruments": [r["instrument"] for r in refs]}, transport=transport
    ):
        store.ingest(db, result)
    with store.connect(db) as con:
        assert store.lookup(con, "payh")[0]["instrument"] == "SZ000001"
        assert store.lookup(con, "SH000001")[0]["name"] is None
        assert store.lookup(con, "泸州")[0]["instrument"] == "SZ000568"


def test_missing_turnover_does_not_become_zero(tmp_path):
    db = tmp_path / "continuous.sqlite"
    store.ingest(db, batch(bars=[dict(bar(), amount=None, volume=None)]))
    result = api.query(
        {"p": {"dataset": "bars"}},
        calculations={"s": {"kind": "summarize", "input": "p"}},
        database=db,
        as_of=T + timedelta(minutes=1),
    )["results"]["s"]
    assert result["rows"][0]["amount"] is None
    assert result["rows"][0]["volume"] is None


def test_sina_pagination_retains_valid_items_and_reports_later_failure(tmp_path):
    def handle(request):
        page = request.url.params["page"]
        if page == "2":
            return httpx.Response(429)
        rows = [
            None,
            {"id": 7, "create_time": "2026-11-03 10:00:00", "rich_text": ""},
            {
                "id": 8,
                "create_time": "2026-11-03 09:55:00",
                "rich_text": "<script>not body</script><p>新乳业原文</p><img src='https://example.test/image'>",
            },
            {"create_time": "unknown", "rich_text": "时间与原生标识未知，保留原文"},
        ]
        return httpx.Response(200, json={"result": {"data": {"feed": {"list": rows}}}})

    db = tmp_path / "continuous.sqlite"
    batches = list(
        sina.fetch(
            {"since": "2026-11-03T09:00+08:00", "until": "2026-11-03T11:00+08:00"},
            transport=httpx.MockTransport(handle),
        )
    )
    for item in batches:
        store.ingest(db, item)
    assert batches[-1]["read"]["status"] == "failed"
    with store.connect(db):
        rows = facts(db, "documents")["rows"]
        rows.sort(key=lambda r: r["published_at"] or "z")
        assert len(rows) == 2 and "新乳业原文" in rows[0]["text"]
        assert rows[0]["attachments"] == ["https://example.test/image"]
        assert rows[1]["published_at"] is None
        assert rows[1]["metadata"]["native_id_missing"]
        assert facts(db, "acquisitions")["rows"][-1]["status"] == "failed"


def test_tdx_source_yields_rows_without_a_store_and_marks_forming(monkeypatch):
    class Session:
        host, port = "example", 7709

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(tdx.wire, "open_session", lambda *a: Session())
    monkeypatch.setattr(
        tdx,
        "stamp",
        lambda value=None: (
            "2026-11-03T02:02:00.000000+00:00" if value is None else store.stamp(value)
        ),
    )
    page = [
        tdx.wire.Bar("2026-11-03 09:35", 16, 16.1, 16.2, 15.9, 10, 161),
        tdx.wire.Bar("2026-11-03 10:05", 16.1, 16.2, 16.3, 16, 20, 324),
    ]
    monkeypatch.setattr(tdx.wire, "bars", lambda *a: page)
    batches = list(
        tdx.fetch(
            {
                "instruments": ["SZ002946"],
                "since": "2026-11-03T09:30+08:00",
                "until": "2026-11-03T10:04+08:00",
            }
        )
    )
    assert len(batches) == 1
    assert [r["complete"] for r in batches[0]["bars"]] == [True, False]
    assert batches[0]["bars"][0]["end"] == "2026-11-03T01:35:00.000000+00:00"


def test_installed_entry_reads_without_creating_a_database(tmp_path):
    db = tmp_path / "absent.sqlite"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "marketsift",
            "query",
            "--database",
            str(db),
            "--spec",
            "-",
        ],
        input="{}",
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    assert result.returncode == 2 and "absent.sqlite" in result.stderr
    assert not db.exists()
    result = subprocess.run(
        [sys.executable, "-m", "marketsift", "sources", "--database", str(db)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    sources = json.loads(result.stdout)["sources"]
    assert sources["sina"]["connected"] and sources["pbc"]["connected"]
