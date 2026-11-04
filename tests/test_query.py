"""Public composition: facts, relations and measurements obey independent scope."""

import json
import sqlite3
from datetime import timedelta

import httpx
import pytest
from test_continuous import T, bar, batch, document

from marketsift import api, prices, store
from marketsift.sources import http
from marketsift.time import stamp


def select(db, dataset, **spec):
    return api.query(
        {"x": dict(dataset=dataset, **spec)}, database=db, as_of="2030-01-01T00:00Z"
    )["results"]["x"]


def test_exact_latest_time_axes_and_projection(tmp_path):
    db = tmp_path / "facts.sqlite"
    store.ingest(db, batch([document()]))
    old = select(db, "documents")["rows"][0]
    store.ingest(db, batch([document("replacement", T + timedelta(minutes=1))]))
    assert not select(db, "documents", where={"text": "新信息"})["rows"]
    assert select(db, "documents", ids=[old["id"]])["rows"][0]["text"] == old["text"]
    with pytest.raises(ValueError, match="exact ids"):
        select(db, "documents", ids=[old["id"]], where={"text": "x"})
    assert select(db, "documents", ids=[999])["missing_ids"] == [999]
    assert len(select(db, "documents", versions="all")["rows"]) == 2
    store.ingest(
        db,
        batch(
            [
                dict(
                    document(),
                    native_id="date",
                    published_at=None,
                    published_label="2026-11-03",
                )
            ]
        ),
    )
    q = {
        "dataset": "documents",
        "time": {
            "axis": "published",
            "since": "2026-11-03T00:00+08:00",
            "until": "2026-11-04T00:00+08:00",
        },
    }
    r = api.query({"d": q}, database=db, emit={"d": ["id", "text"]})["results"]["d"]
    assert len(r["rows"]) == 1 and len(r["unplaced"]) == 1
    assert set(r["rows"][0]) == {"id", "text"}
    assert len(select(db, "acquisitions")["rows"]) == 3
    assert (
        len(
            select(
                db,
                "documents",
                time={
                    "axis": "observed",
                    "since": T,
                    "until": T + timedelta(seconds=1),
                },
            )["rows"]
        )
        == 1
    )


def test_conflict_forming_and_same_snapshot(tmp_path, monkeypatch):
    db = tmp_path / "facts.sqlite"
    store.ingest(db, batch([document()], bars=[bar(complete=False)]))
    original = store.select

    def interleave(con, spec, **kwargs):
        result = original(con, spec, **kwargs)
        if spec["dataset"] == "documents":
            store.ingest(db, batch(bars=[bar(at=T + timedelta(seconds=1))]))
        return result

    monkeypatch.setattr(store, "select", interleave)
    spec = {"d": {"dataset": "documents"}, "p": {"dataset": "bars"}}
    args = {
        "calculations": {"s": {"kind": "summarize", "input": "p"}},
        "database": db,
        "as_of": T + timedelta(minutes=1),
    }
    r = api.query(spec, **args)["results"]
    assert not r["s"]["rows"] and r["s"]["excluded"][0]["exclusion"] == "forming"
    monkeypatch.setattr(store, "select", original)
    assert (
        api.query({"p": spec["p"]}, **args)["results"]["s"]["rows"][0]["close"] == 16.2
    )
    store.ingest(db, batch(bars=[dict(bar(at=T + timedelta(seconds=1)), close=18)]))
    assert all(r["conflict"] for r in select(db, "bars")["rows"])
    store.ingest(db, batch(bars=[dict(bar(at=T + timedelta(seconds=2)), close=18)]))
    assert not select(db, "bars")["rows"][0]["conflict"]


def test_local_metrics_units_missing_members_and_standalone_consistency(tmp_path):
    db = tmp_path / "facts.sqlite"
    rows = []
    for day in (0, 1):
        for code in ("A", "B", "INDEX"):
            for i in (0, 1):
                rows.append(
                    dict(
                        bar(code),
                        start=T + timedelta(days=day, minutes=5 * i),
                        end=T + timedelta(days=day, minutes=5 * (i + 1)),
                        observed_at=T + timedelta(days=2),
                        open=10,
                        close=11 if code == "A" else 10,
                        units={"price": "CNY", "amount": "CNY"},
                        amount=100 if not day or code != "B" else 20,
                    )
                )
    store.ingest(db, batch(bars=rows))
    windows = {
        str(day): {
            "since": stamp(T + timedelta(days=day)),
            "until": stamp(T + timedelta(days=day, minutes=10)),
        }
        for day in (0, 1)
    }
    spec = {"p": {"dataset": "bars"}}
    calc = {
        "kind": "compare",
        "input": "p",
        "pool": ["A", "B"],
        "groups": {"ab": ["A", "B"]},
        "benchmark": "INDEX",
        "windows": windows,
        "pairs": [["0", "1"]],
    }

    def run(c):
        return api.query(
            spec, calculations={"c": c}, database=db, as_of=T + timedelta(days=3)
        )["results"]["c"]["comparisons"][0]

    full = run(calc)
    assert full["windows"]["0"]["pool"]["amount"] == 400
    assert full["windows"]["1"]["pool"]["amount"] == 240
    assert full["changes"][0]["members"]["A"]["amount_ratio"] == 0
    assert full["windows"]["1"]["members"]["A"]["amount_share"] > 0.5
    missing = run(dict(calc, pool=["A", "B", "C"]))
    assert missing["windows"]["1"]["pool"]["amount"] is None
    assert missing["windows"]["1"]["groups"]["ab"]["amount"] == 240
    assert missing["changes"][0]["members"]["A"] == full["changes"][0]["members"]["A"]
    selected = select(db, "bars")["rows"]
    standalone = prices.summarize(
        selected, window=windows["1"], as_of=T + timedelta(days=3)
    )
    assert next(r for r in standalone["rows"] if r["instrument"] == "A") == dict(
        source="example",
        instrument="A",
        interval="5m",
        basis="trade",
        **full["windows"]["1"]["members"]["A"]["observed"],
    )
    with store.connect(db, write=True) as con:
        con.execute(
            "UPDATE bar SET units=? WHERE instrument='A'",
            (json.dumps({"price": "CNY"}),),
        )
    unknown = run(calc)
    assert unknown["windows"]["1"]["members"]["A"]["excess_return"] == pytest.approx(
        0.1
    )
    assert unknown["windows"]["1"]["pool"]["amount"] is None
    short = prices.summarize(
        selected[:1],
        window={"since": T, "until": T + timedelta(minutes=15)},
        as_of=T + timedelta(days=3),
    )["rows"][0]
    assert short["amount"] == 100 and short["window_amount"] is None
    assert (
        short["window_return"] is None
        and short["coverage"]["expected_session_slots"] is None
    )


def test_period_revision_and_available_observation(tmp_path):
    db = tmp_path / "facts.sqlite"
    definition = {"series": "s", "source": "test", "unit": "percent"}
    month = {
        "series": "s",
        "period_kind": "month",
        "period_start": "2026-01-01",
        "period_end": "2026-02-01",
        "value": 1,
    }
    year = dict(month, period_kind="year", period_end="2027-01-01", value=10)
    b = batch(at=T)
    b.update(series=[definition], series_values=[month, year])
    store.ingest(db, b)
    b = batch(at=T + timedelta(hours=1))
    b.update(series=[definition], series_values=[dict(month, value=2)])
    store.ingest(db, b)
    r = api.query(
        {"s": {"dataset": "series", "versions": "all"}},
        calculations={
            "r": {"kind": "change", "input": "s", "axis": "revision"},
            "p": {"kind": "change", "input": "s", "axis": "period"},
            "at": {"kind": "available_asof", "input": "s", "at": T},
        },
        database=db,
    )["results"]
    assert len(r["r"]["rows"]) == 1 and r["r"]["rows"][0]["difference"] == 1
    assert not r["p"]["rows"]
    assert {v["value"] for v in r["at"]["rows"]} == {1, 10}
    assert (
        len(
            select(
                db,
                "series",
                time={"axis": "period", "since": T, "until": T + timedelta(hours=1)},
            )["unplaced"]
        )
        == 2
    )


def test_native_relations_ambiguity_scope_and_event_response(tmp_path):
    db = tmp_path / "facts.sqlite"
    refs = [
        dict(instrument=c, symbol=c, name=n, aliases=["shared"])
        for c, n in [("A", "甲公司"), ("B", "乙公司")]
    ]
    docs = [
        dict(
            document("甲公司与乙公司。shared。"),
            native_id="p",
            links=["https://test/original", "https://missing"],
            attachments=["https://image"],
            published_at=T,
        ),
        dict(
            document("reply"),
            native_id="r",
            metadata={"parent_native_id": "p"},
            url="https://reply",
        ),
        dict(document("original"), native_id="o", url="https://test/original"),
    ]
    bars = [
        dict(
            bar(c),
            start=T + timedelta(minutes=m),
            end=T + timedelta(minutes=m + 5),
            units={"price": "CNY", "amount": "CNY"},
        )
        for c in ["A", "B"]
        for m in [-5, 0, 5]
    ]
    store.ingest(db, batch(docs, bars, refs))
    selections = {
        "seed": {"dataset": "documents", "where": {"native_ids": ["p"]}},
        "all": {"dataset": "documents"},
        "prices": {"dataset": "bars"},
    }
    r = api.query(
        selections,
        relations={
            "m": {"kind": "literal_mentions", "input": "seed"},
            "replies": {"kind": "replies", "input": "seed", "direction": "in"},
            "links": {"kind": "outbound_link", "input": "seed", "scope": "all"},
        },
        calculations={
            "groups": {"kind": "cooccurrence", "input": "m"},
            "response": {
                "kind": "event_response",
                "events": "m",
                "prices": "prices",
                "anchor": "published",
                "before": "5m",
                "after": "7m",
            },
        },
        database=db,
        as_of=T + timedelta(minutes=1),
    )["results"]
    assert r["m"]["rows"][0]["mentions"]["shared"] == ["A", "B"]
    assert len(r["replies"]["rows"]) == 1 and len(r["links"]["rows"]) == 1
    assert len(r["links"]["unresolved"]) == 2
    e = r["response"]["rows"][0]
    assert e["after_pending"] and e["subjects"] == ["A", "B"]
    p = e["response"]["comparisons"][0]["windows"]["after"]
    assert p["members"]["A"]["observed"] is None
    assert e["response"]["excluded"]
    assert len(r["groups"]["groups"]) == 1
    r = api.query(
        {"p": selections["prices"], "d": selections["all"]},
        relations={"news": {"kind": "documents_for", "input": "p", "scope": "d"}},
        database=db,
        emit=["news"],
    )
    assert len(r["results"]["news"]["rows"]) == 1


def test_acquisition_partial_progress_and_cli(tmp_path, monkeypatch, capsys):
    from marketsift.cli import main

    db = tmp_path / "facts.sqlite"

    def fetch(request, *, timeout):
        yield batch([document()])
        yield dict(
            read=dict(
                source="fixture",
                requested_at=T,
                received_at=T,
                status="failed",
                detail="HTTP 429",
            )
        )

    monkeypatch.setitem(
        api.SOURCES,
        "fixture",
        dict(
            connected=True,
            fetch=fetch,
            datasets=["info"],
            defaults=["info"],
            options=[],
        ),
    )
    r = api.collect([{"source": "fixture"}], database=db)
    assert r["status"] == "partial" and len(select(db, "documents")["rows"]) == 1
    assert len(select(db, "acquisitions")["rows"]) == 2
    import io
    import sys

    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(json.dumps({"selections": {"d": {"dataset": "documents"}}})),
    )
    assert main(["query", "--database", str(db), "--spec", "-"]) == 0
    assert json.loads(capsys.readouterr().out)["results"]["d"]["rows"][0]["text"]
    missing = tmp_path / "absent.sqlite"
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"selections": {}}'))
    assert (
        main(["query", "--database", str(missing), "--spec", "-"]) == 2
        and not missing.exists()
    )
    assert api.sources()["sources"]["okx"]["connected"]


def test_collection_retains_progress_closes_source_and_rolls_back_bad_batch(
    tmp_path, monkeypatch
):
    db = tmp_path / "facts.sqlite"
    closed = []

    def fetch(request, *, timeout):
        try:
            yield batch(documents=[document()])
            yield batch(bars=[dict(bar(), instrument=None)])
        finally:
            closed.append(True)

    monkeypatch.setitem(
        api.SOURCES,
        "fixture",
        dict(
            connected=True,
            fetch=fetch,
            datasets=("info",),
            defaults=("info",),
            options=(),
        ),
    )
    with pytest.raises(sqlite3.IntegrityError):
        api.collect([{"source": "fixture"}], database=db)
    assert closed == [True] and len(select(db, "documents")["rows"]) == 1
    with store.connect(db) as con:
        assert con.execute("SELECT count(*) FROM source_read").fetchone()[0] == 1
        assert (
            con.execute("SELECT read_id FROM document_version").fetchone()[0]
            is not None
        )


def test_common_http_reports_actual_error_and_retry_hint():
    with http.open_client(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(429, headers={"Retry-After": "30"})
        )
    ) as client:
        with pytest.raises(http.FetchError) as failure:
            http.request(client, "GET", "https://example.test/feed")
    assert failure.value.read["http_status"] == 429
    assert failure.value.read["retry_after"] == "30"


def test_coarse_publication_alias_reuse_and_identity_relations(tmp_path, monkeypatch):
    db = tmp_path / "facts.sqlite"
    bounds = {
        "since": stamp(T.replace(hour=0)),
        "until": stamp(T.replace(hour=0) + timedelta(days=1)),
    }
    docs = [
        dict(
            document("甲公司"),
            published_at=None,
            metadata={"publication_bounds": bounds, "publication_precision": "day"},
        ),
        dict(document("linked full body"), source="page", url="https://example.test/1"),
    ]
    store.ingest(
        db,
        batch(
            docs,
            instruments=[
                dict(
                    instrument="A",
                    symbol="AAA",
                    name="甲公司",
                    aliases=[],
                    rules={"underlying": "native-asset"},
                )
            ],
        ),
    )
    original = api.information.literal_matcher
    calls = []

    def counted(refs):
        calls.append(1)
        return original(refs)

    monkeypatch.setattr(api.information, "literal_matcher", counted)
    r = api.query(
        {
            "day": {
                "dataset": "documents",
                "where": {"mentions": ["A"]},
                "time": {"axis": "published", **bounds},
            },
            "intraday": {
                "dataset": "documents",
                "where": {"mentions": ["A"]},
                "time": {
                    "axis": "published",
                    "since": T,
                    "until": T + timedelta(hours=1),
                },
            },
            "r": {"dataset": "references"},
            "identity": {"dataset": "instruments", "ids": ["A"]},
        },
        relations={
            "m": {"kind": "literal_mentions", "input": "day"},
            "same": {"kind": "same_url", "input": "day"},
            "u": {"kind": "underlying", "input": "r"},
        },
        database=db,
    )["results"]
    assert (
        len(calls) == 1
        and len(r["day"]["rows"]) == 1
        and len(r["intraday"]["unplaced"]) == 1
    )
    assert len(r["same"]["rows"]) == 1
    assert r["u"]["rows"][0]["underlying"] == "native-asset"
    assert r["identity"]["rows"][0]["id"] == "A"
    assert select(db, "instruments", where={"text": "甲公司"})["rows"][0]["id"] == "A"
    e = api.query(
        {
            "d": {"dataset": "documents", "where": {"mentions": ["A"]}},
            "p": {"dataset": "bars"},
        },
        calculations={
            "r": {
                "kind": "event_response",
                "events": "d",
                "prices": "p",
                "anchor": "published",
                "before": "5m",
                "after": "5m",
                "subjects": ["A"],
            }
        },
        database=db,
    )["results"]["r"]["rows"][0]
    assert e["gap"] == "unknown/imprecise anchor or conflicting event"


def test_endpoints_survive_missing_middle_without_certifying_path():
    from marketsift import comparison

    rows = []
    for i in (0, 2):
        rows.append(
            dict(
                bar("A"),
                basis="trade",
                id=i,
                start=stamp(T + timedelta(minutes=i * 5)),
                end=stamp(T + timedelta(minutes=(i + 1) * 5)),
                units={"price": "CNY", "amount": "CNY"},
            )
        )
    m = prices.measure(rows, window={"since": T, "until": T + timedelta(minutes=15)})
    assert m["window_return"] == pytest.approx(16.2 / 16 - 1)
    assert m["window_amount"] is None and m["close_drawdown"] is None
    rows[1]["units"] = {"price": "USDT", "amount": "USDT"}
    r = comparison.compare(
        rows,
        windows={"w": {"since": T, "until": T + timedelta(minutes=15)}},
        pool=["A"],
        as_of=T + timedelta(hours=1),
    )
    assert len(r["comparisons"]) == 2
    assert all(
        p["windows"]["w"]["members"]["A"]["observed"]["window_return"] is None
        for p in r["comparisons"]
    )


def test_cli_utf8_json_ignores_windows_legacy_pipe_encoding(tmp_path):
    import os
    import subprocess
    import sys

    db = tmp_path / "facts.sqlite"
    store.ingest(db, batch([document()]))
    spec = {"selections": {"d": {"dataset": "documents", "where": {"text": "新乳业"}}}}
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
        input=json.dumps(spec, ensure_ascii=False),
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=dict(os.environ, PYTHONIOENCODING="gbk"),
    )
    assert result.returncode == 0, result.stderr
    assert (
        json.loads(result.stdout)["results"]["d"]["rows"][0]["text"] == "新乳业的新信息"
    )
