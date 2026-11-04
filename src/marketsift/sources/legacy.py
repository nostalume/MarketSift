"""Read selected existing facts without running their former admission protocols."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..time import instant, stamp
from .tdx import UNITS, instrument, session_role

CHINA = timezone(timedelta(hours=8))


def fetch(request, *, timeout=15):
    loader = sqlite_history if request["source"] == "sqlite" else archive
    for path in request.get("options", {})["input"]:
        for batch in loader(
            path,
            request.get("since"),
            request.get("until"),
            instruments=request.get("instruments", ()),
        ):
            selected = request.get("datasets", ("info", "prices", "instruments"))
            if "info" not in selected:
                batch.pop("documents", None)
            if "prices" not in selected:
                batch.pop("bars", None)
            if "instruments" not in selected and not batch.get("bars"):
                batch.pop("instruments", None)
            yield batch


def _read(source, path, since, until, requested, **values):
    return {
        "source": source,
        "requested_at": requested,
        "received_at": stamp(),
        "since": stamp(since) if since else None,
        "until": stamp(until) if until else None,
        "status": "ok",
        "detail": f"local read: {path}; no original files changed",
        **values,
    }


def _within(start, since, until):
    return (since is None or start >= instant(since)) and (
        until is None or start < instant(until)
    )


def _bar(row, observed, requested=None, availability="historical availability unknown"):
    end = (
        instant(row["end"])
        if row.get("end")
        else datetime.strptime(str(row["trade_date"]), "%Y%m%d").replace(tzinfo=CHINA)
        + timedelta(minutes=row["bar_time"])
    )
    return {
        "source": "legacy_tdx",
        "instrument": (row.get("instrument") or row["code"]).upper(),
        "interval": "5m",
        "start": stamp(end - timedelta(minutes=5)),
        "end": stamp(end),
        "session": end.astimezone(CHINA).date().isoformat(),
        "observed_at": observed,
        "requested_at": requested,
        "complete": end <= instant(requested or observed),
        **{k: row.get(k) for k in ("open", "high", "low", "close", "amount", "volume")},
        "units": UNITS,
        "metadata": {"availability": availability, "session_role": session_role(end)},
    }


def sqlite_history(path, since, until, *, instruments=()):
    requested = stamp()
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as con:
        con.row_factory = sqlite3.Row
        con.execute("BEGIN")
        lower = int(instant(since).astimezone(CHINA).strftime("%Y%m%d"))
        upper = int(instant(until).astimezone(CHINA).strftime("%Y%m%d"))
        sql = "SELECT * FROM minute5 WHERE trade_date BETWEEN ? AND ?"
        args = [lower, upper]
        if instruments:
            sql += " AND code IN (SELECT value FROM json_each(?))"
            args.append(json.dumps([c.lower() for c in instruments]))
        cursor = con.execute(sql + " ORDER BY code,trade_date,bar_time", args)
        observed = stamp()
        found = False
        while True:
            chunk = cursor.fetchmany(
                1000
            )  # I/O batch size; the cursor continues to exhaustion.
            if not chunk:
                break
            found = True
            rows = [_bar(dict(r), observed) for r in chunk]
            rows = [r for r in rows if _within(instant(r["start"]), since, until)]
            refs = [instrument(c) for c in dict.fromkeys(r["instrument"] for r in rows)]
            yield {
                "instruments": refs,
                "bars": rows,
                "read": _read(
                    "legacy_tdx",
                    path,
                    since,
                    until,
                    requested,
                    instruments=[r["instrument"] for r in refs],
                    first_time=min((r["start"] for r in rows), default=None),
                    last_time=max((r["end"] for r in rows), default=None),
                ),
            }
        if not found:
            yield {
                "read": _read(
                    "legacy_tdx",
                    path,
                    since,
                    until,
                    requested,
                    instruments=list(instruments),
                    first_time=None,
                    last_time=None,
                )
            }


def archive(path, since=None, until=None, *, instruments=()):
    path = Path(path)
    requested = stamp()
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if "aliases" in data:
        refs = []
        for code, aliases in data["aliases"].items():
            if instruments and code.upper() not in instruments:
                continue
            ref = instrument(code)
            ref.update(
                name=next((n for n in aliases if not n.isascii()), None),
                aliases=aliases,
            )
            refs.append(ref)
        yield {
            "instruments": refs,
            "read": _read("legacy_reference", path, since, until, requested),
        }
        return
    if "bars" in data:
        observed = data.get("observed_at") or stamp()
        rows = []
        for row in data["bars"]:
            code = (row.get("instrument") or row["code"]).upper()
            if instruments and code not in instruments:
                continue
            check = data.get("checks", {}).get(code, {})
            bar = _bar(
                row,
                check.get("received_at") or observed,
                check.get("requested_at"),
                "archived observation"
                if data.get("observed_at")
                else "historical availability unknown",
            )
            if _within(instant(bar["start"]), since, until):
                rows.append(bar)
        yield {
            "instruments": [
                instrument(c) for c in dict.fromkeys(r["instrument"] for r in rows)
            ],
            "bars": rows,
            "read": _read("legacy_tdx", path, since, until, requested),
        }
        return
    records = data.get("records")
    if records is None:
        raise ValueError(
            "local input must be an existing material, capture, securities reference or minute snapshot"
        )
    rows = records.values() if isinstance(records, dict) else records
    documents = []
    for row in rows:
        published = row.get("published", {})
        when = row.get("published_at") or published.get("lower")
        if when and not _within(instant(when), since, until):
            continue
        source = {"sina_finance": "sina"}.get(
            row.get("source"), row.get("source", "legacy")
        )
        documents.append(
            {
                "source": source,
                "native_id": row.get("native_id") or row.get("id"),
                "url": row.get("url"),
                "author": row.get("author"),
                "title": row.get("title"),
                "text": row.get("text") or "",
                "observed_at": row.get("observed_at") or requested,
                "published_at": when,
                "published_label": published.get("label"),
                "attachments": row.get("attachments", []),
                "links": row.get("links", []),
                "metadata": {
                    "kind": row.get("kind"),
                    "parent_id": row.get("parent_id"),
                    "origin": row.get("origin"),
                },
            }
        )
    yield {
        "documents": documents,
        "read": _read("legacy", path, since, until, requested),
    }
