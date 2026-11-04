"""One continuous SQLite fact store; research judgments belong to the agent."""

import json
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

from .time import stamp

DDL = """
CREATE TABLE IF NOT EXISTS instrument (
    instrument TEXT PRIMARY KEY, venue TEXT, symbol TEXT, kind TEXT,
    name TEXT, aliases TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS document (
    id INTEGER PRIMARY KEY, source TEXT NOT NULL, native_id TEXT,
    url TEXT, UNIQUE(source, native_id)
);
CREATE TABLE IF NOT EXISTS document_version (
    id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL REFERENCES document(id),
    observed_at TEXT NOT NULL, published_at TEXT, published_label TEXT,
    title TEXT, author TEXT, text TEXT NOT NULL, raw_text TEXT,
    attachments TEXT NOT NULL, links TEXT NOT NULL, metadata TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS document_time ON document_version(document_id, observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS document_publication ON document_version(published_at);
CREATE VIRTUAL TABLE IF NOT EXISTS document_text USING fts5(text, tokenize='trigram');
CREATE TABLE IF NOT EXISTS bar (
    id INTEGER PRIMARY KEY, source TEXT NOT NULL, instrument TEXT NOT NULL,
    interval TEXT NOT NULL, start TEXT NOT NULL, end TEXT NOT NULL, session TEXT,
    observed_at TEXT NOT NULL, requested_at TEXT, complete INTEGER,
    open REAL, high REAL, low REAL, close REAL, amount REAL, volume REAL,
    units TEXT NOT NULL, metadata TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS bar_time ON bar(instrument, interval, start, source, observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS bar_observation ON bar(observed_at);
CREATE TABLE IF NOT EXISTS source_read (
    id INTEGER PRIMARY KEY, source TEXT NOT NULL, requested_at TEXT NOT NULL,
    received_at TEXT NOT NULL, since TEXT, until TEXT, status TEXT NOT NULL,
    detail TEXT, instruments TEXT NOT NULL, rows INTEGER NOT NULL,
    first_time TEXT, last_time TEXT
);
CREATE INDEX IF NOT EXISTS read_source ON source_read(source, received_at DESC);
CREATE TABLE IF NOT EXISTS instrument_version (
    id INTEGER PRIMARY KEY, instrument TEXT NOT NULL REFERENCES instrument(instrument),
    source TEXT NOT NULL, observed_at TEXT NOT NULL, name TEXT, aliases TEXT NOT NULL,
    rules TEXT NOT NULL, metadata TEXT NOT NULL, read_id INTEGER REFERENCES source_read(id)
);
CREATE INDEX IF NOT EXISTS reference_time ON instrument_version(instrument,source,observed_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS series (
    id INTEGER PRIMARY KEY, series TEXT NOT NULL, source TEXT NOT NULL, native_id TEXT,
    instrument TEXT REFERENCES instrument(instrument), name TEXT, unit TEXT, frequency TEXT,
    measure TEXT, aggregation TEXT, metadata TEXT NOT NULL, observed_at TEXT NOT NULL,
    read_id INTEGER REFERENCES source_read(id)
);
CREATE INDEX IF NOT EXISTS series_time ON series(series,observed_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS series_value (
    id INTEGER PRIMARY KEY, series_id INTEGER NOT NULL REFERENCES series(id),
    native_id TEXT NOT NULL DEFAULT '', period_kind TEXT NOT NULL, period_start TEXT NOT NULL, period_end TEXT,
    value REAL, state TEXT NOT NULL, published_at TEXT, metadata TEXT NOT NULL,
    observed_at TEXT NOT NULL, read_id INTEGER REFERENCES source_read(id)
);
CREATE INDEX IF NOT EXISTS value_time ON series_value(series_id,period_kind,period_start,period_end,observed_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS trade (
    id INTEGER PRIMARY KEY, source TEXT NOT NULL, instrument TEXT NOT NULL REFERENCES instrument(instrument),
    native_id TEXT NOT NULL, ts TEXT NOT NULL, price REAL, size REAL, side TEXT,
    units TEXT NOT NULL, metadata TEXT NOT NULL, observed_at TEXT NOT NULL,
    read_id INTEGER REFERENCES source_read(id)
);
CREATE INDEX IF NOT EXISTS trade_time ON trade(source,instrument,native_id,observed_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS trade_event ON trade(instrument,ts);
"""

DOCUMENT_VALUES = (
    "published_at",
    "published_label",
    "title",
    "author",
    "text",
    "raw_text",
    "attachments",
    "links",
    "metadata",
)
BAR_VALUES = (
    "end",
    "session",
    "requested_at",
    "complete",
    "open",
    "high",
    "low",
    "close",
    "amount",
    "volume",
    "units",
    "metadata",
)


def packed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@contextmanager
def connect(path, *, write=False):
    path = Path(path).resolve()
    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
    elif not path.is_file():
        raise FileNotFoundError(path)
    con = (
        sqlite3.connect(path)
        if write
        else sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    )
    con.row_factory = sqlite3.Row
    try:
        with con:
            if write:
                con.execute("PRAGMA journal_mode=WAL")
                con.execute("PRAGMA foreign_keys=ON")
                con.executescript(DDL)
                rebuilding = con.execute("PRAGMA user_version").fetchone()[0] < 2
                if rebuilding:
                    # SQLite records deferred violations when a populated parent
                    # is dropped, even if an identical replacement is restored.
                    con.execute("PRAGMA foreign_keys=OFF")
                con.execute("BEGIN IMMEDIATE")
                if con.execute("PRAGMA user_version").fetchone()[0] < 1:
                    con.execute(
                        "ALTER TABLE bar ADD COLUMN basis TEXT NOT NULL DEFAULT 'trade'"
                    )
                    for table in ("bar", "document_version"):
                        con.execute(
                            f"ALTER TABLE {table} ADD COLUMN read_id INTEGER REFERENCES source_read(id)"
                        )
                    con.execute(
                        "ALTER TABLE source_read ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'"
                    )
                    con.execute(
                        "CREATE INDEX bar_basis_time ON bar(instrument,interval,basis,start,source,observed_at DESC)"
                    )
                    con.execute("PRAGMA user_version=1")
                if con.execute("PRAGMA user_version").fetchone()[0] < 2:
                    con.execute(
                        "CREATE TABLE document_next(id INTEGER PRIMARY KEY,source TEXT NOT NULL,native_id TEXT,url TEXT,UNIQUE(source,native_id))"
                    )
                    con.execute(
                        "INSERT INTO document_next SELECT id,source,native_id,url FROM document"
                    )
                    con.execute("DROP TABLE document")
                    con.execute("ALTER TABLE document_next RENAME TO document")
                    at = stamp()
                    for row in con.execute("SELECT * FROM instrument").fetchall():
                        con.execute(
                            "INSERT INTO instrument_version(instrument,source,observed_at,name,aliases,rules,metadata) VALUES(?,?,?,?,?,?,?)",
                            (
                                row["instrument"],
                                "legacy_reference",
                                at,
                                row["name"],
                                row["aliases"],
                                "{}",
                                packed(
                                    {
                                        "availability": "reference first availability unknown; baseline observed at migration"
                                    }
                                ),
                            ),
                        )
                    con.execute("ALTER TABLE instrument DROP COLUMN name")
                    con.execute("ALTER TABLE instrument DROP COLUMN aliases")
                    con.execute(
                        "INSERT OR IGNORE INTO instrument(instrument) SELECT DISTINCT instrument FROM bar"
                    )
                    con.execute(
                        "CREATE TABLE bar_next (id INTEGER PRIMARY KEY,source TEXT NOT NULL,instrument TEXT NOT NULL REFERENCES instrument(instrument),interval TEXT NOT NULL,start TEXT NOT NULL,end TEXT NOT NULL,session TEXT,observed_at TEXT NOT NULL,requested_at TEXT,complete INTEGER,open REAL,high REAL,low REAL,close REAL,amount REAL,volume REAL,units TEXT NOT NULL,metadata TEXT NOT NULL,basis TEXT NOT NULL DEFAULT 'trade',read_id INTEGER REFERENCES source_read(id))"
                    )
                    con.execute("INSERT INTO bar_next SELECT * FROM bar")
                    con.execute("DROP TABLE bar")
                    con.execute("ALTER TABLE bar_next RENAME TO bar")
                    con.execute(
                        "CREATE INDEX bar_time ON bar(instrument,interval,start,source,observed_at DESC,id DESC)"
                    )
                    con.execute("CREATE INDEX bar_observation ON bar(observed_at)")
                    con.execute(
                        "CREATE INDEX bar_basis_time ON bar(instrument,interval,basis,start,source,observed_at DESC)"
                    )
                    con.execute("PRAGMA user_version=2")
                    if con.execute("PRAGMA foreign_key_check").fetchone():
                        raise sqlite3.IntegrityError(
                            "schema migration broke a fact reference"
                        )
                    con.commit()
                    con.execute("PRAGMA foreign_keys=ON")
                    con.execute("BEGIN IMMEDIATE")
            else:
                con.execute("BEGIN")
            yield con
    finally:
        con.close()


def _append(con, table, key, observed, data, read_id):
    fields, values = tuple(data), tuple(data.values())
    if _unchanged(con, table, key, observed, fields, values):
        where = " AND ".join(f"{k} IS ?" for k in key)
        row = con.execute(
            f"SELECT id FROM {table} WHERE {where} AND observed_at<=? ORDER BY observed_at DESC,id DESC LIMIT 1",
            (*key.values(), observed),
        ).fetchone()
        return row[0], 0
    columns = [*key, *fields, "observed_at", "read_id"]
    row = con.execute(
        f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
        (*key.values(), *values, observed, read_id),
    )
    return row.lastrowid, 1


def put_instruments(con, instruments, read_id=None, source=None, observed_at=None):
    changed = 0
    for row in instruments:
        con.execute(
            "INSERT INTO instrument(instrument,venue,symbol,kind) VALUES(?,?,?,?) ON CONFLICT(instrument) DO UPDATE SET venue=coalesce(excluded.venue,instrument.venue),symbol=coalesce(excluded.symbol,instrument.symbol),kind=coalesce(excluded.kind,instrument.kind)",
            (row["instrument"], row.get("venue"), row.get("symbol"), row.get("kind")),
        )
        _, added = _append(
            con,
            "instrument_version",
            {
                "instrument": row["instrument"],
                "source": row.get("source", source or "unknown"),
            },
            stamp(row.get("observed_at", observed_at)),
            dict(
                name=row.get("name"),
                aliases=packed(row.get("aliases", [])),
                rules=packed(row.get("rules", {})),
                metadata=packed(row.get("metadata", {})),
            ),
            read_id,
        )
        changed += added
    return changed


def _unchanged(con, table, key, observed, fields, values):
    where = " AND ".join(f"{k} IS ?" for k in key)
    old = con.execute(
        f"SELECT observed_at,{','.join(fields)} FROM {table} WHERE {where} AND observed_at<=? ORDER BY observed_at DESC,id DESC",
        (*key.values(), observed),
    )
    latest, peers = None, []
    for row in old:
        if latest is not None and row["observed_at"] != latest:
            break
        latest = row["observed_at"]
        peers.append(tuple(row[k] for k in fields))
    # A later unambiguous observation also resolves a previously tied conflict.
    return values in peers and (observed == latest or len(peers) == 1)


def put_documents(con, rows, read_id=None):
    inserted = 0
    for row in rows:
        native = str(row["native_id"]) if row.get("native_id") is not None else None
        if native is None:
            doc = con.execute(
                "INSERT INTO document(source,native_id,url) VALUES(?,NULL,?)",
                (row["source"], row.get("url")),
            ).lastrowid
        else:
            con.execute(
                "INSERT INTO document(source,native_id,url) VALUES(?,?,?) ON CONFLICT(source,native_id) DO UPDATE SET url=excluded.url",
                (row["source"], native, row.get("url")),
            )
            doc = con.execute(
                "SELECT id FROM document WHERE source=? AND native_id=?",
                (row["source"], native),
            ).fetchone()[0]
        observed = stamp(row["observed_at"])
        data = dict(row)
        data["published_at"] = (
            stamp(row["published_at"]) if row.get("published_at") else None
        )
        data.setdefault("text", "")
        for key, default in (("attachments", []), ("links", []), ("metadata", {})):
            data[key] = packed(row.get(key, default))
        values = tuple(data.get(k) for k in DOCUMENT_VALUES)
        if _unchanged(
            con,
            "document_version",
            {"document_id": doc},
            observed,
            DOCUMENT_VALUES,
            values,
        ):
            continue
        cur = con.execute(
            f"INSERT INTO document_version(document_id,observed_at,{','.join(DOCUMENT_VALUES)},read_id) VALUES({','.join('?' for _ in range(3 + len(values)))})",
            (doc, observed, *values, read_id),
        )
        con.execute(
            "INSERT INTO document_text(rowid,text) VALUES(?,?)",
            (cur.lastrowid, (row.get("title") or "") + "\n" + data["text"]),
        )
        inserted += 1
    return inserted


def put_bars(con, rows, read_id=None):
    inserted = 0
    for row in rows:
        con.execute(
            "INSERT OR IGNORE INTO instrument(instrument) VALUES(?)",
            (row["instrument"],),
        )
        key = dict(
            source=row["source"],
            instrument=row["instrument"],
            interval=row["interval"],
            start=stamp(row["start"]),
            basis=row.get("basis", "trade"),
        )
        observed = stamp(row["observed_at"])
        data = dict(row, end=stamp(row["end"]))
        data["requested_at"] = (
            stamp(row["requested_at"]) if row.get("requested_at") else None
        )
        for name in ("units", "metadata"):
            data[name] = packed(row.get(name, {}))
        values = tuple(data.get(k) for k in BAR_VALUES)
        # A new acquisition bound alone is not a new price version.
        same_values = tuple(
            v for k, v in zip(BAR_VALUES, values) if k != "requested_at"
        )
        fields = tuple(k for k in BAR_VALUES if k != "requested_at")
        if _unchanged(con, "bar", key, observed, fields, same_values):
            continue
        con.execute(
            f"INSERT INTO bar({','.join(key)},observed_at,{','.join(BAR_VALUES)},read_id) VALUES({','.join('?' for _ in range(len(key) + len(values) + 2))})",
            (*key.values(), observed, *values, read_id),
        )
        inserted += 1
    return inserted


def ingest(path, batch):
    """One returned source batch is one transaction, independent of other sources."""
    with connect(path, write=True) as con:
        receipt = batch["read"]
        read = con.execute(
            "INSERT INTO source_read(source,requested_at,received_at,since,until,status,detail,instruments,rows,first_time,last_time,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                receipt["source"],
                stamp(receipt["requested_at"]),
                stamp(receipt["received_at"]),
                stamp(receipt["since"]) if receipt.get("since") else None,
                stamp(receipt["until"]) if receipt.get("until") else None,
                receipt["status"],
                receipt.get("detail"),
                packed(receipt.get("instruments", [])),
                sum(
                    len(batch.get(k, []))
                    for k in (
                        "documents",
                        "bars",
                        "instruments",
                        "series_values",
                        "trades",
                    )
                ),
                receipt.get("first_time"),
                receipt.get("last_time"),
                packed(
                    {
                        k: v
                        for k, v in receipt.items()
                        if k
                        not in {
                            "source",
                            "requested_at",
                            "received_at",
                            "since",
                            "until",
                            "status",
                            "detail",
                            "instruments",
                            "first_time",
                            "last_time",
                        }
                    }
                ),
            ),
        )
        instruments = put_instruments(
            con,
            batch.get("instruments", []),
            read.lastrowid,
            receipt["source"],
            receipt["received_at"],
        )
        documents = put_documents(con, batch.get("documents", []), read.lastrowid)
        bars = put_bars(con, batch.get("bars", []), read.lastrowid)
        definitions, values = put_series(
            con, batch, read.lastrowid, receipt["received_at"]
        )
        trades = put_trades(con, batch.get("trades", []), read.lastrowid)
    return {
        "documents_added": documents,
        "bar_versions_added": bars,
        "instruments_updated": instruments,
        "read_id": read.lastrowid,
        "series_versions_added": definitions,
        "series_values_added": values,
        "trade_versions_added": trades,
    }


def lookup(con, query="", instruments=(), *, as_of=None):
    identities = [
        dict(r) for r in con.execute("SELECT * FROM instrument ORDER BY instrument")
    ]
    versioned = con.execute(
        "SELECT 1 FROM sqlite_master WHERE name='instrument_version'"
    ).fetchone()
    references = defaultdict(list)
    if versioned:
        rows = con.execute(
            "SELECT * FROM (SELECT *,dense_rank() OVER(PARTITION BY instrument,source ORDER BY observed_at DESC) AS rank FROM instrument_version WHERE observed_at<=?) WHERE rank=1 ORDER BY observed_at,id",
            (stamp(as_of),),
        )
        for r in rows:
            r = dict(r)
            r.pop("rank")
            for k in ("aliases", "rules", "metadata"):
                r[k] = json.loads(r[k])
            references[r["instrument"]].append(r)
    result, wanted = [], set(instruments)
    for row in identities:
        if wanted and row["instrument"] not in wanted:
            continue
        if versioned:
            refs = references[row["instrument"]]
            preferred = sorted(
                refs,
                key=lambda r: (r["source"] != "legacy_reference", r["observed_at"]),
            )
            row["name"] = next(
                (r["name"] for r in reversed(preferred) if r["name"]), None
            )
            row["aliases"] = list(dict.fromkeys(a for r in refs for a in r["aliases"]))
            row["references"] = refs
        else:
            row["aliases"] = json.loads(row["aliases"])
        text = " ".join(
            str(v or "")
            for v in (row["instrument"], row["symbol"], row["name"], *row["aliases"])
        )
        if query.casefold() in text.casefold():
            result.append(row)
    return result


def _conflict_sql(table, key, fields):
    same = " AND ".join(f"p.{k} IS v.{k}" for k in (*key, "observed_at"))
    different = " OR ".join(f"p.{k} IS NOT v.{k}" for k in fields)
    return f"EXISTS(SELECT 1 FROM {table} p WHERE {same} AND ({different})) AS value_conflict"


def _observations(rows, key, *, all_versions):
    """Equal visible facts may carry different provenance, not competing values."""
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[k] for k in (*key, "observed_at"))].append(row)
    selected = set()
    for peers in groups.values():
        conflict = any(r["value_conflict"] for r in peers)
        for row in peers:
            row.pop("value_conflict")
            row["conflict"] = conflict
        if all_versions or conflict or len(peers) == 1:
            selected.update(r["id"] for r in peers)
        else:
            representative = max(peers, key=lambda r: r["id"])
            representative["equivalent_version_ids"] = [r["id"] for r in peers]
            selected.add(representative["id"])
    return [row for row in rows if row["id"] in selected]


def neighbors(con, seeds, urls, *, as_of=None):
    """Select native identities/links in SQLite before reading neighbor bodies."""
    addresses = set(urls) | {r["url"] for r in seeds}
    targets = {u for r in seeds for u in r["links"]}
    native = [
        r["source"] + "\n" + r["native_id"] for r in seeds if r["native_id"] is not None
    ]
    parents = [
        r["source"] + "\n" + str(r["metadata"]["parent_native_id"])
        for r in seeds
        if r["metadata"].get("parent_native_id")
    ]
    topics = [
        r["source"] + "\n" + str(r["metadata"]["topic_id"])
        for r in seeds
        if r["metadata"].get("topic_id") is not None
    ]
    rows = con.execute(
        """SELECT DISTINCT v.document_id FROM document_version v JOIN document d ON d.id=v.document_id
        WHERE v.observed_at<=? AND (
        d.url IN (SELECT value FROM json_each(?)) OR
        EXISTS(SELECT 1 FROM json_each(v.links) link WHERE link.value IN (SELECT value FROM json_each(?))) OR
        d.source||char(10)||d.native_id IN (SELECT value FROM json_each(?)) OR
        d.source||char(10)||json_extract(v.metadata,'$.parent_native_id') IN (SELECT value FROM json_each(?)) OR
        d.source||char(10)||json_extract(v.metadata,'$.topic_id') IN (SELECT value FROM json_each(?)))""",
        (
            stamp(as_of),
            packed(list(addresses | targets)),
            packed(list(addresses)),
            packed(parents),
            packed(native),
            packed(topics),
        ),
    )
    return [r[0] for r in rows]


def put_series(con, batch, read_id, observed_at):
    definitions, values, identifiers = 0, 0, {}
    for row in batch.get("series", ()):
        instrument = row.get("instrument")
        if instrument:
            con.execute(
                "INSERT OR IGNORE INTO instrument(instrument) VALUES(?)", (instrument,)
            )
        data = {
            k: row.get(k)
            for k in (
                "source",
                "native_id",
                "instrument",
                "name",
                "unit",
                "frequency",
                "measure",
                "aggregation",
            )
        }
        data["metadata"] = packed(row.get("metadata", {}))
        identifier, added = _append(
            con,
            "series",
            {"series": row["series"]},
            stamp(row.get("observed_at", observed_at)),
            data,
            read_id,
        )
        identifiers[row["series"]] = identifier
        definitions += added
    for row in batch.get("series_values", ()):
        observed = stamp(row.get("observed_at", observed_at))
        identifier = identifiers.get(row["series"])
        if identifier is None:
            definition = con.execute(
                "SELECT id FROM series WHERE series=? AND observed_at<=? ORDER BY observed_at DESC,id DESC LIMIT 1",
                (row["series"], observed),
            ).fetchone()
            if definition is None:
                raise ValueError(f"series definition missing: {row['series']}")
            identifier = definition[0]
        kind = row["period_kind"]
        begin, end = row["period_start"], row.get("period_end")
        if kind == "instant":
            begin, end = stamp(begin), stamp(end) if end else None
        key = dict(
            series_id=identifier,
            native_id=row.get("native_id", ""),
            period_kind=kind,
            period_start=begin,
            period_end=end,
        )
        data = dict(
            value=row.get("value"),
            state=row.get(
                "state", "known" if row.get("value") is not None else "missing"
            ),
            published_at=stamp(row["published_at"])
            if row.get("published_at")
            else None,
            metadata=packed(row.get("metadata", {})),
        )
        _, added = _append(con, "series_value", key, observed, data, read_id)
        values += added
    return definitions, values


def put_trades(con, rows, read_id):
    changed = 0
    for row in rows:
        con.execute(
            "INSERT OR IGNORE INTO instrument(instrument) VALUES(?)",
            (row["instrument"],),
        )
        key = {k: row[k] for k in ("source", "instrument", "native_id")}
        data = {k: row.get(k) for k in ("price", "size", "side")}
        data.update(
            ts=stamp(row["ts"]),
            units=packed(row.get("units", {})),
            metadata=packed(row.get("metadata", {})),
        )
        _, added = _append(con, "trade", key, stamp(row["observed_at"]), data, read_id)
        changed += added
    return changed


# Every public read uses this selection owner. Table/column names below are
# internal constants; caller values are always SQL parameters.
DATASETS = {
    "documents": (
        "document_version",
        ("document_id",),
        ("text", "title", "published_at", "author", "attachments", "links"),
    ),
    "bars": (
        "bar",
        ("source", "instrument", "interval", "basis", "start"),
        tuple(k for k in BAR_VALUES if k not in {"metadata", "requested_at"}),
    ),
    "series": (
        "series_value",
        ("series_id", "native_id", "period_kind", "period_start", "period_end"),
        ("value", "state", "published_at"),
    ),
    "trades": (
        "trade",
        ("source", "instrument", "native_id"),
        ("ts", "price", "size", "side", "units"),
    ),
    "references": (
        "instrument_version",
        ("instrument", "source"),
        ("name", "aliases", "rules"),
    ),
    "series_definitions": (
        "series",
        ("series",),
        ("unit", "frequency", "measure", "aggregation", "instrument", "name"),
    ),
    "instruments": ("instrument", (), ()),
    "acquisitions": ("source_read", (), ()),
}


def select(con, spec, *, as_of):
    """Exact versions or latest-as-known facts, followed by condition/time filters."""
    from .selection import place

    dataset = spec["dataset"]
    if dataset not in DATASETS:
        raise ValueError(f"unknown dataset {dataset!r}")
    exact = "ids" in spec
    if exact and set(spec) - {"dataset", "ids"}:
        raise ValueError("exact ids cannot be combined with where/time/versions")
    if set(spec) - {"dataset", "ids", "where", "time", "versions"}:
        raise ValueError("selection supports dataset, ids or where/time/versions")
    versions = spec.get("versions", "latest")
    if versions not in {"latest", "all"}:
        raise ValueError("versions must be latest or all")
    table, key, fields = DATASETS[dataset]
    if not key and "versions" in spec:
        raise ValueError(f"{dataset} is not versioned")
    where = dict(spec.get("where", {}))
    mentions = where.pop("mentions", None) if dataset == "documents" else None
    columns = {
        "sources": "source",
        "instruments": "instrument",
        "series_ids": "series",
        "urls": "url",
        "document_ids": "document_id",
        "interval": "interval",
        "basis": "basis",
        "kinds": "kind",
        "venues": "venue",
        "status": "status",
        "native_ids": "native_id",
        "definition_ids": "series_id",
        "read_ids": "read_id",
        "sessions": "session",
        "session_roles": "json_extract(metadata,'$.session_role')",
    }
    supported = {
        "documents": {
            "sources",
            "urls",
            "document_ids",
            "native_ids",
            "text",
            "read_ids",
        },
        "bars": {
            "sources",
            "instruments",
            "interval",
            "basis",
            "read_ids",
            "sessions",
            "session_roles",
        },
        "series": {
            "sources",
            "instruments",
            "series_ids",
            "native_ids",
            "definition_ids",
            "read_ids",
        },
        "trades": {"sources", "instruments", "native_ids", "read_ids"},
        "references": {"sources", "instruments", "read_ids", "text"},
        "series_definitions": {"sources", "instruments", "series_ids", "read_ids"},
        "instruments": {"instruments", "kinds", "venues", "text"},
        "acquisitions": {"sources", "status"},
    }[dataset]
    if set(where) - supported:
        raise ValueError(
            f"unsupported {dataset} predicates: {sorted(set(where) - supported)}"
        )
    args = []
    if key:
        conflict = _conflict_sql(table, key, fields)
        ranking = (
            ""
            if exact
            else f",dense_rank() OVER(PARTITION BY {','.join('v.' + k for k in key)} ORDER BY v.observed_at DESC) AS version_rank"
        )
        base = f"SELECT v.*,{conflict}{ranking} FROM {table} v WHERE observed_at<=?"
        args.append(as_of)
        if exact:
            base += " AND v.id IN (SELECT value FROM json_each(?))"
            args.append(packed(spec["ids"]))
    else:
        base = f"SELECT * FROM {table}"
        if dataset == "acquisitions":
            base += " WHERE received_at<=?"
            args.append(as_of)
    # Predicates on natural identity commute with version choice. Push them
    # inside the window scan; text, corrected times and observed filters do not.
    if key:
        for name, choices in where.items():
            column = columns.get(name)
            if column in key:
                base += f" AND v.{column} IN (SELECT value FROM json_each(?))"
                args.append(packed([choices] if isinstance(choices, str) else choices))
    if dataset == "documents":
        base = f"SELECT v.*,d.source,d.native_id,d.url FROM ({base}) v JOIN document d ON d.id=v.document_id"
    elif dataset == "series":
        base = f"SELECT v.*,s.series,s.source,s.instrument,s.unit,s.name,s.frequency,s.measure,s.aggregation FROM ({base}) v JOIN series s ON s.id=v.series_id WHERE s.observed_at<=?"
        args.append(as_of)
    conditions = []
    if exact:
        conditions.append(
            ("instrument" if dataset == "instruments" else "id")
            + " IN (SELECT value FROM json_each(?))"
        )
        args.append(packed(spec["ids"]))
    elif key and versions == "latest":
        conditions.append("version_rank=1")
    for name, values in where.items():
        if name == "text":
            if dataset == "instruments":
                conditions.append(
                    "(instr(lower(instrument||coalesce(symbol,'')||coalesce(venue,'')||coalesce(kind,'')),lower(?))>0 OR instrument IN (SELECT instrument FROM (SELECT *,dense_rank() OVER(PARTITION BY instrument,source ORDER BY observed_at DESC) AS rank FROM instrument_version WHERE observed_at<=?) WHERE rank=1 AND instr(lower(coalesce(name,'')||aliases),lower(?))>0))"
                )
                args.extend([values, as_of, values])
            elif dataset == "documents" and len(values) >= 3:
                conditions.append(
                    "id IN (SELECT rowid FROM document_text WHERE document_text MATCH ?)"
                )
                args.append('"' + values.replace('"', '""') + '"')
            else:
                expression = (
                    "coalesce(title,'')||text"
                    if dataset == "documents"
                    else "coalesce(name,'')||coalesce(aliases,'')||instrument"
                )
                conditions.append(f"instr(lower({expression}),lower(?))>0")
                args.append(values)
        else:
            conditions.append(f"{columns[name]} IN (SELECT value FROM json_each(?))")
            args.append(packed([values] if isinstance(values, str) else values))
    sql = f"SELECT * FROM ({base})"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    rows = []
    for raw in con.execute(sql, args):
        row = dict(raw)
        row.pop("version_rank", None)
        for name in (
            "metadata",
            "links",
            "attachments",
            "aliases",
            "rules",
            "units",
            "instruments",
        ):
            if name in row:
                row[name] = json.loads(row[name])
        if dataset == "instruments":
            row["id"] = row["instrument"]
        if "complete" in row and row["complete"] is not None:
            row["complete"] = bool(row["complete"])
        rows.append(row)
    if key:
        rows = _observations(rows, key, all_versions=exact or versions == "all")
    rows, unplaced = place(rows, spec.get("time"), dataset)
    axis = spec.get("time", {}).get("axis")
    order = {
        "published": "published_at",
        "observed": "observed_at",
        "event": "start" if dataset == "bars" else "ts",
        "period": "period_start",
        "received": "received_at",
    }.get(axis)
    order = order or {
        "documents": "published_at",
        "bars": "start",
        "series": "period_start",
        "trades": "ts",
        "acquisitions": "received_at",
    }.get(dataset, "observed_at")
    rows.sort(key=lambda r: (r.get(order) is None, r.get(order) or "", r["id"]))
    return dict(
        dataset=dataset,
        rows=rows,
        unplaced=unplaced,
        missing_ids=[
            i for i in spec.get("ids", []) if i not in {r["id"] for r in rows}
        ],
        **({"mentions": mentions} if mentions is not None else {}),
    )
