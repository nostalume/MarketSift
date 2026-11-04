"""Explicit collection and local queries. No research state or hidden network reads."""

from contextlib import closing
from datetime import date
from pathlib import Path

from . import comparison, events, information, store
from . import prices as price_data
from . import series as scalar_data
from .sources import SOURCES
from .time import instant, stamp
from .types import CollectRequest

DEFAULT_DB = Path("data/marketsift.sqlite")


def collect(requests: list[CollectRequest], *, database=DEFAULT_DB, timeout=15):
    if not requests:
        raise ValueError("select source requests; see marketsift sources")
    prepared = []
    for original in requests:
        request = dict(original)
        name = request.get("source")
        spec = SOURCES.get(name, {})
        if not spec.get("connected"):
            raise ValueError(
                f"source {name!r} is not connected; see marketsift sources"
            )
        datasets = request.get("datasets") or spec["defaults"]
        if set(datasets) - set(spec["datasets"]):
            raise ValueError(f"{name} supports datasets {spec['datasets']}")
        options = request.get("options", {})
        if set(options) - set(spec["options"]):
            raise ValueError(f"{name} supports options {spec['options']}")
        if spec.get("requires_range") and not all(
            request.get(k) for k in ("since", "until")
        ):
            raise ValueError(f"{name} needs --since and --until with UTC offsets")
        if (
            request.get("since")
            and request.get("until")
            and (
                date.fromisoformat(request["since"])
                if spec.get("range_clock") == "date"
                else instant(request["since"])
            )
            >= (
                date.fromisoformat(request["until"])
                if spec.get("range_clock") == "date"
                else instant(request["until"])
            )
        ):
            raise ValueError("--since must precede --until")
        for field, capability in (("interval", "intervals"), ("basis", "bases")):
            if request.get(field) and request[field] not in spec.get(capability, ()):
                raise ValueError(f"{name} does not support {field}={request[field]!r}")
        if "input" in spec["options"] and not options.get("input"):
            raise ValueError(
                f"{name} needs options.input pointing to existing local material"
            )
        if (
            not request.get("instruments")
            and spec.get("default_instruments") == "stored"
        ):
            prefixes = tuple(spec["instrument_prefixes"])
            request["instruments"] = [
                r["instrument"]
                for r in _stored_references(database)
                if r["instrument"].startswith(prefixes)
            ]
        for field in ("instruments", "series_ids", "urls", "since", "until"):
            if request.get(field) and field not in spec.get("filters", ()):
                raise ValueError(f"{name} does not support filtering by {field}")
        request["datasets"] = list(datasets)
        prepared.append((request, spec["fetch"]))
    added = dict(
        documents_added=0,
        bar_versions_added=0,
        instruments_updated=0,
        series_versions_added=0,
        series_values_added=0,
        trade_versions_added=0,
    )
    results = []
    for request, fetch in prepared:
        result = dict(source=request["source"], batches=0, read_ids=[], failures=[])
        good = 0
        with closing(fetch(request, timeout=timeout)) as batches:
            for batch in batches:
                batch["read"].setdefault("datasets", request["datasets"])
                changed = store.ingest(database, batch)
                for key in added:
                    added[key] += changed[key]
                result["read_ids"].append(changed["read_id"])
                result["batches"] += 1
                read = batch["read"]
                if read["status"] != "ok":
                    result["failures"].append(read)
                if read["status"] == "ok" or any(
                    batch.get(k)
                    for k in (
                        "documents",
                        "bars",
                        "instruments",
                        "series",
                        "series_values",
                        "trades",
                    )
                ):
                    good += 1
        result["status"] = (
            "partial" if result["failures"] and good else "failed" if not good else "ok"
        )
        results.append(result)
    status = (
        "ok"
        if all(r["status"] == "ok" for r in results)
        else "failed"
        if all(r["status"] == "failed" for r in results)
        else "partial"
    )
    return dict(
        database=str(Path(database).resolve()), status=status, **added, sources=results
    )


def _stored_references(database):
    with store.connect(database) as con:
        return store.lookup(con)


def sources():
    return dict(
        sources={
            name: {k: v for k, v in spec.items() if k != "fetch"}
            for name, spec in SOURCES.items()
        }
    )


def _project(value, columns):
    # Projection is last: it cannot change a relation or calculation's inputs.
    result = dict(value)

    def project(row):
        if columns is not None:
            return {k: row[k] for k in columns if k in row}
        row = {k: v for k, v in row.items() if k != "raw_text"}
        if "metadata" in row:
            row["metadata"] = {
                k: v for k, v in row["metadata"].items() if k != "native_tables"
            }
        return row

    if "rows" in result:
        result["rows"] = [project(r) for r in result["rows"]]
    if "unplaced" in result:
        result["unplaced"] = [
            dict(r, row=project(r["row"])) for r in result["unplaced"]
        ]
    return result


def query(
    selections=None,
    *,
    relations=None,
    calculations=None,
    emit=None,
    database=DEFAULT_DB,
    as_of=None,
):
    cutoff = stamp(as_of)
    selections, relations, calculations = (
        selections or {},
        relations or {},
        calculations or {},
    )
    names = [*selections, *relations, *calculations]
    if len(set(names)) != len(names):
        raise ValueError(
            "result names must be unique across selections/relations/calculations"
        )
    with store.connect(database) as con:
        # Establish the SQLite snapshot before any relation or concurrent writer.
        con.execute("SELECT count(*) FROM sqlite_master").fetchone()
        find = None

        def associate(rows):
            nonlocal find
            if find is None:
                find = information.literal_matcher(store.lookup(con, as_of=cutoff))
            return information.associate(rows, find=find)

        def select(spec):
            value = store.select(con, spec, as_of=cutoff)
            wanted = value.pop("mentions", None)
            if wanted is not None:
                wanted = set(wanted)
                associated = associate(
                    value["rows"] + [r["row"] for r in value["unplaced"]]
                )
                matched = {
                    r["id"]: r
                    for r in associated
                    if any(wanted.intersection(c) for c in r["mentions"].values())
                }
                value["rows"] = [
                    matched[r["id"]] for r in value["rows"] if r["id"] in matched
                ]
                value["unplaced"] = [
                    dict(r, row=matched[r["row"]["id"]])
                    for r in value["unplaced"]
                    if r["row"]["id"] in matched
                ]
            return value

        values = {name: select(spec) for name, spec in selections.items()}
        associated = {}

        def mentions(name):
            if values[name].get("dataset") != "documents":
                raise ValueError("literal mentions require documents")
            if name not in associated:
                rows = values[name]["rows"]
                associated[name] = (
                    rows if all("mentions" in r for r in rows) else associate(rows)
                )
            return associated[name]

        relation_results = {}
        for name, original in relations.items():
            spec = dict(original)
            kind, input_name = spec.pop("kind"), spec.pop("input")
            if input_name not in selections:
                raise ValueError(
                    "relations must reference a selection, not another relation"
                )
            source = values[input_name]
            if kind == "literal_mentions":
                if spec:
                    raise ValueError(
                        f"unsupported literal_mentions parameters: {list(spec)}"
                    )
                relation_results[name] = dict(
                    dataset="documents",
                    rows=mentions(input_name),
                    edges=[
                        e
                        for e in information.connections(mentions(input_name))
                        if e["kind"] == "literal_mention"
                    ],
                )
            elif kind in {"replies", "thread", "outbound_link", "same_url"}:
                direction = spec.pop("direction", "out")
                scope = spec.pop("scope", None)
                if spec or direction not in {"out", "in", "both"}:
                    raise ValueError(
                        "relation needs direction=out/in/both and optional scope selection"
                    )
                seeds = source["rows"]
                if scope:
                    if isinstance(scope, str):
                        if scope not in selections:
                            raise ValueError(
                                "relation scope must reference a selection"
                            )
                        candidates = values[scope]["rows"]
                    else:
                        candidates = select(dict(dataset="documents", **scope))["rows"]
                else:
                    doc_ids = store.neighbors(con, seeds, (), as_of=cutoff)
                    candidates = (
                        select(
                            dict(dataset="documents", where={"document_ids": doc_ids})
                        )["rows"]
                        if doc_ids
                        else []
                    )
                relation_results[name] = information.expand(
                    seeds, candidates, kind=kind, direction=direction
                )
            elif kind == "documents_for":
                scope = spec.pop("scope")
                if spec or scope not in selections:
                    raise ValueError(
                        "documents_for needs scope naming document selection"
                    )
                objects = {
                    r["instrument"] for r in source["rows"] if r.get("instrument")
                }
                rows = [
                    r
                    for r in mentions(scope)
                    if any(objects.intersection(c) for c in r["mentions"].values())
                ]
                relation_results[name] = dict(
                    dataset="documents", rows=rows, edges=information.connections(rows)
                )
            elif kind in {"underlying", "instrument_series"}:
                if spec:
                    raise ValueError(f"unsupported {kind} parameters")
                rows = source["rows"]
                edges = information.connections(
                    [],
                    [dict(instrument=r["instrument"], references=[r]) for r in rows]
                    if kind == "underlying"
                    else [],
                    rows if kind == "instrument_series" else [],
                )
                relation_results[name] = dict(rows=edges)
            else:
                raise ValueError(f"unknown relation {kind!r}")
        values.update(relation_results)
        calculation_results = {}
        for name, original in calculations.items():
            spec = dict(original)
            kind = spec.pop("kind")
            if kind == "event_response":
                event_name, price_name = spec.pop("events"), spec.pop("prices")
                context_name = spec.pop("context", None)
                result = events.response(
                    values[event_name]["rows"],
                    values[price_name]["rows"],
                    context=values[context_name]["rows"] if context_name else (),
                    event_dataset=values[event_name].get("dataset", "documents"),
                    as_of=cutoff,
                    **spec,
                )
            else:
                input_name = spec.pop("input")
                data = values[input_name]["rows"]
                if kind == "summarize":
                    result = price_data.summarize(data, as_of=cutoff, **spec)
                elif kind == "compare":
                    result = comparison.compare(data, as_of=cutoff, **spec)
                elif kind == "cooccurrence":
                    if spec:
                        raise ValueError("cooccurrence only takes input")
                    result = information.cooccurrences(
                        data
                        if all("mentions" in r for r in data)
                        else mentions(input_name)
                    )
                elif kind == "change":
                    result = scalar_data.changes(data, **spec)
                elif kind == "available_asof":
                    result = scalar_data.available(data, **spec)
                else:
                    raise ValueError(f"unknown calculation {kind!r}")
            calculation_results[name] = result
        values.update(calculation_results)
        selected = (
            {n: None for n in values}
            if emit is None
            else {n: None for n in emit}
            if isinstance(emit, list)
            else emit
        )
        if set(selected) - set(values):
            raise ValueError("emit references unknown result")
        return dict(
            as_of=cutoff,
            results={
                n: _project(values[n], columns) for n, columns in selected.items()
            },
        )
