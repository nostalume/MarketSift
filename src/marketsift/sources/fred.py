"""FRED CSV into continuous source-revision values, not task snapshots."""

import csv
import io

from ..time import stamp
from . import http
from .common import number, period, source_read

SERIES = {
    "WALCL": (
        "Federal Reserve assets",
        "USD millions, NSA",
        "weekly Wednesday",
        "level",
    ),
    "WRBWFRBL": ("Reserve balances", "USD millions, NSA", "weekly Wednesday", "level"),
    "WDTGAL": (
        "Treasury General Account",
        "USD millions, NSA",
        "weekly Wednesday",
        "level",
    ),
    "RRPONTSYD": ("Overnight reverse repo", "USD billions, NSA", "daily", "level"),
    "DFF": ("Effective federal funds rate", "percent, NSA", "daily 7-day", "rate"),
    "DFII10": ("10-year real Treasury yield", "percent, NSA", "daily", "rate"),
    "BAMLH0A0HYM2": ("US high-yield OAS", "percent, NSA", "daily close", "rate"),
    "CPIAUCSL": ("Headline CPI", "index 1982-84=100, SA", "monthly", "level"),
    "CPILFESL": ("Core CPI", "index 1982-84=100, SA", "monthly", "level"),
    "UNRATE": ("Unemployment rate", "percent, SA", "monthly", "rate"),
    "INDPRO": ("Industrial production", "index 2017=100, SA", "monthly", "level"),
    "GDPC1": (
        "Real GDP",
        "USD billions chained 2017, SA annual rate",
        "quarterly",
        "level",
    ),
}
URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def fetch(request, *, timeout=15, transport=None):
    identifiers = request.get("series_ids", ())
    if not identifiers:
        raise ValueError(
            "FRED needs explicit series_ids; see sources for known metadata"
        )
    if not request.get("since") or not request.get("until"):
        raise ValueError("FRED needs date since/until")
    with http.open_client(timeout=timeout, transport=transport) as client:
        for identifier in identifiers:
            native = identifier.removeprefix("fred:")
            try:
                response = http.request(
                    client,
                    "GET",
                    URL,
                    params={
                        "id": native,
                        "cosd": request["since"],
                        "coed": request["until"],
                    },
                )
                reader = csv.reader(io.StringIO(response.body.decode("utf-8-sig")))
                header = next(reader, [])
                if (
                    len(header) != 2
                    or header[1] != native
                    or header[0] not in {"DATE", "observation_date"}
                ):
                    raise ValueError("FRED did not return requested CSV")
                name, unit, frequency, measure = SERIES.get(
                    native, (native, None, None, None)
                )
                key = "fred:" + native
                definition = dict(
                    series=key,
                    source="fred",
                    native_id=native,
                    name=name,
                    unit=unit,
                    frequency=frequency,
                    measure=measure,
                    aggregation="native; not summed",
                    metadata={
                        "url": "https://fred.stlouisfed.org/series/" + native,
                        "revision": "latest at retrieval, not historical vintage",
                    },
                )
                values, issues = [], []
                for row in reader:
                    if not row:
                        continue
                    try:
                        day, value = row
                        if not request["since"] <= day < request["until"]:
                            continue
                        kind = (
                            "month"
                            if frequency == "monthly"
                            else "quarter"
                            if frequency == "quarterly"
                            else None
                        )
                        dates = (
                            period(day, kind)
                            if kind
                            else dict(
                                period_kind="date", period_start=day, period_end=None
                            )
                        )
                        values.append(
                            dict(
                                series=key,
                                **dates,
                                value=number(value),
                                observed_at=response.received_at,
                                metadata={
                                    "native_value": value,
                                    "publication_time": "unknown",
                                },
                            )
                        )
                    except (ValueError, TypeError) as exc:
                        issues.append(str(exc))
                yield dict(
                    series=[definition],
                    series_values=values,
                    read=source_read(
                        "fred",
                        response,
                        detail="retrieval-time revision; publication timestamps unavailable",
                        status="partial" if issues else "ok",
                        period_since=request["since"],
                        period_until=request["until"],
                        native_series=native,
                        issues=issues,
                    ),
                )
            except http.FetchError as exc:
                yield {"read": dict(source="fred", **exc.read)}
            except (ValueError, UnicodeError, csv.Error) as exc:
                yield {
                    "read": dict(
                        source="fred",
                        requested_at=stamp(),
                        received_at=stamp(),
                        status="failed",
                        detail=str(exc),
                    )
                }
