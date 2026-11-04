"""Unregistered BLS public series, paged by the provider's year window."""

import json
from datetime import date

from ..time import stamp
from . import http
from .common import number, period, source_read


def fetch(request, *, timeout=15, transport=None):
    begin, end = (
        date.fromisoformat(request["since"]),
        date.fromisoformat(request["until"]),
    )
    identifiers = request.get("series_ids") or ["LNS14000000"]
    with http.open_client(timeout=timeout, transport=transport) as client:
        for identifier in identifiers:
            native = identifier.removeprefix("bls:")
            for first in range(begin.year, end.year + 1, 10):
                try:
                    response = http.request(
                        client,
                        "GET",
                        "https://api.bls.gov/publicAPI/v2/timeseries/data/" + native,
                        params={
                            "startyear": first,
                            "endyear": min(first + 9, end.year),
                        },
                    )
                    data = json.loads(response.body)
                    if data["status"] != "REQUEST_SUCCEEDED":
                        raise ValueError(str(data.get("message")))
                    values = []
                    for series in data["Results"]["series"]:
                        for row in series["data"]:
                            year, p = int(row["year"]), row["period"]
                            if not p.startswith("M"):
                                continue
                            month = int(p[1:])
                            dates = period(
                                f"{year}-{1 if month == 13 else month:02d}-01",
                                "year" if month == 13 else "month",
                            )
                            if (
                                dates["period_end"] <= request["since"]
                                or dates["period_start"] >= request["until"]
                            ):
                                continue
                            values.append(
                                dict(
                                    series="bls:" + native,
                                    **dates,
                                    value=number(row["value"]),
                                    observed_at=response.received_at,
                                    metadata={
                                        "native_period": p,
                                        "period_name": row.get("periodName"),
                                        "footnotes": row.get("footnotes"),
                                        "publication_time": "unknown",
                                    },
                                )
                            )
                    definition = dict(
                        series="bls:" + native,
                        source="bls",
                        native_id=native,
                        name=native,
                        unit="percent" if native == "LNS14000000" else None,
                        frequency="native month/year",
                        measure="unemployment rate"
                        if native == "LNS14000000"
                        else None,
                        aggregation="native; M13 is annual, not another month",
                        metadata={
                            "unit_scope": "known default series only; other units not inferred"
                        },
                    )
                    yield dict(
                        series=[definition],
                        series_values=values,
                        read=source_read(
                            "bls",
                            response,
                            detail="native months and footnotes; exact release times unavailable",
                            period_since=request["since"],
                            period_until=request["until"],
                        ),
                    )
                except http.FetchError as exc:
                    yield {"read": dict(source="bls", **exc.read)}
                except (ValueError, KeyError, TypeError) as exc:
                    yield {
                        "read": dict(
                            source="bls",
                            requested_at=stamp(),
                            received_at=stamp(),
                            status="failed",
                            detail=str(exc),
                        )
                    }
