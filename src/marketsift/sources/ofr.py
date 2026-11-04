"""OFR financial stress index and native component series."""

import csv
import io

from ..time import stamp
from . import http
from .common import number, source_read

URL = "https://www.financialresearch.gov/financial-stress-index/data/fsi.csv"


def fetch(request, *, timeout=15, transport=None):
    with http.open_client(timeout=timeout, transport=transport) as client:
        try:
            response = http.request(client, "GET", URL)
            reader = csv.DictReader(io.StringIO(response.body.decode("utf-8-sig")))
            if not reader.fieldnames or reader.fieldnames[:2] != ["Date", "OFR FSI"]:
                raise ValueError("OFR did not return the expected source CSV")
            fields = reader.fieldnames[1:]
            if request.get("series_ids"):
                fields = [
                    f
                    for f in fields
                    if f in request["series_ids"] or "ofr:" + f in request["series_ids"]
                ]
            definitions = [
                dict(
                    series="ofr:" + f,
                    source="ofr",
                    native_id=f,
                    name=f,
                    unit="index/contribution",
                    frequency="source date",
                    measure="financial stress",
                    aggregation="not probability or time sum",
                    metadata={"url": URL},
                )
                for f in fields
            ]
            values, issues = [], []
            for row in reader:
                day = row["Date"]
                if (
                    request.get("since")
                    and day < request["since"]
                    or request.get("until")
                    and day >= request["until"]
                ):
                    continue
                for field in fields:
                    try:
                        values.append(
                            dict(
                                series="ofr:" + field,
                                period_kind="date",
                                period_start=day,
                                period_end=None,
                                value=number(row[field]),
                                observed_at=response.received_at,
                                metadata={
                                    "native_value": row[field],
                                    "publication_time": "unknown",
                                },
                            )
                        )
                    except ValueError as exc:
                        issues.append(str(exc))
            yield dict(
                series=definitions,
                series_values=values,
                read=source_read(
                    "ofr",
                    response,
                    detail="current revision of source dates; release timestamps unknown",
                    status="partial" if issues else "ok",
                    period_since=request.get("since"),
                    period_until=request.get("until"),
                    issues=issues,
                ),
            )
        except http.FetchError as exc:
            yield {"read": dict(source="ofr", **exc.read)}
        except (ValueError, UnicodeError, csv.Error) as exc:
            yield {
                "read": dict(
                    source="ofr",
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="failed",
                    detail=str(exc),
                )
            }
