"""Native New York Fed repo operations and explicitly labeled scalar fields."""

import json

from ..time import stamp
from . import http
from .common import number, source_read

URL = "https://markets.newyorkfed.org/api/rp/all/all/results/lastTwoWeeks.json"


def fetch(request, *, timeout=15, transport=None):
    with http.open_client(timeout=timeout, transport=transport) as client:
        try:
            response = http.request(client, "GET", URL)
            raw = json.loads(response.body)
            definitions, values, documents = {}, [], []
            for row in raw["repo"]["operations"]:
                day = row.get("operationDate")
                if day and (
                    request.get("since")
                    and day < request["since"]
                    or request.get("until")
                    and day >= request["until"]
                ):
                    continue
                identity = row.get("operationId") or row.get("operationID")
                text = json.dumps(row, ensure_ascii=False, sort_keys=True)
                documents.append(
                    dict(
                        source="nyfed",
                        native_id=str(identity) if identity is not None else None,
                        url=URL,
                        title="Repo operation " + str(day),
                        text=text,
                        raw_text=text,
                        observed_at=response.received_at,
                        published_label=day,
                        metadata={
                            "scope": "native operation JSON; units not converted",
                            "native_id_missing": identity is None,
                        },
                    )
                )
                if not day:
                    continue
                for field in (
                    "totalAmtAccepted",
                    "totalAmtSubmitted",
                    "rate",
                    "stopOutRate",
                ):
                    if row.get(field) in (None, ""):
                        continue
                    key = f"nyfed:{row.get('operationType', '')}:{row.get('term', '')}:{field}"
                    definitions[key] = dict(
                        series=key,
                        source="nyfed",
                        native_id=field,
                        name=field,
                        unit=None,
                        frequency="operation date",
                        measure=field,
                        aggregation="operation-specific; unit/scaling not inferred",
                        metadata={
                            "operation_type": row.get("operationType"),
                            "term": row.get("term"),
                        },
                    )
                    values.append(
                        dict(
                            series=key,
                            native_id=str(identity) if identity is not None else "",
                            period_kind="date",
                            period_start=day,
                            period_end=None,
                            value=number(row[field]),
                            observed_at=response.received_at,
                            metadata={"native_operation": row},
                        )
                    )
            yield dict(
                documents=documents if "info" in request["datasets"] else [],
                series=list(definitions.values())
                if "series" in request["datasets"]
                else [],
                series_values=values if "series" in request["datasets"] else [],
                read=source_read(
                    "nyfed",
                    response,
                    detail="source latest two weeks only; exact release times/scaling not inferred",
                    period_since=request.get("since"),
                    period_until=request.get("until"),
                ),
            )
        except http.FetchError as exc:
            yield {"read": dict(source="nyfed", **exc.read)}
        except (ValueError, KeyError, TypeError) as exc:
            yield {
                "read": dict(
                    source="nyfed",
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="failed",
                    detail=str(exc),
                )
            }
