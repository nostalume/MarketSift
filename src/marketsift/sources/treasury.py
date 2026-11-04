"""Treasury native account/field series, with actual pagination and unit metadata."""

import json

from ..time import stamp
from . import http
from .common import number, source_read

URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance"


def fetch(request, *, timeout=15, transport=None):
    page = 1
    with http.open_client(timeout=timeout, transport=transport) as client:
        while True:
            try:
                response = http.request(
                    client,
                    "GET",
                    URL,
                    params={
                        "filter": f"record_date:gte:{request['since']},record_date:lt:{request['until']}",
                        "sort": "record_date",
                        "page[size]": 1000,
                        "page[number]": page,
                    },
                )
                data = json.loads(response.body)
                definitions, values, documents = {}, [], []
                meta = data["meta"]
                for row in data["data"]:
                    day, account = row["record_date"], row.get("account_type", "")
                    documents.append(
                        dict(
                            source="treasury",
                            native_id=f"{day}:{account}",
                            url=URL,
                            title=f"{day} {account}",
                            text=json.dumps(row, ensure_ascii=False),
                            raw_text=json.dumps(row, ensure_ascii=False),
                            published_label=day,
                            observed_at=response.received_at,
                            metadata={
                                "scope": "native account row",
                                "source_metadata": meta,
                            },
                        )
                    )
                    for field, raw in row.items():
                        native_type = meta.get("dataTypes", {}).get(field, "")
                        if not native_type.startswith(
                            ("CURRENCY", "NUMBER", "PERCENTAGE")
                        ):
                            continue
                        key = f"treasury:{account}:{field}"
                        definitions[key] = dict(
                            series=key,
                            source="treasury",
                            native_id=field,
                            name=meta.get("labels", {}).get(field, field),
                            unit=meta.get("units", {}).get(field)
                            or meta.get("dataFormats", {}).get(field),
                            frequency="source date",
                            measure=field,
                            aggregation="account-specific; scaling not inferred",
                            metadata={
                                "account_type": account,
                                "native_type": meta.get("dataTypes", {}).get(field),
                                "native_format": meta.get("dataFormats", {}).get(field),
                                "unit_scope": "unknown unless explicitly supplied by source",
                            },
                        )
                        values.append(
                            dict(
                                series=key,
                                period_kind="date",
                                period_start=day,
                                period_end=None,
                                value=number(raw),
                                observed_at=response.received_at,
                                metadata={"native_value": raw},
                            )
                        )
                yield dict(
                    documents=documents if "info" in request["datasets"] else [],
                    series=list(definitions.values())
                    if "series" in request["datasets"]
                    else [],
                    series_values=values if "series" in request["datasets"] else [],
                    read=source_read(
                        "treasury",
                        response,
                        detail="source account fields, not an assumed TGA proxy; all available pages",
                        period_since=request["since"],
                        period_until=request["until"],
                        page=page,
                        total_pages=meta.get("total-pages"),
                    ),
                )
                if not data["data"] or page >= int(meta.get("total-pages", page)):
                    return
                page += 1
            except http.FetchError as exc:
                yield {"read": dict(source="treasury", **exc.read)}
                return
            except (ValueError, KeyError, TypeError) as exc:
                yield {
                    "read": dict(
                        source="treasury",
                        requested_at=stamp(),
                        received_at=stamp(),
                        status="failed",
                        detail=str(exc),
                    )
                }
                return
