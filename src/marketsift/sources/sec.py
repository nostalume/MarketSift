"""SEC filing references and company-concept observations, retaining accession identities."""

import json
from datetime import date, timedelta
from urllib.parse import quote

from ..time import stamp
from . import http
from .common import number, source_read


def fetch(request, *, timeout=15, transport=None):
    if not request.get("options", {}).get("cik"):
        raise ValueError("SEC needs options.cik identifying the issuer")
    options = request.get("options", {})
    cik = str(int(options["cik"]))
    concept = options.get("concept", "Revenues")
    filings = request["source"] == "sec-filings"
    url = (
        f"https://data.sec.gov/submissions/CIK{int(cik):010d}.json"
        if filings
        else f"https://data.sec.gov/api/xbrl/companyconcept/CIK{int(cik):010d}/us-gaap/{quote(concept, safe='')}.json"
    )
    headers = {"User-Agent": options["user_agent"]} if options.get("user_agent") else {}
    with http.open_client(timeout=timeout, transport=transport) as client:
        try:
            response = http.request(client, "GET", url, headers=headers)
            data = json.loads(response.body)
            if int(data["cik"]) != int(cik):
                raise ValueError("SEC returned another CIK")
            documents, definitions, values = [], [], []
            if filings:
                recent = data["filings"]["recent"]
                for i, accession in enumerate(recent["accessionNumber"]):
                    row = {k: v[i] for k, v in recent.items()}
                    day = row["filingDate"]
                    if (
                        request.get("since")
                        and day < request["since"]
                        or request.get("until")
                        and day >= request["until"]
                    ):
                        continue
                    link = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/{row.get('primaryDocument', '')}"
                    text = json.dumps(row, ensure_ascii=False, sort_keys=True)
                    documents.append(
                        dict(
                            source="sec-filings",
                            native_id=accession,
                            url=link,
                            title=f"{data.get('name')} {row.get('form')} {row.get('reportDate')}",
                            author=data.get("name"),
                            text=text,
                            raw_text=text,
                            observed_at=response.received_at,
                            published_label=day,
                            links=[link],
                            metadata={
                                "scope": "recent filing index, not filing body",
                                "cik": cik,
                            },
                        )
                    )
                older = [
                    "https://data.sec.gov/submissions/" + r["name"]
                    for r in data["filings"].get("files", [])
                ]
            else:
                if data["taxonomy"] != "us-gaap" or data["tag"] != concept:
                    raise ValueError("SEC returned another concept")
                older = []
                for unit, points in data["units"].items():
                    key = f"sec:{cik}:{concept}:{unit}"
                    definitions.append(
                        dict(
                            series=key,
                            source="sec-concept",
                            native_id=concept,
                            name=data.get("label", concept),
                            unit=unit,
                            frequency="native filing period",
                            measure=concept,
                            aggregation="accessions/units retained; no restatement merge",
                            metadata={
                                "cik": cik,
                                "taxonomy": data["taxonomy"],
                                "description": data.get("description"),
                            },
                        )
                    )
                    for row in points:
                        start = row.get("start") or row["end"]
                        end = (
                            (
                                date.fromisoformat(row["end"]) + timedelta(days=1)
                            ).isoformat()
                            if row.get("start")
                            else None
                        )
                        if (
                            request.get("since")
                            and (
                                end <= request["since"]
                                if end
                                else start < request["since"]
                            )
                            or request.get("until")
                            and start >= request["until"]
                        ):
                            continue
                        native = ":".join(
                            str(row.get(k, ""))
                            for k in ("accn", "frame", "fy", "fp", "form")
                        )
                        values.append(
                            dict(
                                series=key,
                                native_id=native,
                                period_kind="date_range" if end else "date",
                                period_start=start,
                                period_end=end,
                                value=number(row.get("val")),
                                observed_at=response.received_at,
                                metadata={
                                    **row,
                                    "published_date": row.get("filed"),
                                    "exact_publication_time": "unknown",
                                },
                            )
                        )
            yield dict(
                documents=documents,
                series=definitions,
                series_values=values,
                read=source_read(
                    request["source"],
                    response,
                    detail="filing dates retain date precision; values not assumed known before acquisition",
                    older_filing_references=older,
                ),
            )
        except http.FetchError as exc:
            yield {"read": dict(source=request["source"], **exc.read)}
        except (ValueError, KeyError, TypeError) as exc:
            yield {
                "read": dict(
                    source=request["source"],
                    requested_at=stamp(),
                    received_at=stamp(),
                    status="failed",
                    detail=str(exc),
                )
            }
