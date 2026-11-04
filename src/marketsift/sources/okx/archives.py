"""Existing OKX historical-download capability, without packages, hashes or byte quotas."""

import csv
import gzip
import io
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

from ...time import stamp
from .. import http
from ..common import from_ms, milliseconds, number, source_read
from . import get
from .derivatives import scalar

LINK_PATH = "/priapi/v5/broker/public/trade-data/download-link"


def fetch(client, product, request, *, funding=False):
    options = request.get("options", {})
    since, until = milliseconds(request["since"]), milliseconds(request["until"])
    urls = options.get("archive_urls", [])
    try:
        if not urls:
            query = (
                {"instIdList": [product["symbol"]]}
                if product["kind"] == "SPOT"
                else {
                    "instFamilyList": [
                        product["rules"].get("underlying") or product["symbol"]
                    ]
                }
            )
            payload = dict(
                module="3" if funding else "1",
                instType=product["kind"],
                instQueryParam=query,
                dateQuery=dict(
                    dateAggrType=options.get("archive_date_aggr", "daily"),
                    begin=str(since),
                    end=str(until),
                ),
            )
            response, data = get(client, LINK_PATH, method="POST", payload=payload)
            urls = [
                i["url"]
                for group in data["details"]
                for i in group.get("groupDetails", [])
            ]
            yield {
                "read": source_read(
                    "okx",
                    response,
                    detail="native historical download links; web endpoint availability is independent of REST",
                    status="ok" if urls else "partial",
                    archive_links=urls,
                    native_download_response=data,
                )
            }
        for url in urls:
            if urlsplit(url).hostname != "static.okx.com":
                raise ValueError("archive URL does not identify the OKX static source")
            with tempfile.TemporaryDirectory(prefix="marketsift-okx-") as directory:
                path = Path(directory) / "native.zip"
                response = http.save(client, url, path)
                with zipfile.ZipFile(path) as archive:
                    found_csv = False
                    for member in archive.infolist():
                        if not member.filename.lower().endswith((".csv", ".csv.gz")):
                            continue
                        found_csv = True
                        with archive.open(member) as raw:
                            stream = (
                                gzip.GzipFile(fileobj=raw)
                                if member.filename.lower().endswith(".gz")
                                else raw
                            )
                            with io.TextIOWrapper(
                                stream, encoding="utf-8-sig", newline=""
                            ) as text:
                                definitions, values, trades = {}, [], []
                                for row in csv.DictReader(text):
                                    if row.get("instrument_name") != product["symbol"]:
                                        continue
                                    ts = int(
                                        row[
                                            "funding_time"
                                            if funding
                                            else "created_time"
                                        ]
                                    )
                                    if not since <= ts < until:
                                        continue
                                    if funding:
                                        definition, point = scalar(
                                            product,
                                            "funding_rate",
                                            "fraction per source settlement event",
                                            response,
                                            ts,
                                            row["funding_rate"],
                                        )
                                        definitions[definition["series"]] = definition
                                        values.append(point)
                                    else:
                                        trades.append(
                                            dict(
                                                source="okx",
                                                instrument=product["instrument"],
                                                native_id=row["trade_id"],
                                                ts=from_ms(ts),
                                                price=number(row["price"]),
                                                size=number(row["size"]),
                                                side=row.get("side"),
                                                units={
                                                    "price": product["rules"].get(
                                                        "quote"
                                                    ),
                                                    "size": product["rules"].get("base")
                                                    if product["kind"] == "SPOT"
                                                    else "contracts",
                                                },
                                                observed_at=response.received_at,
                                                metadata={
                                                    "native_source": row.get("source")
                                                },
                                            )
                                        )
                                    if len(values) + len(trades) >= 1000:
                                        yield dict(
                                            series=list(definitions.values()),
                                            series_values=values,
                                            trades=trades,
                                            read=source_read(
                                                "okx",
                                                response,
                                                detail="native CSV transaction batch; archive not certified beyond returned file",
                                                archive_member=member.filename,
                                            ),
                                        )
                                        values, trades = [], []
                                yield dict(
                                    series=list(definitions.values()),
                                    series_values=values,
                                    trades=trades,
                                    read=source_read(
                                        "okx",
                                        response,
                                        detail="native archive member read to EOF; no synthetic time filling",
                                        archive_member=member.filename,
                                    ),
                                )
                    if not found_csv:
                        raise ValueError("returned archive contains no CSV member")
    except http.FetchError as exc:
        yield {"read": dict(source="okx", **exc.read)}
    except (
        ValueError,
        KeyError,
        TypeError,
        zipfile.BadZipFile,
        csv.Error,
        UnicodeError,
    ) as exc:
        yield {
            "read": dict(
                source="okx",
                requested_at=stamp(),
                received_at=stamp(),
                status="failed",
                detail=str(exc),
            )
        }
