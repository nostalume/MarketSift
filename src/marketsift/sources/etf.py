"""Official daily ETF shares; native units/revisions, not investor identities."""

import json
from datetime import date, timedelta

from . import http, tdx
from .common import number, source_read

SSE = "https://query.sse.com.cn/commonQuery.do"
SZSE = "https://www.szse.cn/api/report/ShowReport/data"


def fetch(request, *, timeout=15, transport=None):
    source = request["source"]
    sh = source == "sse-etf"
    wanted = set(request.get("instruments", ()))
    begin, end = (
        date.fromisoformat(request["since"]),
        date.fromisoformat(request["until"]),
    )
    with http.open_client(timeout=timeout, transport=transport) as client:
        for ordinal in range((end - begin).days):
            day = (begin + timedelta(days=ordinal)).isoformat()
            page = 1
            while True:
                params = (
                    dict(
                        sqlId="COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
                        STAT_DATE=day,
                        isPagination="true",
                        **{"pageHelp.pageSize": 1000, "pageHelp.pageNo": page},
                    )
                    if sh
                    else dict(
                        SHOWTYPE="JSON",
                        CATALOGID="scsj_fund_jjgm",
                        jjlb="ETF",
                        txtStart=day,
                        txtEnd=day,
                        PAGENO=page,
                    )
                )
                try:
                    response = http.request(
                        client,
                        "GET",
                        SSE if sh else SZSE,
                        params=params,
                        headers={
                            "Referer": "https://www.sse.com.cn/"
                            if sh
                            else "https://www.szse.cn/"
                        },
                    )
                    data = json.loads(response.body)
                    if sh:
                        rows = data["pageHelp"]["data"]
                        meta = data["pageHelp"]
                        pages = int(meta.get("pageCount", 1))
                        unit = None
                    else:
                        meta = data[0]["metadata"]
                        rows = data[0]["data"]
                        pages = int(meta.get("pagecount", 1))
                        unit = (
                            "10,000 shares"
                            if "万份" in meta.get("cols", {}).get("current_size", "")
                            else None
                        )
                    refs = []
                    definitions = []
                    values = []
                    issues = []
                    for row in rows:
                        native = row["SEC_CODE"] if sh else row["fund_code"]
                        key = ("SH" if sh else "SZ") + native
                        if wanted and key not in wanted:
                            continue
                        period = row["STAT_DATE"] if sh else row["size_date"]
                        if period != day:
                            issues.append("returned date differs: " + period)
                            continue
                        ref = tdx.instrument(key)
                        ref.update(
                            name=row["SEC_NAME"] if sh else row["security_short_name"],
                            metadata={"native": row},
                        )
                        refs.append(ref)
                        series = source + ":" + native + ":shares"
                        definitions.append(
                            dict(
                                series=series,
                                source=source,
                                native_id=native,
                                instrument=key,
                                name=ref["name"] + " ETF shares",
                                unit=unit,
                                frequency="source date",
                                measure="ETF shares outstanding",
                                aggregation="stock level; not net inflow",
                                metadata={
                                    "native_field": "TOT_VOL" if sh else "current_size",
                                    "native_columns": meta.get("cols"),
                                    "unit_scope": "source label only; SSE scaling unconfirmed"
                                    if sh
                                    else "source column label",
                                },
                            )
                        )
                        values.append(
                            dict(
                                series=series,
                                period_kind="date",
                                period_start=day,
                                period_end=None,
                                value=number(
                                    (
                                        row["TOT_VOL"] if sh else row["current_size"]
                                    ).replace(",", "")
                                ),
                                observed_at=response.received_at,
                                metadata={
                                    "native": row,
                                    "revision_scope": "reference/current source observation; later revisions retained",
                                },
                            )
                        )
                    yield dict(
                        instruments=refs,
                        series=definitions,
                        series_values=values,
                        read=source_read(
                            source,
                            response,
                            detail="official returned ETF share rows; no national-team inference",
                            status="partial" if issues or not rows else "ok",
                            issues=issues,
                            source_rows=len(rows),
                            page=page,
                            total_pages=pages,
                            period_since=day,
                            period_until=day,
                        ),
                    )
                    if page >= pages or not rows:
                        break
                    page += 1
                except http.FetchError as exc:
                    yield {"read": dict(source=source, **exc.read)}
                    break
                except (ValueError, KeyError, TypeError, IndexError) as exc:
                    from ..time import stamp

                    yield {
                        "read": dict(
                            source=source,
                            requested_at=stamp(),
                            received_at=stamp(),
                            status="failed",
                            detail=str(exc),
                        )
                    }
                    break
