"""Public Tencent quote snapshots, separate from native five-minute history."""

import re

from ..time import stamp
from . import http, tdx
from .common import china_time, number, source_read


def fetch(request, *, timeout=15, transport=None):
    wanted = list(dict.fromkeys(request.get("instruments", ())))
    if not wanted:
        raise ValueError("Tencent needs explicit exchange-qualified instruments")
    for key in wanted:
        tdx.instrument(key)
    with http.open_client(timeout=timeout, transport=transport) as client:
        # URL batches are transport organization, not a total instrument limit.
        for offset in range(0, len(wanted), 80):
            keys = wanted[offset : offset + 80]
            try:
                response = http.request(
                    client,
                    "GET",
                    "https://qt.gtimg.cn/q=" + ",".join(k.lower() for k in keys),
                )
                text = response.body.decode("gb18030")
                refs = []
                defs = []
                values = []
                seen = set()
                for match in re.finditer(r'v_((?:sh|sz|bj)\d{6})="([^"\n]*)"', text):
                    key = match[1].upper()
                    parts = match[2].split("~")
                    if key not in keys or len(parts) < 35:
                        continue
                    seen.add(key)
                    ref = tdx.instrument(key)
                    ref.update(name=parts[1])
                    refs.append(ref)
                    label = parts[30]
                    when = china_time(
                        label[:4]
                        + "-"
                        + label[4:6]
                        + "-"
                        + label[6:8]
                        + " "
                        + label[8:10]
                        + ":"
                        + label[10:12]
                        + ":"
                        + label[12:14]
                    )
                    if when is None:
                        raise ValueError("native quote time unavailable")
                    for index, metric, unit in [
                        (3, "last", "CNY"),
                        (4, "previous_close", "CNY"),
                        (5, "open", "CNY"),
                        (33, "high", "CNY"),
                        (34, "low", "CNY"),
                        (32, "change_pct", "percent"),
                    ]:
                        series = "tencent:" + key + ":" + metric
                        defs.append(
                            dict(
                                series=series,
                                source="tencent",
                                native_id=str(index),
                                instrument=key,
                                name=metric,
                                unit=unit,
                                frequency="snapshot",
                                measure=metric,
                                aggregation="not intraday history",
                                metadata={"source_field_index": index},
                            )
                        )
                        values.append(
                            dict(
                                series=series,
                                period_kind="instant",
                                period_start=when,
                                value=number(parts[index]),
                                observed_at=response.received_at,
                                metadata={
                                    "native_fields": parts,
                                    "history_supported": False,
                                },
                            )
                        )
                yield dict(
                    instruments=refs,
                    series=defs,
                    series_values=values,
                    read=source_read(
                        "tencent",
                        response,
                        detail="native current quote snapshots; historical requested dates cannot be backfilled",
                        status="partial" if set(keys) - seen else "ok",
                        missing_instruments=sorted(set(keys) - seen),
                        instruments=keys,
                    ),
                )
            except http.FetchError as exc:
                yield {"read": dict(source="tencent", **exc.read)}
            except (ValueError, KeyError, IndexError) as exc:
                yield {
                    "read": dict(
                        source="tencent",
                        requested_at=stamp(),
                        received_at=stamp(),
                        status="failed",
                        detail=str(exc),
                    )
                }
