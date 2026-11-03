"""Small public macro snapshots: FRED CSV and the Federal Reserve policy feed.

CSV contains the revision available at retrieval, not historical vintage data.
Source formats enter once here; consumers reuse the returned values.
"""
from __future__ import annotations

import calendar
import csv
import io
import json
import math
import time
from bisect import bisect_right
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree

import httpx

from msharesift.files import atomic_write

FRED_CSV = 'https://fred.stlouisfed.org/graph/fredgraph.csv'
POLICY_FEED = 'https://www.federalreserve.gov/feeds/press_monetary.xml'
# Metadata for common series, checked against their official series pages.
# Other IDs can be fetched; their unestablished units/frequency remain unknown.
SERIES = {
    'WALCL': ('Federal Reserve assets', 'USD millions, NSA', 'weekly Wednesday', 'level'),
    'WRBWFRBL': ('Reserve balances', 'USD millions, NSA', 'weekly Wednesday', 'level'),
    'WDTGAL': ('Treasury General Account', 'USD millions, NSA', 'weekly Wednesday', 'level'),
    'RRPONTSYD': ('Overnight reverse repo', 'USD billions, NSA', 'daily', 'level'),
    'DFF': ('Effective federal funds rate', 'percent, NSA', 'daily 7-day', 'rate'),
    'DFII10': ('10-year real Treasury yield', 'percent, NSA', 'daily', 'rate'),
    'BAMLH0A0HYM2': ('US high-yield OAS', 'percent, NSA', 'daily close', 'rate'),
    'CPIAUCSL': ('Headline CPI', 'index 1982-84=100, SA', 'monthly', 'level'),
    'CPILFESL': ('Core CPI', 'index 1982-84=100, SA', 'monthly', 'level'),
    'UNRATE': ('Unemployment rate', 'percent, SA', 'monthly', 'rate'),
    'INDPRO': ('Industrial production', 'index 2017=100, SA', 'monthly', 'level'),
    'GDPC1': ('Real GDP', 'USD billions chained 2017, SA annual rate', 'quarterly', 'level'),
}


def _fred_rows(payload, series_id, start, end):
    reader = csv.reader(io.StringIO(payload.decode('utf-8-sig')))
    header = next(reader, [])
    if header not in (['observation_date', series_id], ['DATE', series_id]):
        raise ValueError('fred_csv_columns')
    rows, previous = [], None
    for row in reader:
        if not row:
            continue
        if len(row) != 2:
            raise ValueError('fred_csv_row')
        observed = date.fromisoformat(row[0])
        if not start <= observed <= end or (previous is not None and observed <= previous):
            raise ValueError('fred_observation_order_or_window')
        value = None if row[1] in ('', '.') else float(row[1])
        if value is not None and not math.isfinite(value):
            raise ValueError('fred_nonfinite_value')
        rows.append([observed.isoformat(), value])
        previous = observed
    return rows


def _policy_rows(payload, start, end):
    root = ElementTree.fromstring(payload)
    channel = root.find('channel')
    if root.tag != 'rss' or channel is None:
        raise ValueError('policy_rss_channel')
    releases = []
    for item in channel.findall('item'):
        published = parsedate_to_datetime(item.findtext('pubDate', ''))
        if published.tzinfo is None:
            raise ValueError('policy_publication_timezone')
        published = published.astimezone(UTC)
        if not start <= published.date() <= end:
            continue
        title, url = item.findtext('title'), item.findtext('link')
        if not title or not url:
            raise ValueError('policy_release_identity')
        releases.append({'title': title, 'url': url, 'published_at': published.isoformat()})
    return releases


def fetch_macro(root, series_ids, start, end, *, policy=False, seconds=90,
                max_response_bytes=2*1024**2, transport=None):
    """Fetch selected series once; reuse successes and resume only failed sources.

    A root fixes its IDs, observation window and retrieval revision. Use another
    root to obtain a fresh revision. Missing dates are never synthesized.
    """
    ids = sorted(set(series_ids))
    if start > end or seconds <= 0 or max_response_bytes <= 0 or not (ids or policy):
        raise ValueError('select series/policy and a valid observation window/budget')
    root = Path(root)
    path = root/'macro.json'
    contract = {'series_ids': ids, 'start': start.isoformat(), 'end': end.isoformat(),
                'policy_feed': policy, 'revision': 'latest_at_retrieval'}
    if path.exists():
        pack = json.loads(path.read_bytes())
        if pack['contract'] != contract:
            raise ValueError('macro snapshot contract mismatch; use a new root')
    else:
        pack = {'contract': contract, 'created_at': datetime.now(UTC).isoformat(), 'series': {}}
    pending = [sid for sid in ids if pack['series'].get(sid, {}).get('status') != 'ok']
    need_policy = policy and pack.get('policy', {}).get('status') != 'ok'
    requests, received = 0, 0
    deadline = time.monotonic()+seconds

    def get(client, url, params=None):
        nonlocal requests, received
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError('macro_acquisition_budget')
        requests += 1
        with client.stream('GET', url, params=params, timeout=min(15, remaining)) as response:
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_bytes():
                received += len(chunk)
                content.extend(chunk)
                if len(content) > max_response_bytes:
                    raise ValueError('macro_response_budget')
                if time.monotonic() >= deadline:
                    raise TimeoutError('macro_acquisition_budget')
            return bytes(content), str(response.url)

    if pending or need_policy:
        root.mkdir(parents=True, exist_ok=True)
        with httpx.Client(transport=transport, follow_redirects=True) as client:
            for sid in pending:
                title, units, frequency, measure = SERIES.get(sid, (sid, None, None, None))
                entry = {'title': title, 'units': units, 'frequency': frequency, 'measure': measure,
                         'source': f'https://fred.stlouisfed.org/series/{sid}'}
                if sid == 'BAMLH0A0HYM2':
                    entry['coverage_note'] = 'FRED limits this ICE series to three years from April 2026; see source notes.'
                try:
                    payload, url = get(client, FRED_CSV,
                                       {'id': sid, 'cosd': start.isoformat(), 'coed': end.isoformat()})
                    entry.update(status='ok', rows=_fred_rows(payload, sid, start, end),
                                 download_url=url, retrieved_at=datetime.now(UTC).isoformat())
                except (httpx.HTTPError, ValueError, csv.Error, TimeoutError) as exc:
                    entry.update(status='failed', error=f'{type(exc).__name__}: {exc}')
                pack['series'][sid] = entry
            if need_policy:
                entry = {'source': POLICY_FEED, 'coverage_note': 'Recent RSS items only, not a full policy archive.'}
                try:
                    payload, url = get(client, POLICY_FEED)
                    entry.update(status='ok', releases=_policy_rows(payload, start, end),
                                 download_url=url, retrieved_at=datetime.now(UTC).isoformat())
                except (httpx.HTTPError, ValueError, ElementTree.ParseError, TimeoutError) as exc:
                    entry.update(status='failed', error=f'{type(exc).__name__}: {exc}')
                pack['policy'] = entry
        pack['status'] = 'complete' if (all(e['status'] == 'ok' for e in pack['series'].values())
                          and (not policy or pack['policy']['status'] == 'ok')) else 'partial'
        pack['last_attempt_at'] = datetime.now(UTC).isoformat()
        # One snapshot, one serialization. Complete means acquisition succeeded,
        # not that every calendar date exists or the requested history is available.
        atomic_write(path, json.dumps(pack, ensure_ascii=False, allow_nan=False).encode())
    return {**pack, 'requests_this_call': requests, 'received_bytes_this_call': received}


def summarize_macro(pack):
    """Calendar comparisons of admitted observations; no common-frequency filling."""
    result = {}
    for sid, entry in pack['series'].items():
        if entry['status'] != 'ok':
            result[sid] = {'status': entry['status'], 'error': entry['error']}
            continue
        points = [(observed, value) for observed, value in entry['rows'] if value is not None]
        summary = {key: entry[key] for key in ('title', 'units', 'frequency', 'source', 'retrieved_at')}
        summary.update(observations=len(entry['rows']), numeric_observations=len(points),
                       missing_observations=len(entry['rows'])-len(points),
                       first_available=points[0][0] if points else None,
                       latest={'observed_at': points[-1][0], 'value': points[-1][1]} if points else None)
        if 'coverage_note' in entry:
            summary['coverage_note'] = entry['coverage_note']
        comparisons = {}
        if points:
            dates = [p[0] for p in points]
            latest_date = date.fromisoformat(points[-1][0])
            for months in (3, 12, 36):
                year, month = divmod(latest_date.year*12+latest_date.month-1-months, 12)
                reference = date(year, month+1, min(latest_date.day, calendar.monthrange(year, month+1)[1]))
                index = bisect_right(dates, reference.isoformat())-1
                comparison = {'reference_cutoff': reference.isoformat(), 'reference': None}
                if index >= 0:
                    observed, value = points[index]
                    comparison['reference'] = {'observed_at': observed, 'value': value}
                    comparison['change'] = points[-1][1]-value
                    comparison['change_unit'] = 'percentage_points' if entry['measure'] == 'rate' else entry['units']
                    if entry['measure'] == 'level':
                        comparison['relative_change_pct'] = (points[-1][1]/value-1)*100 if value else None
                comparisons[str(months)] = comparison
        summary['calendar_month_comparisons'] = comparisons
        result[sid] = summary
    return result
