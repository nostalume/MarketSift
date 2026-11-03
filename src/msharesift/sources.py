"""On-demand public releases, research feeds and source-native official data.

No search engine or background crawler. Sources enter once; the agent consumes
the returned content and follows relevant links rather than counting headlines.
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
import time
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote, urlencode, urljoin, urlsplit
from xml.etree import ElementTree as ET

import httpx

from msharesift.files import atomic_write

FEEDS = {
    'bea': ('official', 'https://apps.bea.gov/rss/rss.xml'),
    'bls-releases': ('official', 'https://www.bls.gov/feed/bls_latest.rss'),
    'coindesk': ('news', 'https://www.coindesk.com/arc/outboundfeeds/rss/'),
    'cointelegraph': ('news', 'https://cointelegraph.com/rss'),
    'blockworks': ('news', 'https://blockworks.com/feed'),
    'ethereum-research': ('forum', 'https://ethresear.ch/latest.rss'),
    'uniswap': ('forum', 'https://gov.uniswap.org/latest.rss'),
    'reddit-cryptocurrency': ('forum', 'https://www.reddit.com/r/CryptoCurrency/new/.rss'),
    'hayes': ('kol', 'https://cryptohayes.substack.com/feed'),
    'lynalden': ('kol', 'https://www.lynalden.com/feed/'),
    'bitcoinmarkets': ('forum', 'https://www.reddit.com/r/BitcoinMarkets/new/.rss'),
    'conks': ('kol', 'https://www.conks.plumbing/feed'),
    'cryptoslate': ('news', 'https://cryptoslate.com/feed/'),
    'coinmetrics': ('research', 'https://coinmetrics.substack.com/feed'),
    'aave': ('forum', 'https://governance.aave.com/latest.rss'),
    'apricitas': ('research', 'https://www.apricitas.io/feed'),
}
OFFICIAL = ('nyfed', 'treasury', 'ofr', 'bls', 'sec-filings', 'sec-concept')


class _Text(HTMLParser):
    """One HTML pass retaining links, figures, quotations and table structure."""

    def __init__(self, base_url=''):
        super().__init__(convert_charrefs=True)
        self.parts, self.article, self.links, self.metadata = [], [], [], {}
        self.images, self.article_images, self.article_links = [], [], []
        self.depth, self.ignored, self.title_depth = 0, 0, 0
        self.title, self.anchors = [], []
        self.base_url, self.table_depth = base_url, 0

    def _emit(self, value):
        self.parts.append(value)
        if self.depth:
            self.article.append(value)

    def _url(self, value):
        url = urljoin(self.base_url, value) if value else None
        return url if url and urlsplit(url).scheme in ('http', 'https') else None

    def handle_starttag(self, tag, attrs):
        tag = tag.rsplit(':', 1)[-1]
        attrs = dict(attrs)
        if tag in ('script', 'style', 'noscript', 'svg'):
            self.ignored += 1
        if self.ignored:
            return
        if tag in ('article', 'main'):
            self.depth += 1
        if tag == 'title':
            self.title_depth += 1
        if tag == 'meta':
            key = attrs.get('property') or attrs.get('name')
            if key in ('author', 'article:published_time', 'article:modified_time'):
                self.metadata[key] = attrs.get('content')
        if tag == 'a':
            url = self._url(attrs.get('href'))
            self.anchors.append(url)
            if url:
                self.links.append(url)
                if self.depth:
                    self.article_links.append(url)
                self._emit('[')
        if tag == 'img':
            url = self._url(attrs.get('src')) or self._url(attrs.get('data-src'))
            alt = attrs.get('alt', '')
            image = {'url': url, 'alt': alt, 'title': attrs.get('title'),
                     'srcset': attrs.get('srcset'),
                     'link_url': self.anchors[-1] if self.anchors else None}
            self.images.append(image)
            if self.depth:
                self.article_images.append(image)
            label = alt.replace(']', r'\]').replace('\n', ' ')
            self._emit(f'![{label}](<{url}>)' if url else f'[image: {label}]')
        if tag == 'table':
            self.table_depth += 1
            self._emit('\n<table>\n')
        elif tag in ('thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'caption') and self.table_depth:
            spans = ''.join(f' {key}="{escape(attrs[key], quote=True)}"'
                            for key in ('colspan', 'rowspan') if key in attrs)
            self._emit(('<tr>' if tag == 'tr' else f'<{tag}{spans}>'))
        elif tag == 'blockquote':
            self._emit('\n<blockquote>\n')
        elif tag == 'pre':
            self._emit('\n```\n')
        elif tag in ('p', 'br', 'div', 'figure', 'figcaption', 'h1', 'h2', 'h3', 'h4'):
            self._emit('\n')
        elif tag == 'li':
            self._emit('\n- ')

    def handle_startendtag(self, tag, attrs):
        tag = tag.rsplit(':', 1)[-1]
        self.handle_starttag(tag, attrs)
        if tag not in ('img', 'br', 'meta', 'link', 'input', 'hr'):
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        tag = tag.rsplit(':', 1)[-1]
        if tag in ('script', 'style', 'noscript', 'svg'):
            self.ignored = max(0, self.ignored-1)
            return
        if self.ignored:
            return
        if tag == 'a' and self.anchors:
            url = self.anchors.pop()
            if url:
                self._emit(f'](<{url}>)')
        if tag == 'table' and self.table_depth:
            self._emit('\n</table>\n')
            self.table_depth -= 1
        elif tag in ('thead', 'tbody', 'tfoot', 'tr', 'th', 'td', 'caption') and self.table_depth:
            self._emit(f'</{tag}>'+ ('\n' if tag == 'tr' else ''))
        elif tag == 'blockquote':
            self._emit('\n</blockquote>\n')
        elif tag == 'pre':
            self._emit('\n```\n')
        elif tag in ('p', 'li', 'div', 'figure', 'figcaption', 'h1', 'h2', 'h3', 'h4'):
            self._emit('\n')
        if tag in ('article', 'main'):
            self.depth = max(0, self.depth-1)
        if tag == 'title':
            self.title_depth = max(0, self.title_depth-1)

    def handle_data(self, data):
        if not self.ignored:
            self._emit(escape(data, quote=False) if self.table_depth else data)
            if self.title_depth:
                self.title.append(data)

    def text(self, article=False):
        return '\n'.join(line.strip() for line in ''.join(self.article if article else self.parts).splitlines() if line.strip())

    def resources(self, article=False):
        return {'links': list(dict.fromkeys(self.article_links if article else self.links)),
                'images': self.article_images if article else self.images}


def _passages(text, max_chars=None, focus=(), offset=0):
    """Choose passages in stored text coordinates, without parsing or I/O."""
    found, missing = [], []
    for term in dict.fromkeys(focus):
        hit = re.search(re.escape(term), text, re.IGNORECASE) if term else None
        (found if hit else missing).append((term, hit.start()) if hit else term)
    ranges = []
    if found:
        found = found[:max_chars] if max_chars is not None else found
        width = max_chars // len(found) if max_chars is not None else len(text)
        for _, start in sorted(found, key=lambda item: item[1]):
            left = max(0, start-min(250, width//4))
            right = min(len(text), left+width)
            if ranges and left <= ranges[-1][1]:
                ranges[-1][1] = max(ranges[-1][1], right)
            else:
                ranges.append([left, right])
    else:
        ranges = [[min(offset, len(text)), min(len(text), offset+max_chars) if max_chars is not None else len(text)]]
    return {'text': '\n[…]\n'.join(text[a:b] for a,b in ranges),
            'text_ranges': ranges,
            'text_selection': 'focus passages' if found else ('offset' if offset else 'head'),
            'focus_not_found': missing,
            'truncated': ranges != [[0, len(text)]]}


def _content(parser, text, max_chars=None, focus=(), *, article=False):
    return {**_passages(text, max_chars, focus), 'text_format': 'markdown_with_html_tables',
            'supplied_text_characters': len(text), **parser.resources(article)}


def _select_records(records, *, authors=(), topics=(), urls=(), focus=(),
                    limit=None, max_chars=None, offset=0, catalog=False):
    wanted_authors = {a.strip().casefold() for a in authors}
    selected = []
    for record in records:
        if limit is not None and len(selected) >= limit:
            break
        author = record.get('author') or record.get('metadata', {}).get('author')
        if wanted_authors and (not author or author.strip().casefold() not in wanted_authors):
            continue
        if urls and record.get('url') not in urls:
            continue
        text = record['text']
        if topics and not _topic_matches((record.get('title') or '')+'\n'+text, topics):
            continue
        if catalog:
            item = {k: v for k, v in record.items() if k in (
                'title', 'url', 'author', 'published_at', 'updated_at', 'created_at',
                'post_number', 'reply_to_post_number', 'scope', 'metadata')}
            item.update(stored_text_characters=len(text), source_truncated=record.get('truncated', False),
                        links_count=len(record.get('links', [])), images_count=len(record.get('images', [])))
        else:
            item = {**record, **_passages(text, max_chars, focus, offset),
                    'stored_text_characters': len(text), 'range_basis': 'stored text',
                    'source_truncated': record.get('truncated', False)}
            if item['source_truncated']:
                item['source_text_ranges'] = record.get('text_ranges')
                item['truncated'] = True
        selected.append(item)
    return selected


def select_sources(pack, *, sources=(), urls=(), authors=(), topics=(), focus=(),
                   limit=None, max_chars=None, offset=0, catalog=False):
    """Read a different view of an admitted snapshot; no network, writes or HTML parsing."""
    if offset < 0 or any(v is not None and v <= 0 for v in (limit, max_chars)):
        raise ValueError('select positive reading bounds and a nonnegative offset')
    selection = dict(authors=authors, topics=topics, urls=urls, focus=focus,
                     limit=limit, max_chars=max_chars, offset=offset, catalog=catalog)
    viewed = {}
    for name, entry in pack['sources'].items():
        if sources and name not in sources:
            continue
        if entry['status'] != 'ok':
            viewed[name] = entry
            continue
        data = entry['data']
        if entry['kind'] in ('feed', 'discourse'):
            key = 'items' if entry['kind'] == 'feed' else 'posts'
            records = _select_records(data[key], **selection)
            viewed[name] = {**entry, 'data': {**data, key: records,
                            'stored_records': len(data[key]), 'selected_records': len(records)}}
        elif entry['kind'] == 'page':
            records = _select_records([{**data, 'url': entry.get('final_url', entry['url'])}], **selection)
            if records:
                viewed[name] = {**entry, 'data': records[0]}
        elif not (urls or authors or topics):
            viewed[name] = {**entry, 'data': {'scope': data['scope']} if catalog else data}
    return {**pack, 'sources': viewed, 'requests_this_call': 0, 'received_bytes_this_call': 0}


def _topic_matches(text, topics):
    for term in topics:
        pattern = re.escape(term)
        if term.isascii() and term and term[0].isalnum() and term[-1].isalnum():
            pattern = r'(?<![a-z0-9_])'+pattern+r'(?![a-z0-9_])'
        if term and re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _published(raw):
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except ValueError:
        try:
            value = parsedate_to_datetime(raw)
        except (ValueError, TypeError):
            return None
    return value.astimezone(UTC).isoformat() if value.tzinfo else None


def _embedded(node):
    if node is None:
        return None
    return (node.text or '')+''.join(ET.tostring(child, encoding='unicode') for child in node)


def read_feed(payload, url, limit=None, max_chars=None, *, authors=(), topics=(), focus=()):
    root = ET.fromstring(payload)
    atom = '{http://www.w3.org/2005/Atom}'
    if root.tag == 'rss' and root.find('channel') is not None:
        entries, prefix = root.findall('./channel/item'), ''
    elif root.tag == atom+'feed':
        entries, prefix = root.findall(atom+'entry'), atom
    else:
        raise ValueError('expected RSS channel or Atom feed')
    items, seen, examined = [], set(), 0
    for entry in entries:
        examined += 1
        if prefix:
            link = next((e.get('href') for e in entry.findall(atom+'link')
                         if e.get('rel', 'alternate') == 'alternate'), None)
            content = _embedded(entry.find(atom+'content'))
            summary = _embedded(entry.find(atom+'summary')) or ''
            raw_date = entry.findtext(atom+'published')
            updated = entry.findtext(atom+'updated')
            author = entry.findtext(atom+'author/'+atom+'name')
        else:
            link = entry.findtext('link')
            content = entry.findtext('{http://purl.org/rss/1.0/modules/content/}encoded')
            summary = entry.findtext('description', '')
            raw_date, updated = entry.findtext('pubDate'), None
            author = entry.findtext('author') or entry.findtext('{http://purl.org/dc/elements/1.1/}creator')
        title = entry.findtext(prefix+'title')
        if not link or not title:
            raise ValueError('feed item lacks title/link')
        link = urljoin(url, link)
        if link in seen:
            continue
        parser = _Text(link)
        parser.feed(content or '')
        text = parser.text()
        scope = 'feed_content'
        if not text:
            parser = _Text(link)
            parser.feed(summary)
            text = parser.text()
            scope = 'feed_summary' if text else 'feed_metadata'
        seen.add(link)
        items.append({'title': title, 'url': link, 'author': author,
                      'published_at': _published(raw_date), 'published_raw': raw_date,
                      'updated_at': _published(updated), 'updated_raw': updated,
                      'scope': scope, **_content(parser, text)})
    if limit is not None or max_chars is not None or authors or topics or focus:
        items = _select_records(items, authors=authors, topics=topics, focus=focus,
                                limit=limit, max_chars=max_chars)
    return {'items': items, 'feed_entries': len(entries), 'examined_entries': examined,
            'selection': {'authors': list(authors), 'topics': list(topics)},
            'scope': 'current feed, source order; not an archive or representative sample'}


def source_requests(names, *, start=None, end=None, cik=None, concept='Revenues',
                    bls_series='LNS14000000', feeds=(), pages=()):
    """Build only selected requests. Dates scope official observations, not feeds."""
    requests = []
    for name in sorted(set(names)):
        spec = {'name': name}
        if name in FEEDS:
            category, url = FEEDS[name]
            spec.update(kind='feed', category=category, url=url)
        elif name in ('treasury', 'ofr', 'bls'):
            if start is None or end is None or start > end:
                raise ValueError(f'{name} requires --start and --end observation dates')
            spec.update(kind=name, category='official', start=str(start), end=str(end))
            if name == 'treasury':
                query = urlencode({'filter': f'record_date:gte:{start},record_date:lte:{end}',
                                   'sort': 'record_date', 'page[size]': 1000})
                spec['url'] = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance?'+query
            elif name == 'ofr':
                spec['url'] = 'https://www.financialresearch.gov/financial-stress-index/data/fsi.csv'
            else:
                if end.year-start.year >= 10:
                    raise ValueError('unregistered BLS window must be at most ten inclusive years')
                spec.update(url='https://api.bls.gov/publicAPI/v2/timeseries/data/'+quote(bls_series, safe='')+
                            '?'+urlencode({'startyear': start.year, 'endyear': end.year}), series_id=bls_series)
        elif name == 'nyfed':
            spec.update(kind=name, category='official', url='https://markets.newyorkfed.org/api/rp/all/all/results/lastTwoWeeks.json')
        elif name in ('sec-filings', 'sec-concept'):
            if cik is None or not str(cik).isascii() or not str(cik).isdigit() or len(str(cik)) > 10:
                raise ValueError('SEC source requires a numeric CIK, at most ten digits')
            spec.update(kind=name, category='official', cik=int(cik))
            if name == 'sec-filings':
                spec['url'] = f'https://data.sec.gov/submissions/CIK{int(cik):010}.json'
            else:
                spec.update(url=f'https://data.sec.gov/api/xbrl/companyconcept/CIK{int(cik):010}/us-gaap/{quote(concept, safe="")}.json', concept=concept)
        else:
            raise ValueError(f'unknown public source: {name}')
        requests.append(spec)
    for kind, urls in (('feed', feeds), ('page', pages)):
        for url in dict.fromkeys(urls):
            parsed = urlsplit(url)
            if parsed.scheme not in ('http', 'https'):
                raise ValueError('public source URL requires http or https')
            spec = {'name': kind+':'+url, 'kind': kind, 'category': 'selected', 'url': url}
            if kind == 'page' and parsed.hostname in ('gov.uniswap.org', 'ethresear.ch', 'governance.aave.com') and parsed.path.startswith('/t/'):
                topic = next((p.removesuffix('.json') for p in parsed.path.split('/')[2:]
                              if p.removesuffix('.json').isdigit()), None)
                if topic is not None:
                    base = f'https://{parsed.hostname}/t/{int(topic)}'
                    spec.update(kind='discourse', topic_id=int(topic), topic_url=base, url=base+'.json')
            if not any(r['url'] == spec['url'] for r in requests):
                requests.append(spec)
    return requests


def _read_source(payload, spec, limit=None, max_chars=None, *, authors=(), topics=(), focus=()):
    kind = spec['kind']
    if kind == 'feed':
        return read_feed(payload, spec['url'], limit, max_chars, authors=authors, topics=topics, focus=focus)
    if kind == 'page':
        parser = _Text(spec['url'])
        parser.feed(payload.decode('utf-8-sig'))
        text = parser.text(article=bool(parser.article))
        return {'title': ''.join(parser.title), 'metadata': parser.metadata,
                'scope': 'article/main text' if parser.article else 'page text; may include navigation',
                **_content(parser, text, max_chars, focus, article=bool(parser.article))}
    if kind == 'discourse':
        value = json.loads(payload)
        if value['id'] != spec['topic_id']:
            raise ValueError('forum topic identity mismatch')
        supplied = value['post_stream']['posts']
        posts = []
        for post in supplied[:limit]:
            parser = _Text(spec['topic_url'])
            parser.feed(post['cooked'])
            text = parser.text()
            posts.append({'id': post['id'], 'post_number': post['post_number'],
                          'author': post['username'], 'created_at': post['created_at'],
                          'updated_at': post.get('updated_at'),
                          'reply_to_post_number': post.get('reply_to_post_number'),
                          'url': f'{spec["topic_url"]}/{post["post_number"]}',
                          **_content(parser, text, max_chars, focus)})
        return {'topic_id': value['id'], 'title': value['title'], 'posts': posts,
                'posts_count': value['posts_count'], 'supplied_posts': len(supplied),
                'stream_ids': value['post_stream'].get('stream', []),
                'scope': 'public topic JSON posts supplied in this response; no automatic comment pagination'}
    if kind == 'ofr':
        reader = csv.DictReader(io.StringIO(payload.decode('utf-8-sig')))
        if not reader.fieldnames or reader.fieldnames[:2] != ['Date', 'OFR FSI']:
            raise ValueError('OFR CSV identity')
        rows = []
        for row in reader:
            observed = date.fromisoformat(row['Date']).isoformat()
            if spec['start'] <= observed <= spec['end']:
                values = [None if row[key] in ('', '.') else float(row[key]) for key in reader.fieldnames[1:]]
                if any(v is not None and not math.isfinite(v) for v in values):
                    raise ValueError('OFR nonfinite value')
                rows.append([observed, *values])
        return {'columns': reader.fieldnames, 'rows': rows, 'units': 'index and contributions, not probabilities',
                'scope': 'retrieval-time revision; observed dates; typically published two business days later'}
    value = json.loads(payload)
    if kind == 'treasury':
        value['data'], value['meta']  # Source-native labels and units accompany values.
        return {**value, 'scope': 'one page; inspect meta total-pages and links before claiming coverage'}
    if kind == 'nyfed':
        value['repo']['operations']
        return {**value, 'scope': 'latest two weeks of operations; source dates/times and units retained'}
    if kind == 'bls':
        if value['status'] != 'REQUEST_SUCCEEDED':
            raise ValueError(f'BLS rejected request: {value.get("message")}')
        series = value['Results']['series']
        if [s['seriesID'] for s in series] != [spec['series_id']] or not series[0]['data']:
            raise ValueError('BLS requested series unavailable')
        return {**value, 'scope': 'requested whole calendar years; native periods/footnotes, not daily releases'}
    if int(value['cik']) != spec['cik']:
        raise ValueError('SEC CIK mismatch')
    if kind == 'sec-filings':
        value['filings']['recent']
        scope = 'recent filing index, not filings body or full history; older files are references only'
    else:
        if value['taxonomy'] != 'us-gaap' or value['tag'] != spec['concept']:
            raise ValueError('SEC concept mismatch')
        value['units']
        scope = 'one concept; keep units, periods, filed dates and accession identities; no restatement merge'
    return {**value, 'scope': scope}


def fetch_sources(root, requests, *, seconds=120, max_response_bytes=2*1024**2,
                  retry_failed=False, transport=None,
                  sec_user_agent='MshareSift/0.1 research'):
    """Retain admitted source text once; choose reading views independently."""
    if not requests or min(seconds, max_response_bytes) <= 0:
        raise ValueError('select sources and positive acquisition bounds')
    contract = {'requests': requests}
    path = Path(root)/'sources.json'
    if path.exists():
        pack = json.loads(path.read_bytes())
        if pack['contract']['requests'] != requests:
            raise ValueError('public snapshot contract mismatch; use a new root')
    else:
        pack = {'contract': contract, 'created_at': datetime.now(UTC).isoformat(), 'sources': {}}
    pending = [r for r in requests if r['name'] not in pack['sources'] or
               (retry_failed and pack['sources'][r['name']]['status'] != 'ok')]
    count, received = 0, 0
    deadline = time.monotonic()+seconds
    last_sec_request = -math.inf
    if pending:
        path.parent.mkdir(parents=True, exist_ok=True)
        with httpx.Client(transport=transport, follow_redirects=True) as client:
            for spec in pending:
                entry = {'url': spec['url'], 'category': spec['category'], 'kind': spec['kind']}
                try:
                    headers = {}
                    if urlsplit(spec['url']).hostname == 'data.sec.gov':
                        time.sleep(max(0, .11-(time.monotonic()-last_sec_request)))
                        headers['User-Agent'] = sec_user_agent
                        last_sec_request = time.monotonic()
                    remaining = deadline-time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('public acquisition budget')
                    count += 1
                    with client.stream('GET', spec['url'], headers=headers, timeout=min(15, remaining)) as response:
                        entry['http_status'] = response.status_code
                        response.raise_for_status()
                        if response.status_code != 200:
                            raise ValueError(f'source representation not ready: HTTP {response.status_code}')
                        content = bytearray()
                        for chunk in response.iter_bytes():
                            received += len(chunk)
                            content.extend(chunk)
                            if len(content) > max_response_bytes:
                                raise ValueError('public response byte budget')
                            if time.monotonic() >= deadline:
                                raise TimeoutError('public acquisition budget')
                        payload = bytes(content)
                        final_spec = {**spec, 'url': str(response.url)}
                    entry.update(status='ok', retrieved_at=datetime.now(UTC).isoformat(),
                                 final_url=final_spec['url'], received_bytes=len(payload),
                                 data=_read_source(payload, final_spec))
                except (httpx.HTTPError, ValueError, KeyError, TypeError, csv.Error, ET.ParseError, TimeoutError) as exc:
                    entry.update(status='failed', attempted_at=datetime.now(UTC).isoformat(),
                                 error=f'{type(exc).__name__}: {exc}')
                pack['sources'][spec['name']] = entry
        pack['status'] = 'complete' if all(s['status'] == 'ok' for s in pack['sources'].values()) else 'partial'
        atomic_write(path, json.dumps(pack, ensure_ascii=False, allow_nan=False).encode())
    return {**pack, 'requests_this_call': count, 'received_bytes_this_call': received}


def summarize_sources(pack):
    sources = {}
    for name, entry in pack['sources'].items():
        summary = {k: v for k, v in entry.items() if k != 'data'}
        if entry['status'] == 'ok':
            summary['scope'] = entry['data']['scope']
            if entry['kind'] == 'feed':
                items = entry['data']['items']
                published = [item['published_at'] for item in items if item['published_at']]
                summary.update(items=len(items), truncated_items=sum(item['truncated'] for item in items),
                               latest_retained_publication=max(published) if published else None)
        sources[name] = summary
    return {'status': pack['status'], 'requests': pack['requests_this_call'],
            'bytes': pack['received_bytes_this_call'], 'sources': sources}
