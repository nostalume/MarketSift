"""Source scope, native disclosure semantics and explicit acquisition reuse."""
import json
from datetime import date

import httpx
import pytest

from msharesift import cli as alert
from msharesift import sources as public

START, END = date(2026, 10, 1), date(2026, 10, 31)
RSS = b'''<rss><channel><item><title>A</title><link>https://example.org/a</link>
<pubDate>Wed, 28 Oct 2026 14:00:00 -0400</pubDate><description>&lt;p&gt;Summary&lt;/p&gt;</description>
</item><item><title>Duplicate</title><link>https://example.org/a</link></item>
<item><title>B</title><link>/b</link></item></channel></rss>'''


def test_rss_keeps_publication_content_scope_and_deduplicates_links():
    data = public.read_feed(RSS,'https://example.org/feed',10,100)
    assert len(data['items']) == 2 and data['feed_entries'] == 3
    item = data['items'][0]
    assert item['published_at'] == '2026-10-28T18:00:00+00:00'
    assert item['text'] == 'Summary' and item['scope'] == 'feed_summary'
    assert data['items'][1]['url'] == 'https://example.org/b'
    assert data['items'][1]['published_at'] is None


@pytest.mark.parametrize('content',['','<p></p>'])
def test_empty_encoded_content_does_not_hide_available_summary(content):
    body=f'''<rss xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><item>
    <title>A</title><link>https://example.org/a</link><content:encoded><![CDATA[{content}]]></content:encoded>
    <description>Actual summary</description></item></channel></rss>'''.encode()
    item=public.read_feed(body,'https://example.org/feed',1,100)['items'][0]
    assert item['text'] == 'Actual summary' and item['scope'] == 'feed_summary'


def test_selected_forum_topic_has_post_identity_and_visible_coverage(tmp_path):
    specs=public.source_requests([],pages=['https://gov.uniswap.org/t/topic/26287'])
    payload={'id':26287,'title':'Proposal','posts_count':20,
             'post_stream':{'stream':[1,2,3], 'posts':[
                 {'id':1,'post_number':1,'username':'Author','created_at':'2026-09-18T00:00:00Z',
                  'cooked':'<p>Proposed</p><a href="https://example.org/vote">Vote</a>'},
                 {'id':2,'post_number':2,'username':'Reviewer','created_at':'2026-09-19T00:00:00Z',
                  'cooked':'<p>Not yet executed</p>'}]}}
    def handle(request):
        assert str(request.url) == 'https://gov.uniswap.org/t/26287.json'
        return httpx.Response(200,json=payload)
    pack=public.fetch_sources(tmp_path,specs,transport=httpx.MockTransport(handle))
    assert len(next(iter(pack['sources'].values()))['data']['posts']) == 2
    data=next(iter(public.select_sources(pack,limit=1)['sources'].values()))['data']
    assert data['posts_count'] == 20 and data['supplied_posts'] == 2 and len(data['posts']) == 1
    assert data['posts'][0]['url'] == 'https://gov.uniswap.org/t/26287/1'
    assert data['posts'][0]['text'] == 'Proposed\n[Vote](<https://example.org/vote>)'
    assert data['posts'][0]['links'] == ['https://example.org/vote']


def test_forum_html_and_json_aliases_are_one_request():
    specs=public.source_requests([],pages=['https://ethresear.ch/t/title/26192','https://ethresear.ch/t/26192.json'])
    assert len(specs) == 1


def test_accepted_interstitial_is_not_marked_as_article(tmp_path):
    specs=public.source_requests([],pages=['https://example.org/article'])
    pack=public.fetch_sources(tmp_path,specs,transport=httpx.MockTransport(
        lambda r:httpx.Response(202,text='<html><meta http-equiv="refresh"/></html>')))
    entry=next(iter(pack['sources'].values()))
    assert entry['status'] == 'failed' and entry['http_status'] == 202 and 'data' not in entry


def test_atom_updated_is_not_fabricated_publication_and_bounds_are_explicit():
    body = b'''<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Post</title>
    <link href="https://example.org/post"/><updated>2026-10-29T00:00:00Z</updated>
    <author><name>Writer</name></author><content type="html">&lt;p&gt;Long text&lt;/p&gt;</content>
    </entry></feed>'''
    item = public.read_feed(body,'https://example.org/feed',1,4)['items'][0]
    assert item['published_at'] is None and item['updated_at'] == '2026-10-29T00:00:00+00:00'
    assert item['author'] == 'Writer' and item['text'] == 'Long' and item['truncated']
    assert item['scope'] == 'feed_content'


def test_atom_xhtml_content_is_retained():
    body=b'''<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Post</title>
    <link href="https://example.org/post"/><content type="xhtml">
    <div xmlns="http://www.w3.org/1999/xhtml"><p>Actual text</p></div></content></entry></feed>'''
    assert public.read_feed(body,'https://example.org/feed',1,100)['items'][0]['text'] == 'Actual text'


@pytest.mark.parametrize('body',[b'<html/>', b'<rss/>', b'bad xml'])
def test_bad_feed_is_failed_not_no_news(tmp_path,body):
    pack=public.fetch_sources(tmp_path,public.source_requests(['coindesk']),
        transport=httpx.MockTransport(lambda r:httpx.Response(200,content=body)))
    assert pack['status'] == 'partial' and pack['sources']['coindesk']['status'] == 'failed'
    assert 'data' not in pack['sources']['coindesk']


def test_snapshot_reuse_and_failure_retry_have_one_owner(tmp_path):
    calls=[]
    def handle(request):
        calls.append(request.url.host)
        if request.url.host == 'cointelegraph.com' and calls.count('cointelegraph.com') == 1:
            return httpx.Response(403)
        return httpx.Response(200,content=RSS)
    transport=httpx.MockTransport(handle)
    specs=public.source_requests(['coindesk','cointelegraph'])
    first=public.fetch_sources(tmp_path,specs,transport=transport)
    summary=public.summarize_sources(first)['sources']['coindesk']
    assert summary['latest_retained_publication'] == '2026-10-28T18:00:00+00:00'
    successful=first['sources']['coindesk']
    cached=public.fetch_sources(tmp_path,specs,transport=transport)
    assert cached['requests_this_call'] == 0 and len(calls) == 2
    resumed=public.fetch_sources(tmp_path,specs,transport=transport,retry_failed=True)
    assert resumed['requests_this_call'] == 1 and resumed['status'] == 'complete'
    assert resumed['sources']['coindesk'] == successful


def test_request_selection_no_duplicate_macro_or_custom_url():
    specs=public.source_requests(['coindesk','coindesk'],feeds=[public.FEEDS['coindesk'][1]])
    assert len(specs) == 1
    assert not any('fred' in r['url'] for r in specs)


def test_treasury_keeps_source_units_account_types_and_pagination(tmp_path):
    payload={'data':[{'record_date':'2026-10-01','account_type':'TGA Closing','open_today_bal':'100'}],
             'meta':{'dataFormats':{'open_today_bal':'$1,000,000'},'total-pages':2},'links':{'next':'?page=2'}}
    pack=public.fetch_sources(tmp_path,public.source_requests(['treasury'],start=START,end=END),
        transport=httpx.MockTransport(lambda r:httpx.Response(200,json=payload)))
    data=pack['sources']['treasury']['data']
    assert data['data'] == payload['data'] and data['meta'] == payload['meta']
    assert data['links']['next'] == '?page=2' and 'one page' in data['scope']


def test_ofr_window_and_missing_contributions_are_not_zero(tmp_path):
    body='Date,OFR FSI,Credit\n2026-09-30,1,2\n2026-10-01,-1,.\n2026-10-28,-2,-3\n'
    pack=public.fetch_sources(tmp_path,public.source_requests(['ofr'],start=START,end=END),
        transport=httpx.MockTransport(lambda r:httpx.Response(200,text=body)))
    assert pack['sources']['ofr']['data']['rows'] == [['2026-10-01',-1,None],['2026-10-28',-2,-3]]


@pytest.mark.parametrize('payload',[
    {'status':'REQUEST_NOT_PROCESSED','message':['quota']},
    {'status':'REQUEST_SUCCEEDED','Results':{'series':[{'seriesID':'OTHER','data':[{}]}]}},
])
def test_bls_quota_or_wrong_identity_does_not_become_usable_data(tmp_path,payload):
    pack=public.fetch_sources(tmp_path,public.source_requests(['bls'],start=START,end=END),
        transport=httpx.MockTransport(lambda r:httpx.Response(200,json=payload)))
    assert pack['sources']['bls']['status'] == 'failed'


def test_sec_keeps_units_amendments_and_periods_without_merging(tmp_path):
    payload={'cik':1045810,'taxonomy':'us-gaap','tag':'Revenues','units':{'USD':[
        {'start':'2026-01-01','end':'2026-03-31','val':100,'filed':'2026-05-01','accn':'A'},
        {'start':'2026-01-01','end':'2026-03-31','val':101,'filed':'2026-06-01','accn':'B'}]}}
    def handle(r):
        assert r.headers['User-Agent'] == 'declared research tool'
        return httpx.Response(200,json=payload)
    specs=public.source_requests(['sec-concept'],cik='1045810')
    pack=public.fetch_sources(tmp_path,specs,sec_user_agent='declared research tool',transport=httpx.MockTransport(handle))
    assert pack['sources']['sec-concept']['data']['units'] == payload['units']


def test_page_text_ignores_scripts_preserves_article_scope(tmp_path):
    body='''<html><head><title>Title</title><meta property="article:published_time" content="2026-10-28"/></head>
    <body>Navigation<script>fake news</script><article><p>Actual evidence</p></article></body></html>'''
    specs=public.source_requests([],pages=['https://example.org/ir'])
    pack=public.fetch_sources(tmp_path,specs,transport=httpx.MockTransport(lambda r:httpx.Response(200,text=body)))
    data=next(iter(pack['sources'].values()))['data']
    assert data['text'] == 'Actual evidence' and data['scope'] == 'article/main text'
    assert data['metadata']['article:published_time'] == '2026-10-28'


def test_contract_change_does_not_fetch_or_overwrite(tmp_path):
    specs=public.source_requests(['coindesk'])
    transport=httpx.MockTransport(lambda r:httpx.Response(200,content=RSS))
    public.fetch_sources(tmp_path,specs,transport=transport)
    saved=(tmp_path/'sources.json').read_bytes()
    with pytest.raises(ValueError,match='contract mismatch'):
        public.fetch_sources(tmp_path,public.source_requests(['hayes']),
            transport=httpx.MockTransport(lambda r:pytest.fail('must not fetch')))
    assert (tmp_path/'sources.json').read_bytes() == saved


def test_response_limit_preserves_failure_instead_of_partial_text(tmp_path):
    pack=public.fetch_sources(tmp_path,public.source_requests(['coindesk']),max_response_bytes=8,
        transport=httpx.MockTransport(lambda r:httpx.Response(200,content=RSS)))
    assert 'byte budget' in pack['sources']['coindesk']['error']


def test_cli_public_sources_do_not_scan_or_trade(tmp_path,monkeypatch,capsys):
    monkeypatch.setattr(alert,'ROOT',tmp_path)
    monkeypatch.chdir(tmp_path)
    def fetch(root,specs,**kwargs):
        assert root.as_posix() == '.data/text' and specs[0]['name'] == 'coindesk'
        return {'status':'complete','sources':{},'requests_this_call':0,'received_bytes_this_call':0}
    monkeypatch.setattr(public,'fetch_sources',fetch)
    assert alert.main(['public-data','fetch','--root','.data/text','--source','coindesk']) == 0
    assert json.loads(capsys.readouterr().out)['requests'] == 0


def test_evidence_structure_keeps_table_spans_quotes_and_chart_reference(tmp_path):
    body='''<article><blockquote><p>Quoted claim</p></blockquote><p>Author disagrees.</p>
    <table><caption>Flows, USD million</caption><tr><th>Date</th><th>Flow</th></tr>
    <tr><td rowspan="2">Oct 30</td><td>-643.2</td></tr><tr><td>not final</td></tr></table>
    <figure><a href="/chart-original.png"><img src="/chart-small.png" alt="Credit growth"
    srcset="/chart-large.png 2x"/></a><figcaption>Observed credit, not a forecast.</figcaption></figure></article>'''
    pack=public.fetch_sources(tmp_path,public.source_requests([],pages=['https://example.org/research']),
        transport=httpx.MockTransport(lambda r:httpx.Response(200,text=body)))
    data=next(iter(pack['sources'].values()))['data']
    assert '<tr><th>Date</th><th>Flow</th></tr>' in data['text']
    assert '<td rowspan="2">Oct 30</td><td>-643.2</td>' in data['text']
    assert '<blockquote>\nQuoted claim\n</blockquote>' in data['text']
    assert 'Observed credit, not a forecast.' in data['text']
    assert data['images'][0]['url'] == 'https://example.org/chart-small.png'
    assert data['images'][0]['link_url'] == 'https://example.org/chart-original.png'
    assert data['images'][0]['alt'] == 'Credit growth'
    assert data['images'][0]['srcset'] == '/chart-large.png 2x'


def test_forum_post_limit_does_not_cap_evidence_links_and_reply_parent(tmp_path):
    links=''.join(f'<a href="https://example.org/{i}">evidence {i}</a>' for i in range(15))
    payload={'id':26287,'title':'Discussion','posts_count':2,'post_stream':{'posts':[
        {'id':2,'post_number':2,'username':'Reviewer','created_at':'2026-10-01T00:00:00Z',
         'reply_to_post_number':1,'cooked':links}]}}
    pack=public.fetch_sources(tmp_path,public.source_requests([],pages=['https://gov.uniswap.org/t/26287']),
        transport=httpx.MockTransport(lambda r:httpx.Response(200,json=payload)))
    post=next(iter(pack['sources'].values()))['data']['posts'][0]
    assert len(post['links']) == 15 and post['reply_to_post_number'] == 1


def test_author_and_topic_selection_precede_limit_and_text_crop():
    body='''<rss xmlns:dc="http://purl.org/dc/elements/1.1/"><channel>
    <item><title>AI news</title><link>https://example.org/news</link><dc:creator>Newsdesk</dc:creator><description>AI</description></item>
    <item><title>Email</title><link>https://example.org/mail</link><dc:creator>Marty Bent</dc:creator><description>Email update</description></item>
    <item><title>Research</title><link>https://example.org/selected</link><dc:creator>Marty Bent</dc:creator>
    <description>Opening paragraph. Eventually AI capital expenditure matters.</description></item>
    </channel></rss>'''.encode()
    result=public.read_feed(body,'https://example.org/feed',1,8,authors=['marty bent'],topics=['AI'])
    assert result['items'][0]['url'] == 'https://example.org/selected'
    assert result['examined_entries'] == 3 and result['items'][0]['truncated']


def test_focus_reads_decisive_tail_from_admitted_content_with_no_second_fetch(tmp_path):
    text='Opening. '+('background '*80)+'Falsification: credit shrinks, thesis fails. '+('ending '*20)
    xml=f'<rss><channel><item><title>Thesis</title><link>https://example.org/thesis</link><description>{text}</description></item></channel></rss>'
    calls=[]
    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(200,text=xml)
    specs=public.source_requests([],feeds=['https://example.org/feed'])
    pack=public.fetch_sources(tmp_path,specs,transport=httpx.MockTransport(handle))
    saved=(tmp_path/'sources.json').read_bytes()
    first=public.select_sources(pack,max_chars=160)
    assert 'Falsification' not in next(iter(first['sources'].values()))['data']['items'][0]['text']
    focused=public.select_sources(pack,focus=['Falsification'],max_chars=160)
    item=next(iter(focused['sources'].values()))['data']['items'][0]
    assert 'credit shrinks, thesis fails' in item['text'] and item['text_ranges'][0][0] > 0
    assert item['text_selection'] == 'focus passages' and item['truncated'] and len(calls) == 1
    reused=public.fetch_sources(tmp_path,specs,transport=httpx.MockTransport(handle))
    assert reused['requests_this_call'] == 0 and len(calls) == 1
    assert (tmp_path/'sources.json').read_bytes() == saved


def test_missing_focus_does_not_claim_passage_was_found():
    result=public.read_feed(RSS,'https://example.org/feed',1,100,focus=['not present'])
    assert result['items'][0]['focus_not_found'] == ['not present']
    assert result['items'][0]['text_selection'] == 'head'


def test_reading_selection_does_not_change_snapshot_identity(tmp_path):
    specs=public.source_requests(['coindesk'])
    transport=httpx.MockTransport(lambda r:httpx.Response(200,content=RSS))
    pack=public.fetch_sources(tmp_path,specs,transport=transport)
    before=(tmp_path/'sources.json').read_bytes()
    first=public.select_sources(pack,topics=['A'])
    second=public.select_sources(pack,topics=['B'])
    assert first['sources']['coindesk']['data']['items'][0]['title'] == 'A'
    assert second['sources']['coindesk']['data']['items'][0]['title'] == 'B'
    assert (tmp_path/'sources.json').read_bytes() == before


def test_atom_xhtml_table_preserves_columns():
    body=b'''<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Data</title>
    <link href="https://example.org/post"/><content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml">
    <table><tr><td>Reserves</td><td>100</td></tr></table></div></content></entry></feed>'''
    item=public.read_feed(body,'https://example.org/feed',1,1000)['items'][0]
    assert '<tr><td>Reserves</td><td>100</td></tr>' in item['text']


def test_discovery_retains_items_after_ten_anonymous_authors_and_full_tail(tmp_path):
    body='<rss><channel>'+''.join(f'<item><title>Item {i}</title><link>https://example.org/{i}</link>'
        '<description>'+('background '*2000)+f'Condition {i}</description></item>' for i in range(15))+'</channel></rss>'
    pack=public.fetch_sources(tmp_path,public.source_requests(['coinmetrics']),
                             transport=httpx.MockTransport(lambda r:httpx.Response(200,text=body)))
    items=pack['sources']['coinmetrics']['data']['items']
    assert len(items) == 15 and all(i['author'] is None for i in items)
    assert items[-1]['text'].endswith('Condition 14') and not items[-1]['truncated']
    catalog=public.select_sources(pack,catalog=True)['sources']['coinmetrics']['data']['items']
    assert len(catalog) == 15 and 'text' not in catalog[-1]
    assert catalog[-1]['stored_text_characters'] > 12000


def test_old_selected_snapshot_is_reused_without_upgrade_or_claiming_missing_tail(tmp_path):
    specs=public.source_requests(['coindesk'])
    data=public.read_feed(RSS,specs[0]['url'],1,4)
    old={'contract':{'requests':specs,'item_limit':1,'text_limit':4},'status':'complete',
         'sources':{'coindesk':{'status':'ok','kind':'feed','url':specs[0]['url'],'data':data}}}
    path=tmp_path/'sources.json'
    path.write_text(json.dumps(old))
    before=path.read_bytes()
    pack=public.fetch_sources(tmp_path,specs,
        transport=httpx.MockTransport(lambda r:pytest.fail('old snapshot must not refresh')))
    item=public.select_sources(pack,focus=['missing'],max_chars=100)['sources']['coindesk']['data']['items'][0]
    assert item['source_truncated'] and item['truncated'] and item['text'] == 'Summ'
    assert item['source_text_ranges'] == [[0,4]] and item['range_basis'] == 'stored text'
    assert item['focus_not_found'] == ['missing'] and path.read_bytes() == before


def test_cli_read_is_offline_does_not_parse_html_or_modify_snapshot(tmp_path,monkeypatch,capsys):
    monkeypatch.setattr(alert,'ROOT',tmp_path)
    monkeypatch.chdir(tmp_path)
    public.fetch_sources('.data/text',public.source_requests(['coinmetrics']),
        transport=httpx.MockTransport(lambda r:httpx.Response(200,content=RSS)))
    path=tmp_path/'.data/text/sources.json'
    before=path.read_bytes()
    monkeypatch.setattr(httpx,'Client',lambda **kw:pytest.fail('must stay offline'))
    monkeypatch.setattr(public._Text,'feed',lambda *a:pytest.fail('must not parse stored text'))
    assert alert.main(['public-data','read','--root','.data/text','--url','https://example.org/a',
                       '--offset','3','--max-chars','3']) == 0
    result=json.loads(capsys.readouterr().out)
    item=result['sources']['coinmetrics']['data']['items'][0]
    assert item['text'] == 'mar' and item['text_ranges'] == [[3,6]]
    assert result['requests_this_call'] == 0 and path.read_bytes() == before


def test_new_analysis_feeds_and_aave_topic_use_existing_readers(tmp_path):
    assert [s['kind'] for s in public.source_requests(['coinmetrics','aave','apricitas'])] == ['feed']*3
    specs=public.source_requests([],pages=['https://governance.aave.com/t/risk/25904'])
    assert specs[0]['kind'] == 'discourse' and specs[0]['url'] == 'https://governance.aave.com/t/25904.json'
    payload={'id':25904,'title':'Risk','posts_count':0,'post_stream':{'posts':[]}}
    pack=public.fetch_sources(tmp_path,specs,transport=httpx.MockTransport(lambda r:httpx.Response(200,json=payload)))
    assert next(iter(pack['sources'].values()))['data']['topic_id'] == 25904
