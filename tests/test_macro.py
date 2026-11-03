"""Public-source meaning, missingness, snapshots and on-demand reuse."""
import json
from datetime import date

import httpx
import pytest

from msharesift import cli as alert
from msharesift import macro as macro_data

START, END = date(2025, 1, 1), date(2026, 10, 31)


def csv_response(sid, rows):
    return httpx.Response(200, text=f'observation_date,{sid}\n'+rows)


def test_public_sources_parse_once_keep_missing_and_reuse_snapshot(tmp_path):
    calls = []
    def handle(request):
        calls.append(request)
        return csv_response(request.url.params['id'], '2026-06-30,4.00\n2026-07-01,.\n2026-09-30,3.88\n')
    transport = httpx.MockTransport(handle)
    pack = macro_data.fetch_macro(tmp_path,['DFF','DFF'],START,END,transport=transport)
    assert len(calls) == 1 and pack['series']['DFF']['rows'][1][1] is None
    assert pack['contract']['revision'] == 'latest_at_retrieval'
    saved = (tmp_path/'macro.json').read_bytes()
    reused = macro_data.fetch_macro(tmp_path,['DFF'],START,END,transport=transport)
    assert reused['requests_this_call'] == 0 and len(calls) == 1
    assert (tmp_path/'macro.json').read_bytes() == saved


def test_retry_only_failed_source_and_keep_successful_revision(tmp_path):
    calls = []
    def handle(request):
        sid=request.url.params['id']
        calls.append(sid)
        if sid == 'WALCL' and calls.count(sid) == 1:
            return httpx.Response(503)
        return csv_response(sid,'2026-09-30,100\n')
    transport=httpx.MockTransport(handle)
    first=macro_data.fetch_macro(tmp_path,['DFF','WALCL'],START,END,transport=transport)
    assert first['status'] == 'partial'
    second=macro_data.fetch_macro(tmp_path,['WALCL','DFF'],START,END,transport=transport)
    assert second['status'] == 'complete' and second['requests_this_call'] == 1
    assert calls == ['DFF','WALCL','WALCL']
    assert first['series']['DFF'] == second['series']['DFF']


@pytest.mark.parametrize('body',[
    'DATE,OTHER\n2026-09-30,1\n',
    'DATE,DFF\n2026-09-30,nan\n',
    'DATE,DFF\n2026-09-30,1\n2026-09-29,2\n',
    'DATE,DFF\n2026-09-30,1\n2026-09-30,2\n',
    'DATE,DFF\n2027-01-01,1\n',
])
def test_unusable_source_stays_failed_not_empty_or_zero(tmp_path,body):
    pack=macro_data.fetch_macro(tmp_path,['DFF'],START,END,
        transport=httpx.MockTransport(lambda request:httpx.Response(200,text=body)))
    assert pack['status'] == 'partial' and pack['series']['DFF']['status'] == 'failed'
    assert 'rows' not in pack['series']['DFF']


def test_calendar_comparisons_distinguish_rates_levels_and_unavailable_history(tmp_path):
    def handle(request):
        sid=request.url.params['id']
        return csv_response(sid, '2025-09-30,100\n2026-06-30,101\n2026-09-30,103\n')
    pack=macro_data.fetch_macro(tmp_path,['DFF','CPIAUCSL'],START,END,
                               transport=httpx.MockTransport(handle))
    summary=macro_data.summarize_macro(pack)
    rate=summary['DFF']['calendar_month_comparisons']['3']
    level=summary['CPIAUCSL']['calendar_month_comparisons']['12']
    assert rate['reference_cutoff'] == '2026-06-30'
    assert rate['change'] == 2 and rate['change_unit'] == 'percentage_points'
    assert 'relative_change_pct' not in rate
    assert level['relative_change_pct'] == pytest.approx(3)
    assert summary['DFF']['calendar_month_comparisons']['36']['reference'] is None


def test_policy_publication_time_and_recent_feed_scope(tmp_path):
    xml='''<rss><channel><item><title>FOMC statement</title><link>https://example.org/policy</link>
    <pubDate>Wed, 28 Oct 2026 14:00:00 -0400</pubDate></item>
    <item><title>Later</title><link>https://example.org/later</link>
    <pubDate>Sun, 01 Nov 2026 00:00:00 GMT</pubDate></item></channel></rss>'''
    pack=macro_data.fetch_macro(tmp_path,[],START,END,policy=True,
        transport=httpx.MockTransport(lambda request:httpx.Response(200,text=xml)))
    assert len(pack['policy']['releases']) == 1
    assert pack['policy']['releases'][0]['published_at'] == '2026-10-28T18:00:00+00:00'
    assert pack['policy']['coverage_note'].startswith('Recent RSS items only')


def test_snapshot_contract_changes_do_not_overwrite_or_fetch(tmp_path):
    transport=httpx.MockTransport(lambda request:csv_response('DFF','2026-09-30,3.88\n'))
    macro_data.fetch_macro(tmp_path,['DFF'],START,END,transport=transport)
    saved=(tmp_path/'macro.json').read_bytes()
    def forbidden(request):
        pytest.fail('contract mismatch must not fetch')
    with pytest.raises(ValueError,match='contract mismatch'):
        macro_data.fetch_macro(tmp_path,['DFF'],START,date(2026,10,30),
                               transport=httpx.MockTransport(forbidden))
    assert (tmp_path/'macro.json').read_bytes() == saved


def test_non_rss_response_does_not_become_no_policy_news(tmp_path):
    pack = macro_data.fetch_macro(tmp_path, [], START, END, policy=True,
        transport=httpx.MockTransport(lambda request:httpx.Response(200,text='<html/>')))
    assert pack['status'] == 'partial' and pack['policy']['status'] == 'failed'


def test_response_budget_and_unknown_series_metadata(tmp_path):
    pack=macro_data.fetch_macro(tmp_path/'limited',['DFF'],START,END,max_response_bytes=8,
        transport=httpx.MockTransport(lambda request:csv_response('DFF','2026-09-30,1\n')))
    assert 'macro_response_budget' in pack['series']['DFF']['error']
    pack=macro_data.fetch_macro(tmp_path/'other',['OTHER'],START,END,
        transport=httpx.MockTransport(lambda request:csv_response('OTHER','2026-09-30,1\n')))
    assert pack['series']['OTHER']['units'] is None and pack['series']['OTHER']['frequency'] is None


def test_cli_exposes_context_without_market_scan(tmp_path,monkeypatch,capsys):
    monkeypatch.setattr(alert,'ROOT',tmp_path)
    monkeypatch.chdir(tmp_path)
    def fetch(root,series,start,end,**kwargs):
        assert root.as_posix() == '.data/macro' and series == ['DFF']
        assert start == START and end == END and kwargs['policy']
        return {'status':'complete','series':{},'requests_this_call':0,'received_bytes_this_call':0}
    monkeypatch.setattr(macro_data,'fetch_macro',fetch)
    assert alert.main(['macro-data','fetch','--root','.data/macro','--series','DFF',
                       '--start',str(START),'--end',str(END),'--policy']) == 0
    assert json.loads(capsys.readouterr().out)['requests'] == 0
