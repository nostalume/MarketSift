"""On-demand public data tools for market research."""
import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
from msharesift import market as market_history
from msharesift.model import Unavailable


def market_data_command(args):
    root = args.root
    if root.anchor or '..' in root.parts:
        raise Unavailable('--root must stay project-relative, without ..')
    if args.market_action == 'fetch':
        result = market_history.fetch_market_history(
            root, inst_ids=args.inst_id, per_side=args.per_side,
            target_days=args.target_days, max_requests=args.max_requests,
            seconds=args.seconds, end_ms=args.end_ms, datasets=args.datasets)
    elif args.market_action == 'bulk-fetch':
        result = market_history.bulk_fetch(
            root, inst_id=args.inst_id, dataset=args.dataset, begin=args.begin,
            end=args.end, date_aggregation=args.date_aggregation,
            max_archives=args.max_archives)
    elif args.market_action == 'bulk-verify':
        result = market_history.verify_bulk(root)
    elif args.market_action == 'oi-snapshot':
        result = market_history.collect_oi_snapshot(
            root, inst_ids=args.inst_id, per_side=args.per_side,
            max_requests=args.max_requests, seconds=args.seconds)
    elif args.market_action == 'oi-report':
        result = market_history.oi_report(root)
    else:
        result = market_history.verify(root)
    print(json.dumps(result, sort_keys=True, allow_nan=False))

def candle_data_command(args):
    from msharesift.candles.acquire import acquire
    from msharesift.candles.history import load_histories, normalize_dataset

    if args.root.anchor or '..' in args.root.parts:
        raise Unavailable('--root must stay project-relative, without ..')
    if args.candle_action == 'plan-history':
        from msharesift.candles.plan import full_history_plan
        for path in [args.inventory, *args.reuse_root]:
            if path.anchor or '..' in path.parts:
                raise Unavailable('inventory/reuse roots must stay project-relative')
        result = full_history_plan(args.root, args.inventory, reuse_roots=args.reuse_root,
                                   max_requests=args.max_requests, seconds=args.seconds,
                                   max_bytes=args.max_bytes, workers=args.workers,
                                   sample_requests=args.sample_requests, sample_seed=args.sample_seed)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return
    if args.candle_action == 'session':
        from msharesift.candles.session import acquire_session
        if args.plan.anchor or '..' in args.plan.parts:
            raise Unavailable('--plan must stay project-relative, without ..')
        result = acquire_session(args.root, args.plan)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        if result['status'] != 'complete':
            raise Unavailable('candle_session_partial: inspect persisted global budget and member statuses')
        return
    if args.candle_action == 'products':
        from msharesift.products import (
            catalog_snapshot,
            prepare_catalog,
            summarize_catalog,
        )
        catalog = prepare_catalog(catalog_snapshot(args.root, max_requests=args.max_requests, seconds=args.seconds))
        result = summarize_catalog(catalog, args.inst_id)
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        if result['status'] != 'complete':
            raise Unavailable('research_catalog_partial: inspect failures in catalog.json')
        return
    if args.candle_action == 'inventory':
        from msharesift.candles.universe import inventory
        result = inventory(args.root, end=args.end, max_requests=args.max_requests, seconds=args.seconds)
    elif args.candle_action == 'probe':
        from msharesift.candles.universe import probe
        if args.inventory.anchor or '..' in args.inventory.parts:
            raise Unavailable('--inventory must stay project-relative, without ..')
        result = probe(args.root, args.inventory, days=args.days, max_requests=args.max_requests, seconds=args.seconds, workers=args.workers)
        if not result['probe_complete']:
            print(json.dumps(result, sort_keys=True, allow_nan=False))
            raise Unavailable('availability_probe_partial: retain every catalog member; inspect statuses')
    elif args.candle_action == 'fetch':
        catalog = None
        if args.catalog is not None:
            from msharesift.products import prepare_catalog
            catalog = prepare_catalog(json.loads(args.catalog.read_bytes()))
        result = acquire(args.root, args.inst_id, args.grain, args.start, args.end,
                         max_requests=args.max_requests, seconds=args.seconds, universe=args.universe, workers=args.workers,
                         catalog=catalog)
        if result['complete'] != result['products']:
            print(json.dumps(result, sort_keys=True, allow_nan=False))
            raise Unavailable('candle_history_partial: inspect manifest; no history was fabricated')
    elif args.candle_action == 'normalize':
        if args.source.anchor or '..' in args.source.parts:
            raise Unavailable('--source must stay project-relative, without ..')
        result = normalize_dataset(args.source, args.root)
    else:
        histories, audit = load_histories(args.root)
        result = {'products': len(histories), 'audit': audit}
    print(json.dumps(result, sort_keys=True, allow_nan=False))

def utc_ms(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError('explicit UTC offset required')
    return int(parsed.timestamp() * 1000)

def macro_data_command(args):
    from msharesift.macro import fetch_macro, summarize_macro

    if args.root.anchor or '..' in args.root.parts:
        raise Unavailable('--root must stay project-relative, without ..')
    pack = fetch_macro(args.root, args.series, args.start, args.end,
                       policy=args.policy, seconds=args.seconds)
    print(json.dumps({'snapshot': str(args.root / 'macro.json'), 'status': pack['status'],
                      'requests': pack['requests_this_call'], 'bytes': pack['received_bytes_this_call'],
                      'series': summarize_macro(pack), 'policy': pack.get('policy')},
                     ensure_ascii=False, allow_nan=False))
    if pack['status'] != 'complete':
        raise Unavailable('macro_data_partial: successful sources retained; repeat to retry failures')

def public_data_command(args):
    from msharesift.sources import (
        fetch_sources,
        select_sources,
        source_requests,
        summarize_sources,
    )

    if args.root.anchor or '..' in args.root.parts:
        raise Unavailable('--root must stay project-relative, without ..')
    if args.public_action == 'read':
        pack = json.loads((args.root / 'sources.json').read_bytes())
        view = select_sources(pack, sources=args.source, urls=args.url, authors=args.author,
                              topics=args.topic, focus=args.focus, limit=args.limit,
                              max_chars=args.max_chars, offset=args.offset, catalog=args.catalog)
        print(json.dumps(view, ensure_ascii=False, allow_nan=False))
        return
    requests = source_requests(args.source, start=args.start, end=args.end, cik=args.cik,
                               concept=args.concept, bls_series=args.bls_series,
                               feeds=args.feed, pages=args.page)
    pack = fetch_sources(args.root, requests, seconds=args.seconds, retry_failed=args.retry_failed,
                         sec_user_agent=args.sec_user_agent)
    print(json.dumps({'snapshot': str(args.root / 'sources.json'), **summarize_sources(pack)},
                     ensure_ascii=False, allow_nan=False))
    if pack['status'] != 'complete':
        raise Unavailable('public_data_partial: inspect failures; --retry-failed retries only failures')

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    macro = commands.add_parser('macro-data', help='On-demand public macro series and policy-release index')
    macro_commands = macro.add_subparsers(dest='macro_action', required=True)
    macro_fetch = macro_commands.add_parser('fetch')
    macro_fetch.add_argument('--root', type=Path, required=True)
    macro_fetch.add_argument('--series', nargs='+', default=[])
    macro_fetch.add_argument('--policy', action='store_true')
    macro_fetch.add_argument('--start', type=date.fromisoformat, required=True)
    macro_fetch.add_argument('--end', type=date.fromisoformat, required=True)
    macro_fetch.add_argument('--seconds', type=float, default=90)
    public = commands.add_parser('public-data', help='Selected official data, RSS/Atom and public article text')
    public_commands = public.add_subparsers(dest='public_action', required=True)
    public_fetch = public_commands.add_parser('fetch')
    public_fetch.add_argument('--root', type=Path, required=True)
    public_fetch.add_argument('--source', nargs='+', default=[])
    public_fetch.add_argument('--feed', action='append', default=[], help='Additional public RSS/Atom URL')
    public_fetch.add_argument('--page', action='append', default=[], help='Selected public article/IR/announcement URL')
    public_fetch.add_argument('--start', type=date.fromisoformat)
    public_fetch.add_argument('--end', type=date.fromisoformat)
    public_fetch.add_argument('--cik')
    public_fetch.add_argument('--concept', default='Revenues', help='One us-gaap concept, not all company facts')
    public_fetch.add_argument('--bls-series', default='LNS14000000')
    public_fetch.add_argument('--sec-user-agent', default='MshareSift/0.1 research')
    public_fetch.add_argument('--seconds', type=float, default=120)
    public_fetch.add_argument('--retry-failed', action='store_true')
    public_read = public_commands.add_parser('read', help='Read stored material offline; leaves the snapshot unchanged')
    public_read.add_argument('--root', type=Path, required=True)
    public_read.add_argument('--source', nargs='+', default=[])
    public_read.add_argument('--url', action='append', default=[], help='Exact stored article or post URL')
    public_read.add_argument('--author', action='append', default=[], help='Exact author, case-insensitive')
    public_read.add_argument('--topic', action='append', default=[], help='Literal word/phrase in title or retained text')
    public_read.add_argument('--focus', action='append', default=[], help='Read around a phrase in stored text')
    public_read.add_argument('--limit', type=int, help='Matched items/posts per source; default all')
    public_read.add_argument('--max-chars', type=int, default=12000, help='Delivered characters per item/page')
    public_read.add_argument('--offset', type=int, default=0, help='Stored text character offset when not using --focus')
    public_read.add_argument('--catalog', action='store_true', help='Only identities, dates, scope and content sizes')
    market = commands.add_parser('market-data', help='Fetch or verify canonical public market-history datasets')
    market_commands = market.add_subparsers(dest='market_action', required=True)
    market_fetch = market_commands.add_parser('fetch')
    market_fetch.add_argument('--root', type=Path, default=Path('.data/market'))
    market_fetch.add_argument('--inst-id', action='append', default=None,
                              help='Instrument ID; repeat to request specific products')
    market_fetch.add_argument('--per-side', type=int, default=1, choices=range(1, 101), metavar='1..100')
    market_fetch.add_argument('--target-days', type=int, default=1, choices=range(1, 91), metavar='1..90')
    market_fetch.add_argument('--max-requests', type=int, default=2400, choices=range(1, 2401), metavar='1..2400')
    market_fetch.add_argument('--seconds', type=int, default=480, choices=range(1, 481), metavar='1..480')
    market_fetch.add_argument('--end-ms', type=int, default=None,
                              help='Optional historical UTC end timestamp in milliseconds')
    market_fetch.add_argument('--datasets', nargs='+', choices=market_history.DATASETS,
                              default=list(market_history.DATASETS))
    market_verify = market_commands.add_parser('verify')
    market_verify.add_argument('--root', type=Path, default=Path('.data/market'))
    bulk_fetch = market_commands.add_parser('bulk-fetch', help='Fetch official OKX bulk trade/funding archives')
    bulk_fetch.add_argument('--root', type=Path, default=Path('.data/market-bulk'))
    bulk_fetch.add_argument('--inst-id', required=True)
    bulk_fetch.add_argument('--dataset', choices=('trades', 'funding'), required=True)
    bulk_fetch.add_argument('--begin', required=True, help='Inclusive UTC date YYYY-MM-DD')
    bulk_fetch.add_argument('--end', required=True, help='Exclusive UTC date YYYY-MM-DD')
    bulk_fetch.add_argument('--date-aggregation', choices=('daily', 'monthly'), default=None)
    bulk_fetch.add_argument('--max-archives', type=int, default=31, choices=range(1, 101), metavar='1..100')
    bulk_verify = market_commands.add_parser('bulk-verify')
    bulk_verify.add_argument('--root', type=Path, default=Path('.data/market-bulk'))
    oi_snapshot = market_commands.add_parser('oi-snapshot', help='Capture one canonical open-interest snapshot')
    oi_snapshot.add_argument('--root', type=Path, default=Path('.data/open-interest'))
    oi_snapshot.add_argument('--inst-id', action='append', default=None)
    oi_snapshot.add_argument('--per-side', type=int, default=1, choices=range(1, 101), metavar='1..100')
    oi_snapshot.add_argument('--max-requests', type=int, default=2400, choices=range(1, 2401), metavar='1..2400')
    oi_snapshot.add_argument('--seconds', type=int, default=120, choices=range(1, 481), metavar='1..480')
    oi_report = market_commands.add_parser('oi-report', help='Verify and report accumulated OI snapshots')
    oi_report.add_argument('--root', type=Path, default=Path('.data/open-interest'))
    candle = commands.add_parser('candle-data', help='Mainline candle acquisition, cleaning and audit')
    candle_commands = candle.add_subparsers(dest='candle_action', required=True)
    candle_fetch = candle_commands.add_parser('fetch')
    candle_session = candle_commands.add_parser('session', help='Fetch fixed batches under one persisted total budget; explicit full-history profile supported')
    candle_plan = candle_commands.add_parser('plan-history', help='Freeze every current nativeUSDT product since normal listing;1h source only, reuse canonical data')
    candle_plan.add_argument('--root', type=Path, required=True)
    candle_plan.add_argument('--inventory', type=Path, required=True)
    candle_plan.add_argument('--reuse-root', type=Path, action='append', default=[])
    candle_plan.add_argument('--max-requests', type=int, default=80000)
    candle_plan.add_argument('--seconds', type=int, default=21600)
    candle_plan.add_argument('--max-bytes', type=int, default=4 * 1024**3)
    candle_plan.add_argument('--workers', type=int, choices=(1, 4), default=4)
    candle_plan.add_argument('--sample-requests', type=int, default=None, help='Optional fixed-seed stratified base sample under a request envelope; requires <=1200 seconds')
    candle_plan.add_argument('--sample-seed', default='20261021-20min')
    candle_session.add_argument('--root', type=Path, required=True)
    candle_session.add_argument('--plan', type=Path, required=True)
    candle_fetch.add_argument('--root', type=Path, default=Path('.data/candles'))
    candle_fetch.add_argument('--inst-id', action='append', required=True)
    candle_fetch.add_argument('--universe', choices=('alert', 'research'), default='alert',
                              help='research admits supported OKX asset/contract classes; alert preserves production exclusions')
    candle_fetch.add_argument('--catalog', type=Path, default=None,
                              help='Reuse one public catalog.json (research only)')
    candle_fetch.add_argument('--workers', type=int, choices=(1, 4), default=1)
    candle_fetch.add_argument('--grain', choices=('1m', '5m', '1h'), default='5m')
    candle_fetch.add_argument('--start', type=utc_ms, required=True)
    candle_fetch.add_argument('--end', type=utc_ms, required=True)
    candle_fetch.add_argument('--max-requests', type=int, default=600)
    candle_fetch.add_argument('--seconds', type=int, default=480)
    candle_products = candle_commands.add_parser('products', help='Current OKX research product rules; cached catalog, no capacity scan')
    candle_products.add_argument('--root', type=Path, required=True)
    candle_products.add_argument('--inst-id', action='append', default=[])
    candle_products.add_argument('--max-requests', type=int, default=12)
    candle_products.add_argument('--seconds', type=int, default=90)
    candle_inventory = candle_commands.add_parser('inventory', help='Unranked current USDT crypto catalog and capacity estimates')
    candle_inventory.add_argument('--root', type=Path, required=True)
    candle_inventory.add_argument('--end', type=utc_ms, default=None)
    candle_inventory.add_argument('--max-requests', type=int, default=20)
    candle_inventory.add_argument('--seconds', type=int, default=90)
    candle_probe = candle_commands.add_parser('probe', help='Two boundary pages per catalog member; no full download')
    candle_probe.add_argument('--root', type=Path, required=True)
    candle_probe.add_argument('--inventory', type=Path, required=True)
    candle_probe.add_argument('--days', type=int, default=1825)
    candle_probe.add_argument('--workers', type=int, choices=(1, 2, 4), default=4)
    candle_probe.add_argument('--max-requests', type=int, default=1400)
    candle_probe.add_argument('--seconds', type=int, default=480)
    candle_audit = candle_commands.add_parser('audit')
    candle_audit.add_argument('--root', type=Path, default=Path('.data/candles'))
    candle_normalize = candle_commands.add_parser('normalize')
    candle_normalize.add_argument('--root', type=Path, required=True)
    candle_normalize.add_argument('--source', type=Path, required=True)
    args = parser.parse_args(argv)
    os.chdir(ROOT)
    try:
        handlers = {'market-data': market_data_command, 'candle-data': candle_data_command,
                    'macro-data': macro_data_command, 'public-data': public_data_command}
        handlers[args.command](args)
        return 0
    except KeyboardInterrupt:
        print('Cancelled; completed data retained.', file=sys.stderr)
        return 130
    except (OSError, ValueError) as exc:
        print(f'Data operation stopped: {exc}', file=sys.stderr)
        return 2

if __name__ == '__main__':
    raise SystemExit(main())
