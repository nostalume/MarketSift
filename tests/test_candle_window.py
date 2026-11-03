"""Scoped I/O and the existing integrity/union/UTC-bucket contracts."""

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pytest

from msharesift.candles.collection import load_collection
from msharesift.candles.frame import aggregate, frame_from_bars
from msharesift.candles.history import DATA_SCHEMA, History, load_histories
from msharesift.model import FIVE, Bar, Product, Series, Unavailable
from msharesift.snapshot import _save_snapshot


def dataset(root, products):
    """products: (symbol, [(start, end, omitted timestamps, close)])."""
    root.mkdir()
    entries = []
    for symbol, windows in products:
        shards = []
        for number, (start, end, omitted, price) in enumerate(windows):
            frame = frame_from_bars([
                Bar(t, price, price+1, price-1, price, 1, 1, 100)
                for t in range(start, end, FIVE) if t not in omitted
            ])
            name = f'{symbol}-{number}.parquet'
            frame.write_parquet(root / name)
            shards.append({'file':name, 'start':start, 'end':end, 'rows':frame.height,
                           'complete':not omitted,
                           'sha256':hashlib.sha256((root/name).read_bytes()).hexdigest()})
        entries.append({'product':asdict(Product('SPOT',f'{symbol}-USDT',symbol,1)),
                        'status':'partial' if any(w[2] for w in windows) else 'complete',
                        'shards':shards})
    windows = [w for _, parts in products for w in parts]
    manifest = {'schema':DATA_SCHEMA, 'contract':{'inst_ids':[e['product']['inst_id'] for e in entries],
                'grain':FIVE, 'start':min(w[0] for w in windows), 'end':max(w[1] for w in windows)},
                'products':entries}
    (root/'manifest.json').write_text(json.dumps(manifest))
    return root


def test_selection_opens_only_intersecting_product_shards_once(tmp_path, monkeypatch):
    root = dataset(tmp_path/'data', [
        ('X',[(0,4*FIVE,(),100),(4*FIVE,8*FIVE,(),100)]),
        ('Y',[(0,8*FIVE,(),100)]),
    ])
    # Unread corruption is outside the scoped claim, but full admission rejects it.
    (root/'X-0.parquet').write_bytes(b'corrupt old shard')
    (root/'Y-0.parquet').write_bytes(b'corrupt unrelated product')
    reads = []
    original = Path.open
    def opened(path, *args, **kwargs):
        if path.is_relative_to(root):
            reads.append(path.name)
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', opened)
    histories, audit = load_collection([root], inst_ids=['X-USDT'], start=5*FIVE, end=8*FIVE)
    assert reads == ['manifest.json','X-1.parquet']
    assert [b.ts for b in histories[0].bars] == [5*FIVE,6*FIVE,7*FIVE]
    assert audit[0]['audit'][0]['integrity_scope'] == 'selected_shards'
    assert audit[0]['audit'][0]['shards_skipped'] == 1
    with pytest.raises(Unavailable, match='research_shard_hash'):
        load_histories(root)


def test_selected_shard_is_checked_before_cropping(tmp_path):
    root = dataset(tmp_path/'data', [('X',[(0,4*FIVE,(),100)])])
    # The invalid row is outside the requested rows but inside the selected file.
    frame = frame_from_bars([Bar(t,100,101,99,200 if t == 0 else 100,1,1,100)
                             for t in range(0,4*FIVE,FIVE)])
    frame.write_parquet(root/'X-0.parquet')
    manifest = json.loads((root/'manifest.json').read_text())
    manifest['products'][0]['shards'][0]['sha256'] = hashlib.sha256((root/'X-0.parquet').read_bytes()).hexdigest()
    (root/'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(Unavailable, match='invalid_rows'):
        load_histories(root,start=FIVE,end=4*FIVE)
    (root/'X-0.parquet').write_bytes(b'corrupt selected bytes')
    with pytest.raises(Unavailable, match='research_shard_hash'):
        load_histories(root,start=FIVE,end=4*FIVE)


def test_scoped_union_keeps_gaps_and_requested_edges(tmp_path):
    a = dataset(tmp_path/'a',[('X',[(FIVE,3*FIVE,(),100)])])
    b = dataset(tmp_path/'b',[('X',[(4*FIVE,6*FIVE,(),100)])])
    histories, audit = load_collection([a,b], start=0, end=7*FIVE)
    h = histories[0]
    assert (h.start,h.end) == (0,7*FIVE)
    assert [b.ts for b in h.bars] == [FIVE,2*FIVE,4*FIVE,5*FIVE]
    assert audit[-1]['missing_rows'] == 3 and audit[-1]['status'] == 'partial'


def test_scoped_overlap_still_rejects_conflicts(tmp_path):
    a = dataset(tmp_path/'a',[('X',[(0,4*FIVE,(),100)])])
    b = dataset(tmp_path/'b',[('X',[(2*FIVE,6*FIVE,(),101)])])
    with pytest.raises(Unavailable,match='conflicting_complete_candle'):
        load_collection([a,b],start=2*FIVE,end=4*FIVE)


def test_local_complete_window_does_not_claim_whole_source_complete(tmp_path):
    root = dataset(tmp_path/'data',[('X',[(0,3*FIVE,(FIVE,),100),(3*FIVE,6*FIVE,(),100)])])
    histories, audit = load_histories(root,start=3*FIVE,end=6*FIVE)
    assert len(histories[0].bars) == 3
    assert audit[0]['status'] == 'complete' and audit[0]['source_status'] == 'partial'
    assert load_histories(root)[1][0]['missing_rows'] == 1


def test_resample_preserves_complete_buckets_and_raw_history_admission(tmp_path):
    root = dataset(tmp_path/'data',[('X',[(0,6*FIVE,(FIVE,),100)])])
    h = load_histories(root)[0][0]
    result = h.resample(15)
    assert result['ts'].to_list() == [3*FIVE]
    assert result['quote'].to_list() == [300]
    assert aggregate(h,15)[0].quote == 300
    raw = History(h.product,FIVE,(Bar(0,100,101,99,200,1,1,100),),0,FIVE,())
    with pytest.raises(Unavailable,match='invalid_rows'):
        raw.resample(5)
    with pytest.raises(ValueError,match='resolution'):
        h.resample(1)


def test_missing_product_and_invalid_window_are_explicit(tmp_path):
    root = dataset(tmp_path/'data',[('X',[(0,3*FIVE,(),100)])])
    histories,audit = load_histories(root,inst_ids=['MISSING'])
    assert histories == [] and audit[-1]['missing_products'] == ['MISSING']
    with pytest.raises(ValueError,match='window'):
        load_histories(root,start=1,end=3*FIVE)


def test_selected_complete_claim_still_detects_missing_shard(tmp_path):
    root = dataset(tmp_path/'data',[('X',[(0,3*FIVE,(),100),(3*FIVE,6*FIVE,(),100)])])
    manifest=json.loads((root/'manifest.json').read_text())
    manifest['products'][0]['shards'].pop()
    (root/'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(Unavailable,match='product_coverage_mismatch'):
        load_histories(root,start=3*FIVE,end=6*FIVE)


def test_legacy_snapshot_payload_is_reused_and_selection_stays_explicit(tmp_path, monkeypatch):
    root = tmp_path/'legacy'
    product = Product('SPOT','X-USDT','X',1)
    bars = {t:Bar(t,100,101,99,100,1,1,100) for t in range(0,6*FIVE,FIVE)}
    saved = _save_snapshot(root,product,0,6*FIVE,Series(FIVE,bars,{}),at=6*FIVE,observations=[])
    manifest = {'schema':'okx-alert-canonical-research-v1',
                'contract':{'window':{'start':0,'end':6*FIVE}},
                'products':[{**saved,'status':'complete'}]}
    (root/'manifest.json').write_text(json.dumps(manifest))
    reads=[]
    original=Path.open
    def opened(path,*args,**kwargs):
        if path.is_relative_to(root):
            reads.append(path)
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',opened)
    histories,audit=load_histories(root,inst_ids=['X-USDT'],start=FIVE,end=4*FIVE)
    assert len(reads) == 2  # Manifest plus one compressed snapshot; no digest re-read.
    assert [b.ts for b in histories[0].bars] == [FIVE,2*FIVE,3*FIVE]
    assert audit[0]['integrity_scope'] == 'selected_snapshots'
    missing,checks=load_histories(root,inst_ids=['ABSENT'])
    assert missing == [] and checks[-1]['missing_products'] == ['ABSENT']
