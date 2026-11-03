import gzip
import json
from pathlib import Path
from types import MappingProxyType

import pytest

from msharesift import snapshot as research_data
from msharesift.model import FIVE, Bar, Product, Series, Unavailable


def snapshot(tmp_path: Path):
    product = Product("SPOT", "X-USDT", "X", 1)
    series = Series(
        FIVE,
        MappingProxyType({
            0: Bar(0, 1, 1, 1, 1, 1, 1, 1),
            FIVE: Bar(FIVE, 1, 1, 1, 1, 1, 1, 1),
        }),
        MappingProxyType({}),
    )
    return research_data._save_snapshot(tmp_path, product, 0, 2 * FIVE, series,
                                        at=2 * FIVE, observations=[])


def test_canonical_snapshot_roundtrip_and_verify(tmp_path):
    item = snapshot(tmp_path)
    product, series, body = research_data._load_snapshot(Path(item["snapshot"]))
    assert product.inst_id == "X-USDT" and len(series.bars) == 2
    assert body["quality"]["complete"] is True


def test_canonical_snapshot_tamper_is_refused(tmp_path):
    item = snapshot(tmp_path)
    path = Path(item["snapshot"])
    with gzip.open(path, "rb") as stream:
        body = json.loads(stream.read())
    body["rows"][0][4] = 2
    path.write_bytes(gzip.compress(json.dumps(body, separators=(",", ":")).encode(), mtime=0))
    with pytest.raises(Unavailable, match="hash"):
        research_data._load_snapshot(path)
