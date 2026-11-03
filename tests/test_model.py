"""Public metadata and complete-candle admission boundaries."""

import pytest

from msharesift import model as trial

T = 20 * trial.WEEK + 2 * trial.HOUR

PRODUCT = trial.Product("SPOT", "ALT-USDT", "ALT", 1)

def row(ts, close=100., quote=25_000., *, confirm="1"):
    return [str(ts), str(close), str(close), str(close), str(close), "1", "1", str(quote), confirm]

def metadata(kind="SPOT"):
    value = dict(instType=kind, instId="ALT-USDT", baseCcy="ALT", quoteCcy="USDT", state="live", ruleType="normal", instCategory="1", listTime="1", contTdSwTime="", preMktSwTime="")
    if kind == "SWAP":
        value.update(instId="ALT-USDT-SWAP", baseCcy="", quoteCcy="", instFamily="ALT-USDT", ctType="linear", settleCcy="USDT", ctValCcy="ALT", ctVal=".1")
    return value

@pytest.mark.parametrize("kind", ["SPOT", "SWAP"])
def test_product_identity_and_derivative_units(kind):
    product = trial.admit_product(metadata(kind), T)
    assert product.key == ("okx", kind, "ALT-USDT" + ("-SWAP" if kind == "SWAP" else ""))
    assert product.base == "ALT"

@pytest.mark.parametrize("field,value", [("instCategory", ""), ("instCategory", "3"), ("state", "suspend"), ("ruleType", "pre_market"), ("quoteCcy", "USD"), ("listTime", ""), ("listTime", "0"), ("listTime", str(T + 1)), ("preMktSwTime", str(T + 1)), ("instId", "other")])
def test_metadata_does_not_guess_eligibility(field, value):
    raw = metadata()
    raw[field] = value
    with pytest.raises(trial.Unavailable):
        trial.admit_product(raw, T)

def test_old_category_and_group_id_do_not_supply_asset_class():
    raw = metadata()
    raw.update(instCategory="", category="1", groupId="1")
    with pytest.raises(trial.Unavailable):
        trial.admit_product(raw, T)

@pytest.mark.parametrize("field,value", [("ctType", "inverse"), ("settleCcy", "USD"), ("ctVal", "0"), ("ctValCcy", "OTHER")])
def test_swap_contract_metadata(field, value):
    raw = metadata("SWAP")
    raw[field] = value
    with pytest.raises(trial.Unavailable):
        trial.admit_product(raw, T)

@pytest.mark.parametrize("offset,expected", [(0, T - trial.MINUTE), (1999, T - trial.MINUTE), (2000, T), (59_999, T)])
def test_source_time_buffer(offset, expected):
    assert trial.source_cutoff(T + offset) == expected

def test_unordered_duplicates_and_incomplete_revision():
    first = row(T - trial.MINUTE, quote=150_000)
    incomplete = first.copy()
    incomplete[7], incomplete[8] = "999999", "0"
    result = trial.Series.parse([first, first.copy(), incomplete], trial.MINUTE)
    assert len(result.bars) == 1 and not result.errors
    assert result.bars[T - trial.MINUTE].quote == 150_000


def test_conflicting_complete_rows_never_choose_newest_or_recover_silently():
    first = row(T - trial.MINUTE)
    conflict = first.copy()
    conflict[7] = "0"
    result = trial.Series.parse([first, conflict, first.copy()], trial.MINUTE)
    assert not result.bars
    assert result.errors[T - trial.MINUTE] == "conflicting_complete_candle"


def test_known_noncrypto_is_distinct_from_missing_metadata():
    raw = metadata()
    raw["instCategory"] = "3"
    with pytest.raises(trial.Unavailable, match="excluded_noncrypto"):
        trial.admit_product(raw, T)
    raw["instCategory"] = ""
    with pytest.raises(trial.Unavailable, match="metadata_unavailable"):
        trial.admit_product(raw, T)
