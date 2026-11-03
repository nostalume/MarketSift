"""Whole-catalog semantics, upper bounds and bounded availability evidence."""

import json

import httpx
import polars as pl
import pytest

from msharesift.candles.acquire import acquire
from msharesift.candles.universe import (
    DAY,
    capacity_rows,
    census_rows,
    inventory,
    probe,
)
from msharesift.model import FIVE, Unavailable, admit_product


def spot(base="BTC", **changes):
    return {
        "instType": "SPOT",
        "instId": base + "-USDT",
        "baseCcy": base,
        "quoteCcy": "USDT",
        "instCategory": "1",
        "state": "live",
        "ruleType": "normal",
        "listTime": "1",
    } | changes


def transport(catalog=None, *, empty=False):
    catalog = catalog or [spot()]

    def handle(request):
        if request.url.path.endswith("/time"):
            data = [{"ts": str(2000 * DAY + 2000)}]
        elif request.url.path.endswith("/instruments"):
            data = catalog if request.url.params["instType"] == "SPOT" else []
        else:
            cursor = int(request.url.params["after"])
            limit = int(request.url.params["limit"])
            data = (
                []
                if empty
                else [
                    [str(t), "100", "101", "99", "100", "1", "1", "100", "1"]
                    for t in range(
                        cursor - FIVE, max(-1, cursor - (limit + 1) * FIVE), -FIVE
                    )
                ]
            )
        return httpx.Response(200, json={"code": "0", "data": data})

    return httpx.MockTransport(handle)


def test_research_policy_does_not_change_alert_defaults():
    with pytest.raises(Unavailable, match="excluded_base"):
        admit_product(spot(), 2000 * DAY)
    assert admit_product(spot(), 2000 * DAY, excluded_bases=()).base == "BTC"
    with pytest.raises(Unavailable, match="excluded_spot_quote"):
        admit_product(spot(quoteCcy="USD"), 2000 * DAY, excluded_bases=())


def test_universe_has_no_top_n_or_100_product_cut():
    rows = census_rows(
        {"SPOT": [spot("X" + str(i)) for i in range(150)] + [spot()], "SWAP": []},
        2000 * DAY,
    )
    assert rows.height == 151 and rows["research_eligible"].sum() == 151
    assert rows["alert_eligible"].sum() == 150


def test_noneligible_and_unknown_metadata_remain_in_census():
    rows = census_rows(
        {
            "SPOT": [spot(), spot("A", instCategory="3"), spot("B", instCategory="")],
            "SWAP": [],
        },
        2000 * DAY,
    )
    assert rows.height == 3 and rows["research_eligible"].sum() == 1
    assert "metadata_unavailable:instCategory" in rows["research_reason"].to_list()


def test_duplicate_catalog_fails_instead_of_double_counting():
    with pytest.raises(Unavailable, match="duplicate_catalog"):
        census_rows({"SPOT": [spot(), spot()], "SWAP": []}, 2000 * DAY)


def test_listing_clipping_and_units():
    rows = census_rows(
        {"SPOT": [spot(listTime=str(1990 * DAY))], "SWAP": []}, 2000 * DAY
    )
    caps = capacity_rows(rows, 2000 * DAY)
    assert (
        caps.filter((pl.col("scenario_days") == 365) & (pl.col("grain") == "5m"))[
            "expected_rows_upper"
        ].item()
        == 10 * 288
    )
    assert (
        caps.filter((pl.col("scenario_days") == 365) & (pl.col("grain") == "1h"))[
            "expected_rows_upper"
        ].item()
        == 10 * 24
    )
    assert caps["history_status"].unique().to_list() == ["listing_bound_unverified"]


def test_inventory_preserves_raw_catalog_and_is_immutable(tmp_path):
    root = tmp_path / "census"
    result = inventory(root, end=1999 * DAY, transport=transport())
    assert result["requests"] == 3 and result["eligible_products"] == 1
    assert (
        json.loads((root / "catalog.json").read_text())["catalogs"]["SPOT"][0]["instId"]
        == "BTC-USDT"
    )
    with pytest.raises(FileExistsError):
        inventory(root, transport=transport())


def test_available_boundary_is_not_full_history_claim(tmp_path):
    root = tmp_path / "census"
    inventory(root, end=1999 * DAY, transport=transport())
    result = probe(tmp_path / "probe", root, days=365, transport=transport())
    rows = pl.read_parquet(tmp_path / "probe/availability.parquet")
    assert result["products"] == 1 and rows.height == 2 and rows["exact_boundary"].all()
    assert "unverified" in result["meaning"]


def test_empty_history_keeps_product_and_unknown_capacity(tmp_path):
    root = tmp_path / "census"
    inventory(root, end=1999 * DAY, transport=transport())
    probe(tmp_path / "probe", root, days=365, transport=transport(empty=True))
    rows = pl.read_parquet(tmp_path / "probe/availability.parquet")
    assert rows.height == 2 and rows["status"].to_list() == ["no_page_in_window"] * 2
    assert not rows["exact_boundary"].any()


def test_budget_exhaustion_keeps_every_unprobed_member(tmp_path):
    root = tmp_path / "census"
    inventory(root, end=1999 * DAY, transport=transport([spot(), spot("ALT")]))
    result = probe(
        tmp_path / "probe",
        root,
        days=365,
        max_requests=1,
        workers=1,
        transport=transport(),
    )
    rows = pl.read_parquet(tmp_path / "probe/availability.parquet")
    assert rows.height == 4 and rows["inst_id"].n_unique() == 2
    assert result["budget_stopped"] and result["requests"] == 1
    assert "not_probed_budget" in rows["status"].to_list()


def test_probe_refuses_changed_inventory(tmp_path):
    root = tmp_path / "census"
    inventory(root, end=1999 * DAY, transport=transport())
    (root / "instruments.csv").write_text("changed")
    with pytest.raises(Unavailable, match="inventory_integrity"):
        probe(tmp_path / "probe", root, transport=transport())


def test_research_fetch_can_admit_btc_without_rewriting_old_contract(tmp_path):
    result = acquire(
        tmp_path / "research",
        ["BTC-USDT"],
        "5m",
        0,
        10 * FIVE,
        transport=transport(),
        universe="research",
    )
    assert result["complete"] == 1
    body = json.loads((tmp_path / "research/manifest.json").read_text())
    assert body["contract"]["universe"] == "explicit_current_usdt_crypto_all_bases"
    with pytest.raises(Unavailable, match="excluded_base"):
        acquire(
            tmp_path / "alert", ["BTC-USDT"], "5m", 0, 10 * FIVE, transport=transport()
        )


def test_optional_progress_write_failure_does_not_lose_probe(tmp_path, monkeypatch):
    import msharesift.candles.universe as owner

    root = tmp_path / "census"
    inventory(root, end=1999 * DAY, transport=transport())
    real = owner.write_json

    def write(path, body):
        if path.name == "progress.json":
            raise PermissionError("reader holds a Windows sharing lock")
        real(path, body)

    monkeypatch.setattr(owner, "write_json", write)
    result = probe(tmp_path / "probe", root, transport=transport())
    assert result["progress_write_warnings"] == 1
    assert pl.read_parquet(tmp_path / "probe/availability.parquet").height == 2
    assert (tmp_path / "probe/manifest.json").exists()


def test_other_quotes_remain_in_total_candidate_capacity():
    rows = census_rows(
        {
            "SPOT": [spot(), spot("EURC", instId="EURC-USDC", quoteCcy="USDC")],
            "SWAP": [],
        },
        2000 * DAY,
    )
    assert rows["candidate_crypto"].sum() == 2
    assert rows["research_eligible"].sum() == 1
    full = capacity_rows(rows, 1999 * DAY, all_quotes=True)
    native = capacity_rows(rows, 1999 * DAY)
    assert full["inst_id"].n_unique() == 2 and native["inst_id"].n_unique() == 1
    assert (
        rows.filter(pl.col("inst_id") == "EURC-USDC")["adapter_status"].item()
        == "quote_or_contract_adapter_required"
    )
