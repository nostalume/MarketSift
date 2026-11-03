"""Contract meaning crosses acquisition, scoped reads, joins and cost comparison."""

import json
import shutil
from dataclasses import asdict

import httpx
import pytest

from msharesift import cli as alert
from msharesift import products as rp
from msharesift.candles.acquire import acquire
from msharesift.candles.collection import load_collection
from msharesift.candles.history import load_histories, normalize_dataset
from msharesift.market import fetch_funding, fetch_open_interest
from msharesift.model import HOUR, Unavailable, admit_product
from msharesift.okx import Budget, PublicClient

AT = 100 * HOUR


def metadata(kind="SWAP", *, inverse=False, rule="normal", category="3", multiplier="1"):
    base = "BTC" if kind == "FUTURES" or inverse else "NVDA"
    settlement = "BTC" if inverse else ("USD" if kind == "FUTURES" else "USDT")
    name = (f"{base}-USD-261225" if inverse else
            f"{base}-USD_UM_XPERP-310613" if rule == "xperp" else
            f"{base}-USD_UM-261225" if kind == "FUTURES" else f"{base}-USDT-SWAP")
    return {"instType": kind, "instId": name, "instCategory": category,
            "state": "live", "ruleType": rule, "listTime": "1", "instFamily": name.rsplit("-", 1)[0],
            "ctType": "inverse" if inverse else "linear", "settleCcy": settlement,
            "ctVal": "100" if inverse else "0.01" if kind == "FUTURES" else "1",
            "ctMult": multiplier, "ctValCcy": "USD" if inverse else base,
            "expTime": str(200 * HOUR) if kind == "FUTURES" else "", "tickSz": "0.01",
            "lotSz": "0.01", "minSz": "0.01", "futureSettlement": inverse}


def pack(*rows):
    return {"cutoff_ms": AT, "catalogs": {kind: [r for r in rows if r["instType"] == kind] for kind in rp.KINDS}}


def bars_transport(calls):
    def handle(request):
        calls.append(request)
        assert request.url.path in ("/api/v5/market/candles", "/api/v5/market/history-candles")
        end, limit = int(request.url.params["after"]), int(request.url.params["limit"])
        rows = [[str(t), "100", "102", "99", "101", "1", "1", "100", "1"]
                for t in range(end-HOUR, max(-HOUR, end-(limit+1)*HOUR), -HOUR)]
        return httpx.Response(200, json={"code": "0", "data": rows})
    return httpx.MockTransport(handle)


@pytest.mark.parametrize("raw", [metadata(), metadata("FUTURES", category="1"),
                                     metadata("FUTURES", inverse=True, category="1"),
                                     metadata("FUTURES", rule="xperp")])
def test_research_expands_identity_without_changing_legacy_admission(raw):
    item = rp.admit_research_product(raw, AT)
    assert set(asdict(item.product)) == {"inst_type", "inst_id", "base", "normal_since"}
    assert item.rules["settlement"] == raw["settleCcy"]
    assert item.rules["funding"] is (raw["instType"] == "SWAP" or raw["ruleType"] == "xperp")
    with pytest.raises(Unavailable):
        admit_product(raw, AT)


def test_unknown_units_and_premarket_are_retained_as_rejections():
    premarket = metadata(rule="pre_market")
    wrong_units = metadata("FUTURES", category="1")
    wrong_units["ctValCcy"] = "USD"
    catalog = rp.prepare_catalog(pack(premarket, wrong_units))
    assert not catalog.products
    assert "trading_rule" in catalog.rejected[premarket["instId"]]
    assert "contract_units" in catalog.rejected[wrong_units["instId"]]


def test_snapshot_reuses_success_and_preserves_a_failed_kind(tmp_path):
    calls = []
    def handle(request):
        calls.append(request)
        if request.url.path.endswith("time"):
            return httpx.Response(200, json={"code": "0", "data": [{"ts": str(AT)}]})
        if request.url.params["instType"] == "FUTURES":
            return httpx.Response(200, json={"code": "500", "data": []})
        rows = [metadata()] if request.url.params["instType"] == "SWAP" else []
        return httpx.Response(200, json={"code": "0", "data": rows})
    transport = httpx.MockTransport(handle)
    first = rp.catalog_snapshot(tmp_path, transport=transport)
    assert len(calls) == 4
    second = rp.catalog_snapshot(tmp_path, transport=transport)
    assert second == first and len(calls) == 4
    summary = rp.summarize_catalog(rp.prepare_catalog(first))
    assert summary["status"] == "partial" and summary["supported"] == 1
    assert "FUTURES" in summary["failures"]


def test_reused_catalog_is_admitted_once_and_rules_survive_all_consumers(tmp_path, monkeypatch):
    calls, admissions = [], []
    rows = [metadata(), metadata("FUTURES", category="1")]
    original = rp.admit_research_product
    def admitted(raw, at):
        admissions.append(raw["instId"])
        return original(raw, at)
    monkeypatch.setattr(rp, "admit_research_product", admitted)
    catalog = rp.prepare_catalog(pack(*rows))
    root = tmp_path/"data"
    result = acquire(root, [r["instId"] for r in rows], "1h", 24*HOUR, 72*HOUR,
                     universe="research", catalog=catalog, transport=bars_transport(calls))
    assert result["complete"] == 2 and result["requests"] == len(calls) == 2
    histories, audit = load_collection([root])
    assert len(admissions) == len(rows)
    assert all(h.rules == catalog.products[h.product.inst_id].rules for h in histories)
    assert all(h.resample(1440).height == 2 for h in histories)
    assert all(row.get("missing_rows", 0) == 0 for row in audit)
    normalize_dataset(root, tmp_path/"normalized")
    normalized = load_histories(tmp_path/"normalized")[0]
    assert [h.rules for h in normalized] == [h.rules for h in histories]
    result = acquire(root, [r["instId"] for r in rows], "1h", 24*HOUR, 72*HOUR,
                     universe="research", catalog=catalog, transport=bars_transport(calls))
    assert result["requests"] == 0 and len(calls) == 2
    assert len(admissions) == len(rows)


def test_rule_change_rejects_union_and_resume_without_backfilling_old_data(tmp_path):
    raw = metadata()
    catalog = rp.prepare_catalog(pack(raw))
    name = raw["instId"]
    root = tmp_path/"first"
    acquire(root, [name], "1h", 24*HOUR, 48*HOUR, universe="research", catalog=catalog,
            transport=bars_transport([]))
    second = tmp_path/"second"
    shutil.copytree(root, second)
    manifest = json.loads((second/"manifest.json").read_bytes())
    manifest["products"][0]["rules"]["contract_value"] = 2
    (second/"manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(Unavailable, match="collection_contract_rules_changed"):
        load_collection([root, second])
    changed = rp.prepare_catalog(pack({**raw, "ctVal": "2"}))
    with pytest.raises(ValueError, match="contract rules changed"):
        acquire(root, [name], "1h", 24*HOUR, 48*HOUR, universe="research", catalog=changed,
                transport=bars_transport([]))
    del manifest["products"][0]["rules"]
    manifest["contract"]["universe"] = "explicit_current_usdt_crypto_all_bases"
    (second/"manifest.json").write_text(json.dumps(manifest))
    histories, _ = load_collection([root, second])
    assert histories[0].rules is None
    acquire(second, [name], "1h", 24*HOUR, 48*HOUR, universe="research", catalog=catalog,
            transport=bars_transport([]))
    assert "rules" not in json.loads((second/"manifest.json").read_bytes())["products"][0]


def test_futures_lookup_falls_through_to_actual_directory():
    raw, calls = metadata("FUTURES", category="1"), []
    def handle(request):
        calls.append(request)
        if request.url.path.endswith("time"):
            rows = [{"ts": str(AT)}]
        else:
            rows = [raw] if request.url.params["instType"] == "FUTURES" else []
        return httpx.Response(200, json={"code": "0", "data": rows})
    with PublicClient(Budget(seconds=20,reserve=2),transport=httpx.MockTransport(handle)) as client:
        catalog = rp.prepare_catalog(rp.request_catalog(client, [raw["instId"]]))
    assert catalog.products[raw["instId"]].product.inst_type == "FUTURES"
    assert "FUTURES" in [r.url.params.get("instType") for r in calls]


def test_funding_rules_distinguish_expiry_xperp_and_unknown_and_oi_supports_futures():
    expiry = rp.admit_research_product(metadata("FUTURES",category="1"),AT)
    xperp = rp.admit_research_product(metadata("FUTURES",rule="xperp"),AT)
    calls = []
    def handle(request):
        calls.append(request)
        if request.url.path.endswith("open-interest"):
            assert request.url.params["instType"] == "FUTURES"
            rows = [{"instId":expiry.product.inst_id,"ts":"9000","oi":"10","oiCcy":"0.1","oiUsd":"5000"}]
        else:
            rows = [{"instId":xperp.product.inst_id,"fundingTime":"9000","fundingRate":"0.001"}] if request.url.params["after"] == "10000" else []
        return httpx.Response(200,json={"code":"0","data":rows})
    with PublicClient(Budget(seconds=20,reserve=2),transport=httpx.MockTransport(handle)) as client:
        assert fetch_funding(client,expiry.product,8000,10000)[1]["complete"] is False
        assert fetch_funding(client,expiry.product,8000,10000,rules=expiry.rules)[1]["not_applicable"]
        assert not calls
        assert fetch_funding(client,xperp.product,8000,10000,rules=xperp.rules)[0][0]["funding_rate"] == .001
        assert fetch_open_interest(client,expiry.product,10000)[0][0]["oi_ccy"] == .1


def test_linear_multiplier_funding_and_unknown_costs_change_the_comparison():
    item = rp.admit_research_product(metadata(),AT)
    result = rp.position_scenario(item.product,item.rules,2,100,110,fee_rate=.0005,funding=[(.001,105)])
    assert result["gross_pnl"] == 20
    assert result["net_pnl"] == pytest.approx(19.58)
    assert result["pnl_ccy"] == "USDT"
    double = rp.position_scenario(item.product,{**item.rules,"multiplier":2},2,100,110,fee_rate=.0005,funding=[(.001,105)])
    assert double["net_pnl"] == pytest.approx(2*result["net_pnl"])
    assert rp.position_scenario(item.product,item.rules,2,100,110)["net_pnl"] is None
    assert rp.position_scenario(item.product,item.rules,2,100,110,fee_rate=0)["funding"] is None


def test_inverse_pnl_is_coin_and_expiry_never_silently_rolls():
    item = rp.admit_research_product(metadata("FUTURES",inverse=True,category="1"),AT)
    result = rp.position_scenario(item.product,item.rules,10,50000,55000,fee_rate=.0005)
    assert result["pnl_ccy"] == "BTC" and result["notional_ccy"] == "USD"
    assert result["entry_notional"] == 1000
    assert result["gross_pnl"] == pytest.approx(1000*(1/50000-1/55000))
    assert result["fees"] == pytest.approx(.0005*(1000/50000+1000/55000))
    assert result["funding"] == 0
    short = rp.position_scenario(item.product,item.rules,10,50000,55000,side="short",fee_rate=0)
    assert short["gross_pnl"] == pytest.approx(-result["gross_pnl"])
    with pytest.raises(ValueError,match="beyond contract expiry"):
        rp.position_scenario(item.product,item.rules,10,50000,55000,exit_at=201*HOUR)


def test_cli_products_uses_saved_catalog_without_network(tmp_path,monkeypatch,capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(alert,"ROOT",tmp_path)
    monkeypatch.setattr(rp,"request_catalog",lambda *args: pytest.fail("unexpected catalog request"))
    root = tmp_path/"catalog"
    root.mkdir()
    (root/"catalog.json").write_text(json.dumps(pack(metadata())))
    assert alert.main(["candle-data","products","--root","catalog","--inst-id","NVDA-USDT-SWAP"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["selected"]["NVDA-USDT-SWAP"]["rules"]["asset_class"] == "stocks"
