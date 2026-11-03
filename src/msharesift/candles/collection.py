"""Join admitted canonical windows by product/grain; retain gaps and provenance."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import polars as pl

from msharesift.candles.frame import _coverage, _merge_frame
from msharesift.candles.history import History, _read_histories
from msharesift.model import Bar, Unavailable
from msharesift.products import same_contract_rules


def load_collection(roots, *, inst_ids=None, start=None, end=None):
    """Read multiple immutable datasets without requiring research-side cleaning.

    Adjacent windows are joined, identical overlap is deduplicated, conflicting
    overlap is rejected. The union of declared windows defines the expected
    range; absent intervals remain missing and are never forward-filled.
    """
    roots = [Path(root).resolve() for root in roots]
    if not roots or len(roots) != len(set(roots)):
        raise ValueError("nonempty distinct canonical dataset roots required")
    inst_ids = None if inst_ids is None else frozenset(inst_ids)
    grouped, root_audit = defaultdict(list), []
    for root in roots:
        loaded, checks, manifest_hash = _read_histories(
            root, inst_ids=inst_ids, start=start, end=end
        )
        root_audit.append(
            {
                "kind": "source",
                "root": str(root),
                "manifest_sha256": manifest_hash,
                "audit": checks,
            }
        )
        for history in loaded:
            grouped[(history.product.key, history.grain)].append(history)
    histories, audit = [], []
    for (_, grain), parts in sorted(grouped.items()):
        product = parts[0].product
        if any(h.product != product for h in parts):
            raise Unavailable("collection_product_identity_changed")
        known_rules = [h.rules for h in parts if h.rules is not None]
        if known_rules and any(not same_contract_rules(known_rules[0], r) for r in known_rules[1:]):
            raise Unavailable("collection_contract_rules_changed")
        # Never attach today's rules to older segments whose units were not recorded.
        rules = (max(known_rules, key=lambda r: r["observed_at_ms"])
                 if len(known_rules) == len(parts) else None)
        start, end = min(h.start for h in parts), max(h.end for h in parts)
        if len(parts) > 1:
            frame, quality = _merge_frame(
                pl.concat([h.frame for h in parts]), grain, start, end
            )
        else:
            frame = parts[0].frame
            quality = _coverage(frame, grain, start, end)
        evidence = {(p["path"], p["sha256"]): p for h in parts for p in h.provenance}
        histories.append(
            History(
                product,
                grain,
                tuple(Bar(*row) for row in frame.iter_rows()),
                start,
                end,
                tuple(evidence.values()),
                frame,
                rules,
            )
        )
        audit.append(
            {
                "kind": "product",
                "product": product.key,
                "grain": grain,
                "start": start,
                "end": end,
                "segments": len(parts),
                "status": "complete" if quality["missing_rows"] == 0 else "partial",
                **quality,
            }
        )
    return histories, root_audit + audit
