"""Canonical Polars cleaning invariants; no network or production state."""

import math

import polars as pl
import pytest

from msharesift.candles.frame import aggregate_frame, clean_frame, frame_from_bars
from msharesift.model import FIVE, Bar, Unavailable


def frame():
    return frame_from_bars(
        [Bar(i * FIVE, 100, 102, 99, 101, 1, 1, 100) for i in range(6)]
    )


def test_exact_duplicates_removed_but_conflicts_rejected():
    source = frame()
    clean, quality = clean_frame(pl.concat([source, source.head(1)]), FIVE, 0, 6 * FIVE)
    assert clean.equals(source) and quality["duplicate_rows_removed"] == 1
    conflict = source.head(1).with_columns(pl.lit(100.5).alias("close"))
    with pytest.raises(Unavailable, match="conflicting"):
        clean_frame(pl.concat([source, conflict]), FIVE, 0, 6 * FIVE)


@pytest.mark.parametrize(
    "column,value",
    [
        ("open", math.nan),
        ("high", math.inf),
        ("low", -1.0),
        ("close", 1000.0),
        ("quote", -1.0),
    ],
)
def test_invalid_candles_never_become_gaps(column, value):
    with pytest.raises(Unavailable, match="invalid_rows"):
        clean_frame(
            frame().with_columns(pl.lit(value).alias(column)), FIVE, 0, 6 * FIVE
        )


def test_missing_bucket_is_removed_and_not_filled():
    source = frame().filter(pl.col("ts") != FIVE)
    clean, quality = clean_frame(source, FIVE, 0, 6 * FIVE)
    assert quality["missing_rows"] == 1
    result = aggregate_frame(clean, FIVE, 3 * FIVE, 0, 6 * FIVE)
    assert result.height == 1 and result["ts"].to_list() == [3 * FIVE]
    assert result["quote"].to_list() == [300.0]


def test_subsecond_and_out_of_window_timestamps_refused():
    for source in (
        frame().with_columns(pl.col("ts").cast(pl.Float64)),
        frame().with_columns(pl.col("ts") + 1),
        frame().with_columns(pl.lit(2**63 - 1).alias("ts")),
    ):
        with pytest.raises(Unavailable):
            clean_frame(source, FIVE, 0, 6 * FIVE)
