"""Mainline canonical candle cleaning and UTC aggregation using Polars."""

from __future__ import annotations

import polars as pl

from msharesift.model import Bar, Unavailable

COLUMNS = ("ts", "open", "high", "low", "close", "volume", "volume_ccy", "quote")
SCHEMA = {name: pl.Int64 if name == "ts" else pl.Float64 for name in COLUMNS}


def frame_from_bars(bars):
    return pl.DataFrame(
        [
            (b.ts, b.open, b.high, b.low, b.close, b.volume, b.volume_ccy, b.quote)
            for b in bars
        ],
        schema=SCHEMA,
        orient="row",
    )


def clean_frame(frame, grain, start, end):
    """Reject invalid/conflicting rows; deduplicate exact repeats; never fill gaps."""
    _check_window(grain, start, end)
    if set(frame.columns) != set(COLUMNS):
        raise Unavailable("candle_frame_schema")
    if frame.schema["ts"] != pl.Int64:
        raise Unavailable("candle_timestamp_type")
    frame = frame.select(
        pl.col(name).cast(dtype, strict=True) for name, dtype in SCHEMA.items()
    )
    invalid = frame.filter(
        pl.any_horizontal(pl.all().is_null())
        | ~pl.all_horizontal(pl.col(name).is_finite() for name in COLUMNS[1:])
        | (pl.col("ts") % grain != 0)
        | (pl.col("ts") < start)
        | (pl.col("ts") > end - grain)
        | (pl.col("low") <= 0)
        | (pl.col("open") < pl.col("low"))
        | (pl.col("close") < pl.col("low"))
        | (pl.col("open") > pl.col("high"))
        | (pl.col("close") > pl.col("high"))
        | pl.any_horizontal(
            pl.col(name) < 0 for name in ("volume", "volume_ccy", "quote")
        )
    )
    if invalid.height:
        raise Unavailable("candle_frame_invalid_rows")
    return _merge_frame(frame, grain, start, end)


def _check_window(grain, start, end):
    if grain <= 0 or not 0 <= start < end or start % grain or end % grain:
        raise ValueError("invalid candle window")


def _coverage(frame, grain, start, end, duplicates=0):
    return {
        "observed_rows": frame.height,
        "duplicate_rows_removed": duplicates,
        "expected_rows": (end - start) // grain,
        "missing_rows": (end - start) // grain - frame.height,
    }


def _merge_frame(frame, grain, start, end):
    """Union admitted rows; only overlap and coverage are new obligations."""
    original = frame.height
    frame = frame.unique()
    if frame.get_column("ts").n_unique() != frame.height:
        raise Unavailable("conflicting_complete_candle")
    frame = frame.sort("ts")
    return frame, _coverage(frame, grain, start, end, original - frame.height)


def aggregate_frame(frame, source_grain, target_grain, start, end):
    """Only complete UTC buckets; a removed bucket remains a visible time gap."""
    _check_resolution(source_grain, target_grain)
    clean, _ = clean_frame(frame, source_grain, start, end)
    return _aggregate_frame(clean, source_grain, target_grain, start, end)


def _check_resolution(source_grain, target_grain):
    if target_grain < source_grain or target_grain % source_grain:
        raise ValueError("source resolution cannot support requested timeframe")


def _aggregate_frame(frame, source_grain, target_grain, start, end):
    """Aggregate admitted rows, retaining only complete UTC buckets."""
    return (
        frame.with_columns(
            (pl.col("ts") // target_grain * target_grain).alias("bucket")
        )
        .group_by("bucket")
        .agg(
            pl.len().alias("count"),
            pl.col("ts").min().alias("first_ts"),
            pl.col("ts").max().alias("last_ts"),
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum(),
            pl.col("volume_ccy").sum(),
            pl.col("quote").sum(),
        )
        .filter(
            (pl.col("count") == target_grain // source_grain)
            & (pl.col("first_ts") == pl.col("bucket"))
            & (pl.col("last_ts") + source_grain == pl.col("bucket") + target_grain)
            & (pl.col("bucket") >= start)
            & (pl.col("bucket") + target_grain <= end)
        )
        .rename({"bucket": "ts"})
        .select(COLUMNS)
        .sort("ts")
    )


def aggregate(history, minutes):
    frame = history.resample(minutes)
    return tuple(Bar(*row) for row in frame.iter_rows())
