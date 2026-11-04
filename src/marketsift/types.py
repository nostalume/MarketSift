"""Ordinary boundary values. Annotations add no runtime admission workflow."""

from datetime import datetime
from typing import NotRequired, Required, TypedDict

Time = str | datetime
Record = dict


class CollectRequest(TypedDict, total=False):
    source: Required[str]
    datasets: list[str]
    instruments: list[str]
    series_ids: list[str]
    urls: list[str]
    since: Time
    until: Time
    interval: str
    basis: str
    options: dict


class Batch(TypedDict, total=False):
    read: Required[Record]
    instruments: list[Record]
    documents: list[Record]
    bars: list[Record]
    series: list[Record]
    series_values: list[Record]
    trades: list[Record]


class Content(TypedDict):
    text: str
    links: list[str]
    attachments: list[str]
    issues: NotRequired[list[str]]
