"""Stateless command adapter for explicit collection and local query composition."""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from . import api


def _json(path):
    return json.loads(
        sys.stdin.read() if str(path) == "-" else path.read_text(encoding="utf-8-sig")
    )


def execute(args):
    if args.command == "sources":
        return api.sources()
    if args.command == "query":
        spec = _json(args.spec)
        if args.as_of:
            if "as_of" in spec:
                raise ValueError("use as_of in spec or --as-of, not both")
            spec["as_of"] = args.as_of
        return api.query(database=args.database, **spec)
    if args.requests:
        if any(
            (
                args.source,
                args.instrument,
                args.dataset,
                args.since,
                args.until,
                args.url,
                args.series_id,
                args.input,
                args.host,
                args.interval,
                args.basis,
            )
        ):
            raise ValueError("--requests is exclusive with flat collection selectors")
        requests = _json(args.requests)
    else:
        requests = []
        for name in args.source:
            request = dict(source=name)
            for field, value in dict(
                instruments=args.instrument,
                datasets=args.dataset,
                urls=args.url,
                series_ids=args.series_id,
                since=args.since,
                until=args.until,
                interval=args.interval,
                basis=args.basis,
            ).items():
                if value:
                    request[field] = value
            request["options"] = {
                k: v for k, v in dict(input=args.input, hosts=args.host).items() if v
            }
            requests.append(request)
    return api.collect(requests, database=args.database, timeout=args.timeout)


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--database", type=Path, default=api.DEFAULT_DB)
    root.add_argument("--pretty", action="store_true")
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("sources", "collect", "query"):
        p = commands.add_parser(name)
        p.add_argument("--database", type=Path, default=argparse.SUPPRESS)
        p.add_argument("--pretty", action="store_true", default=argparse.SUPPRESS)
        if name == "query":
            p.add_argument(
                "--spec", type=Path, required=True, help="JSON file or - for stdin"
            )
            p.add_argument("--as-of")
        elif name == "collect":
            p.add_argument("--requests", type=Path, help="JSON list or - for stdin")
            for flag in (
                "source",
                "instrument",
                "dataset",
                "series-id",
                "url",
                "input",
                "host",
            ):
                p.add_argument("--" + flag, action="append", default=[])
            for flag in ("since", "until", "interval", "basis"):
                p.add_argument("--" + flag)
            p.add_argument("--timeout", type=float, default=15)
    return root


def main(argv=None):
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = parser().parse_args(argv)
    try:
        result = execute(args)
        print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))
        return 1 if args.command == "collect" and result["status"] != "ok" else 0
    except (ValueError, TypeError, KeyError, OSError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
