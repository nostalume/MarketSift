"""Literal discovery and readable original text; themes belong to the agent."""

import re
from collections import deque


def catalog(row):
    item = {k: v for k, v in row.items() if k not in {"text", "raw_text"}}
    item["text_characters"] = len(row["text"])
    item["metadata"] = dict(row["metadata"])
    if "native_tables" in item["metadata"]:
        item["metadata"]["table_count"] = len(item["metadata"].pop("native_tables"))
    return item


def _fold(text):
    return text.translate(
        str.maketrans({"İ": "i", "ı": "i", "ſ": "s", "K": "k"})
    ).casefold()


def _matcher(tokens):
    # Aho-Corasick: one text traversal independent of the number of aliases.
    nodes = [{}]
    failure = [0]
    output = [[]]
    for token in tokens:
        state = 0
        for char in token:
            if char not in nodes[state]:
                nodes[state][char] = len(nodes)
                nodes.append({})
                failure.append(0)
                output.append([])
            state = nodes[state][char]
        output[state].append(token)
    queue = deque(nodes[0].values())
    while queue:
        state = queue.popleft()
        for char, child in nodes[state].items():
            queue.append(child)
            parent = failure[state]
            while parent and char not in nodes[parent]:
                parent = failure[parent]
            failure[child] = nodes[parent].get(char, 0)
            output[child].extend(output[failure[child]])

    def find(text):
        folded = _fold(text)
        positions = None
        if len(folded) != len(text):
            positions = [i for i, char in enumerate(text) for _ in _fold(char)]
        state = 0
        matches = {}

        def word(char):
            return char.isascii() and char.isalnum()

        for end, char in enumerate(folded, 1):
            while state and char not in nodes[state]:
                state = failure[state]
            state = nodes[state].get(char, 0)
            for token in output[state]:
                begin = end - len(token)
                if token.isascii() and (
                    (begin and word(folded[begin - 1]))
                    or (end < len(folded) and word(folded[end]))
                ):
                    continue
                if begin not in matches or end > matches[begin][0]:
                    matches[begin] = (end, token)
        consumed = 0
        for begin, (end, token) in sorted(matches.items()):
            if begin < consumed:
                continue
            a, b = (
                (positions[begin], positions[end - 1] + 1)
                if positions is not None
                else (begin, end)
            )
            yield text[a:b], tokens[token]
            consumed = end

    return find


def literal_matcher(references):
    tokens = {}
    for ref in references:
        for value in (ref["instrument"], ref["symbol"], ref["name"], *ref["aliases"]):
            if value and len(value) >= 2:
                tokens.setdefault(_fold(value), set()).add(ref["instrument"])
    return _matcher(tokens)


def associate(rows, references=(), *, instruments=(), find=None):
    rows = [dict(row) for row in rows]
    find = find or literal_matcher(references)
    for row in rows:
        row["mentions"] = {
            text: sorted(keys)
            for text, keys in find((row.get("title") or "") + "\n" + row["text"])
        }
    if instruments:
        wanted = set(instruments)
        rows = [
            r
            for r in rows
            if any(wanted.intersection(keys) for keys in r["mentions"].values())
        ]
    return rows


def connections(rows, references=(), definitions=()):
    """Literal/source-declared relations only; neither topics nor causality."""
    by_url = {}
    for row in rows:
        by_url.setdefault(row.get("url"), []).append(row["id"])
    native = {}
    for row in rows:
        native.setdefault((row["source"], row["native_id"]), []).append(row["id"])
    edges = []
    ordinals = {}
    for row in rows:
        m = row["metadata"]
        if m.get("topic_id") is not None:
            ordinals.setdefault(
                (row["source"], m["topic_id"], m.get("post_number")), []
            ).append(row["id"])
    for row in rows:
        m = row["metadata"]
        if m.get("topic_id") is not None:
            edges.append(
                dict(
                    kind="forum_topic",
                    document_id=row["id"],
                    source=row["source"],
                    topic_id=m["topic_id"],
                    post_number=m.get("post_number"),
                )
            )
            if m.get("reply_to_post_number"):
                edges.append(
                    dict(
                        kind="reply_to",
                        document_id=row["id"],
                        source=row["source"],
                        target_post_number=m["reply_to_post_number"],
                        target_ids=ordinals.get(
                            (row["source"], m["topic_id"], m["reply_to_post_number"]),
                            [],
                        ),
                    )
                )
        parent = row["metadata"].get("parent_native_id")
        if parent:
            edges.append(
                dict(
                    kind="reply_to",
                    document_id=row["id"],
                    source=row["source"],
                    native_id=parent,
                    target_ids=native.get((row["source"], parent), []),
                )
            )
        for url in row["links"]:
            edges.append(
                dict(
                    kind="outbound_link",
                    document_id=row["id"],
                    url=url,
                    target_ids=by_url.get(url, []),
                )
            )
        for text, keys in row.get("mentions", {}).items():
            edges.append(
                dict(
                    kind="literal_mention",
                    document_id=row["id"],
                    text=text,
                    instruments=keys,
                    ambiguous=len(keys) > 1,
                )
            )
    for ref in references:
        for version in ref.get("references", ()):
            underlying = version["rules"].get("underlying")
            if underlying:
                edges.append(
                    dict(
                        kind="native_underlying",
                        instrument=ref["instrument"],
                        underlying=underlying,
                        reference_id=version["id"],
                        source=version["source"],
                    )
                )
    for definition in definitions:
        if definition.get("instrument"):
            edges.append(
                dict(
                    kind="instrument_series",
                    instrument=definition["instrument"],
                    series=definition["series"],
                    definition_id=definition["id"],
                )
            )
    return edges


def cooccurrences(rows):
    groups = {}
    by_instrument = {}

    def contains(text, sentence):
        return (
            bool(
                re.search(
                    r"(?<![A-Za-z0-9])" + re.escape(text) + r"(?![A-Za-z0-9])",
                    sentence,
                    re.IGNORECASE,
                )
            )
            if text.isascii()
            else text.casefold() in sentence.casefold()
        )

    for row in rows:
        for keys in row["mentions"].values():
            for key in keys:
                by_instrument.setdefault(key, []).append(row["id"])
        for sentence in re.split(
            r"[。！？\n]+", (row.get("title") or "") + "\n" + row.get("text", "")
        ):
            keys = sorted(
                {
                    key
                    for text, codes in row["mentions"].items()
                    if contains(text, sentence)
                    for key in codes
                }
            )
            if len(keys) > 1:
                groups.setdefault(tuple(keys), []).append(
                    dict(
                        id=row["id"],
                        sentence=sentence,
                        ambiguous=any(
                            len(v) > 1
                            for t, v in row["mentions"].items()
                            if contains(t, sentence)
                        ),
                    )
                )
    catalogs = []
    for row in rows:
        catalogs.append(catalog(row))
    return dict(
        documents=catalogs,
        groups=[
            dict(members=list(keys), evidence=values) for keys, values in groups.items()
        ],
        instruments={
            key: list(dict.fromkeys(ids)) for key, ids in by_instrument.items()
        },
        meaning="Same-sentence literal co-occurrence only; no transitive merging, theme or eligibility inference. Exact IDs expand full texts.",
    )


def expand(seeds, candidates, *, kind, direction):
    """Direct replies, whole native topic and URL edges have separate meanings."""
    selected, edges, unresolved = {}, [], []

    def parent(child, target):
        m, t = child["metadata"], target["metadata"]
        return child["source"] == target["source"] and (
            (
                m.get("parent_native_id") is not None
                and str(m["parent_native_id"]) == str(target["native_id"])
            )
            or (
                m.get("topic_id") is not None
                and m.get("topic_id") == t.get("topic_id")
                and m.get("reply_to_post_number") is not None
                and m["reply_to_post_number"] == t.get("post_number")
            )
        )

    for seed in seeds:
        for target in candidates:
            if target["id"] == seed["id"]:
                continue
            out = (
                parent(seed, target)
                if kind == "replies"
                else target["url"] in seed["links"]
                if kind == "outbound_link"
                else seed["source"] == target["source"]
                and seed["metadata"].get("topic_id") is not None
                and seed["metadata"].get("topic_id")
                == target["metadata"].get("topic_id")
            )
            if kind == "same_url":
                out = target.get("url") is not None and target["url"] == seed.get("url")
            incoming = (
                parent(target, seed)
                if kind == "replies"
                else seed["url"] in target["links"]
                if kind == "outbound_link"
                else out
            )
            if kind == "same_url":
                incoming = out
            if (out and direction in {"out", "both"}) or (
                incoming and direction in {"in", "both"}
            ):
                selected[target["id"]] = target
                edges.append(
                    dict(
                        kind=kind,
                        seed_id=seed["id"],
                        target_id=target["id"],
                        direction="out" if out else "in",
                    )
                )
        if kind == "outbound_link" and direction in {"out", "both"}:
            found = {r["url"] for r in candidates}
            unresolved.extend(
                dict(
                    seed_id=seed["id"],
                    url=u,
                    reason="target not resolved within selected scope; inspect scope and acquisition before collecting",
                )
                for u in seed["links"]
                if u not in found
            )
        if (
            kind == "replies"
            and direction in {"out", "both"}
            and seed["metadata"].get("parent_native_id")
            and not any(parent(seed, r) for r in candidates)
        ):
            unresolved.append(
                dict(
                    seed_id=seed["id"],
                    native_id=seed["metadata"]["parent_native_id"],
                    reason="parent not in local scope",
                )
            )
        unresolved.extend(
            dict(
                seed_id=seed["id"],
                url=u,
                reason="attachment referenced; image/content not read",
            )
            for u in seed["attachments"]
        )
    return dict(
        dataset="documents",
        rows=list(selected.values()),
        edges=edges,
        seed_ids=[r["id"] for r in seeds],
        unresolved=unresolved,
    )
