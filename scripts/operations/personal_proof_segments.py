"""Log segments for the personal database proof: a relay stores the signed entries of a batch as
one blob, and names it with one item keyed at the last seq (`docs/designs/relay-protocol.md`,
"Log segments"). These read stored and returned segment items as the entries they hold.
"""
import hashlib
import json
import urllib.request

from record_runs import require


def segment_lines(blob):
    """The signed entries a log segment's bytes hold: one JSON entry and a newline each."""
    return [json.loads(line) for line in blob.split(b"\n") if line]


def stored_items(directory):
    """The items a reference relay stream directory holds, ascending by key: `(key, value)`. An
    item is a signed entry stored at its seq, or a segment item stored at its last seq."""
    found = [(int(path.stem), path) for path in directory.glob("*.json") if path.stem.isdigit()]
    return [(key, json.loads(path.read_text())) for key, path in sorted(found)]


def stored_entries(directory):
    """Every entry a reference relay stream directory holds, `{seq: entry}`. A segment item is read
    from its blob, `<relay data>/blobs/<sha256>`, three levels above `logs/<db>/<node>/<device>`."""
    blobs = directory.parents[3] / "blobs"
    entries = {}
    for key, value in stored_items(directory):
        segment = value.get("segment")
        if segment is None:
            entries[key] = value
            continue
        held = segment_lines((blobs / segment["sha256"]).read_bytes())
        seqs = [entry["writer_seq"] for entry in held]
        require(seqs == list(range(segment["first_seq"], segment["last_seq"] + 1)),
                f"the segment at {key} holds entries {seqs[:1]}..{seqs[-1:]}, not {segment['first_seq']}..{key}")
        entries.update((entry["writer_seq"], entry) for entry in held)
    return entries


def expand_read(entries):
    """The entries of a `log/read` answer, with each segment item replaced by the entries its blob
    holds, fetched from the item's `url` with its `headers`."""
    found = []
    for item in entries:
        segment = item.get("segment") if isinstance(item, dict) else None
        if segment is None:
            found.append(item)
            continue
        request = urllib.request.Request(segment["url"], headers=segment.get("headers") or {})
        with urllib.request.urlopen(request, timeout=60) as answer:
            blob = answer.read()
        require(hashlib.sha256(blob).hexdigest() == segment["sha256"] and len(blob) == segment["bytes"],
                f"the segment blob {segment['sha256'][:12]} came back with other bytes")
        found += segment_lines(blob)
    return found
