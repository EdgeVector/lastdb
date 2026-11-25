"""What the deployed relay answers after a trim: a late repeat of an append, and a read below the floor.

The node calls `log/trim` after its second snapshot. The proof reads two early entries of A before that,
and afterwards signs requests as A: the repeat of those entries must be answered as a duplicate and a
read there must be 410, and the contiguous head that `log/streams` reports must not move. What it cannot
do on a deployed relay: list the stored entries, read the floors, or read the quota counter.
"""
from personal_proof_nodes import require
from personal_proof_segments import expand_read
from personal_proof_trim import Ed25519PrivateKey, relay_post

EARLY = (3, 7)


def node_hex(device):
    key = Ed25519PrivateKey.from_private_bytes((device.node.home / "identity.key").read_bytes())
    return key.public_key().public_bytes_raw().hex()


def before_trim(relay, a):
    """Early entries of A as the relay holds them now, and the head of A's stream."""
    node = node_hex(a)
    found, seq = {}, 1
    while seq <= max(EARLY):
        status, answer = relay_post(relay, a, a.database, node, "/relay/v1/log/read",
                                    {"database_id": a.database, "writer_key": node, "device_id": a.device_id(),
                                     "from_seq": seq, "limit": 10})
        require(status == 200 and answer["entries"], f"early entries of A at {seq}: {status}")
        # A segment item is read as the entries its blob holds.
        found.update((entry["writer_seq"], entry) for entry in expand_read(answer["entries"]))
        seq = max(found) + 1
    return {"node": node, "head": head_of(relay, a, node), "entries": {seq: found[seq] for seq in EARLY}}


def head_of(relay, a, node):
    status, answer = relay_post(relay, a, a.database, node, "/relay/v1/log/streams",
                                {"database_id": a.database, "writer_key": node})
    require(status == 200, f"log/streams: {status} {answer!r}")
    return {item["device_id"]: item["head_seq"] for item in answer["streams"]}[a.device_id()]


def after_trim(relay, a, kept):
    """The repeat is a duplicate, the read is 410, and the head did not move."""
    node = kept["node"]
    seen = []
    for seq, entry in kept["entries"].items():
        status, answer = relay_post(relay, a, a.database, node, "/relay/v1/log/append",
                                    {"database_id": a.database, "entries": [entry]})
        require(status == 200 and answer.get("duplicate") is True, f"repeat of entry {seq}: {status} {answer!r}")
        status, answer = relay_post(relay, a, a.database, node, "/relay/v1/log/read",
                                    {"database_id": a.database, "writer_key": node, "device_id": a.device_id(),
                                     "from_seq": seq, "limit": 1})
        seen.append((seq, status, answer.get("code")))
        require(status == 410 and answer.get("code") == "trimmed", f"read at entry {seq}: {status} {answer!r}")
    head = head_of(relay, a, node)
    require(head == kept["head"], f"the head of A's stream moved from {kept['head']} to {head}")
    print(f"   after the trim on the deployed relay: repeats of entries {list(kept['entries'])} of A were answered "
          f"as duplicates, reads there were {seen}, and the head stayed {head}", flush=True)
    return {"repeated": list(kept["entries"]), "reads": seen, "head": head}
