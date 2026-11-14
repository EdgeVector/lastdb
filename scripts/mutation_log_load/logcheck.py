"""Check one tier's log: no losses, no duplicates, no delivery gaps.

``accepted`` maps a device id to the writer sequences that append returned
successfully. Each device's sequences must be exactly ``1..N``. Delivery
sequences must be exactly ``1..len(entries)`` with no repeats.
"""

from routes import RouteError, decimal, require_object


ENTRY_KEYS = (
    "delivery_seq", "writer_device", "writer_seq",
)


def collect_pages(cursor, fetch):
    """Read list pages until ``next`` reaches the head cursor."""
    cursor = cursor or 0
    after = 0
    found = []
    while True:
        page = fetch(after)
        found.extend(page["entries"])
        if cursor == 0:
            return found
        nxt = page.get("next")
        if nxt is None:
            raise RouteError("log")
        nxt = decimal(nxt)
        if nxt >= cursor:
            return found
        if nxt <= after:
            raise RouteError("log")
        after = nxt


def unresolved_writes_absent(entries, unresolved):
    """An exhausted conflict cannot leave a partially accepted entry."""
    present = {(entry["writer_device"], decimal(entry["writer_seq"])) for entry in entries}
    return all(key not in present for key in unresolved)


def verify_log(entries, accepted):
    if not isinstance(entries, list) or not isinstance(accepted, dict):
        raise RouteError("log")
    seen = _seen(entries)
    _delivery(seen)
    _writers(seen, accepted)


def _seen(entries):
    found = {}
    deliveries = set()
    for entry in entries:
        body = require_object(entry, ENTRY_KEYS)
        device = body["writer_device"]
        if not isinstance(device, str) or not device:
            raise RouteError("log")
        key = (device, decimal(body["writer_seq"]))
        delivery = decimal(body["delivery_seq"])
        if key in found or delivery in deliveries:
            raise RouteError("duplicate")
        found[key] = delivery
        deliveries.add(delivery)
    return found


def _delivery(seen):
    ordered = sorted(seen.values())
    if ordered != list(range(1, len(ordered) + 1)):
        raise RouteError("gap")


def _writers(seen, accepted):
    for device, seqs in accepted.items():
        got = sorted(seq for (found, seq) in seen if found == device)
        expect = list(range(1, len(seqs) + 1))
        if sorted(seqs) != expect or got != expect:
            raise RouteError("writer")
    if len(seen) != sum(len(seqs) for seqs in accepted.values()):
        raise RouteError("lost")
