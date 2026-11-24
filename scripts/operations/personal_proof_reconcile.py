"""Reconcile what the relay holds with what the nodes say they put there, and what it weighs.

The nodes keep an account of what they sent and the relay accepted (`log-sync/<db>/cloud-usage.json`,
shown as `cloud_sync.databases.<db>.usage` in `/api/status`): entries and blobs, each counted once,
and what the trims they asked for released. This step reads the relay's own disk, which the nodes
cannot do, and holds the two against each other.

On the reference relay (`reconcile_reference`), for the one database of the proof:

- entries: every stream holds exactly the entries from its floor (or 1) to the position the nodes
  say the relay acknowledged, with no extra and no missing entry; every stored entry carries a valid
  signature and its `prev_hash` is the hash of the entry before it (`entry_audit`, which uses the
  `mutation_log` code that the nodes and the relay use); the hash of the last entry is the head that
  the relay's `log/streams` reports; the head of A's stream is the confirmed position A keeps on disk;
- blobs: every blob on the relay has the SHA-256 in its name; the relay holds one snapshot entry, and
  its blobs are exactly the manifest of that snapshot, the files the manifest names and the files that
  stored entries name, each with the named length. A blob that nothing names fails the proof: the
  sweep after each snapshot drops it;
- bytes: the length of the stored entries and the blobs, summed from the files of the relay, is what
  the nodes' accounts say: entries put less entries released, blobs put less blobs released (plus the
  blobs a node began to upload and was cut off before it noted the end of, when the relay holds them),
  and the status of each running node shows its own account.

On a deployed relay (`reconcile_deployed`), whose storage cannot be listed, the same questions go through
the routes: `log/streams` heads equal the positions of the nodes, `log/read` of each stream from its
floor returns a chain whose last hash is the head and whose count is what the nodes put less what trims
released, and `blob.get` of the files of the newest snapshot, and of every file a retained entry
names, returns the named length and SHA-256. That the relay holds no other blob is step 23
(`personal_proof_reclaim.py`), which asks `blob/list`.
"""
import hashlib
import json
import subprocess
import time
import urllib.error
import urllib.request

from personal_proof_deployed_trim import node_hex
from personal_proof_snapshot import stored_seqs, stream_dirs
from personal_proof_trim import relay_post
from record_runs import require

MEBIBYTE = 1024 * 1024
FIELDS = ("entries", "entry_bytes", "blobs", "blob_bytes", "released_entries", "released_bytes",
          "released_blobs", "released_blob_bytes")


def audit_binary(relay):
    return relay.binary.with_name("entry_audit")


def run_audit(binary, *arguments, text=None):
    done = subprocess.run([str(binary), *arguments], input=text, capture_output=True, text=True, check=False, timeout=600)
    require(done.returncode == 0, f"entry_audit {arguments[0]} failed: {done.stderr[-1500:]}")
    return json.loads(done.stdout)


def sum_usage(devices):
    """The accounts of every home of `devices`, added up, with the streams and the cut-off blobs they name."""
    total = dict.fromkeys(FIELDS, 0)
    streams, pending = {}, {}
    for device in devices:
        for account in device.all_usage():
            for field in FIELDS:
                total[field] += account.get(field, 0)
            for stream, through in account.get("streams", {}).items():
                streams[stream] = max(streams.get(stream, 0), through)
            pending.update(account.get("pending", {}))
    return total, streams, pending


def uploaded(device):
    """Everything the device accepted is on the relay. A restored device that wrote nothing has nothing to send."""
    state = device.spool_state()
    return state is None or state["head"] == 0 or device.synced_up()


def stable_usage(devices):
    """The sum of the accounts once every running device has uploaded all and the accounts stopped moving."""
    from personal_proof_nodes import wait  # noqa: PLC0415 imported here to keep the module graph a line

    running = [device for device in devices if device.node.process is not None]
    wait("every running device to upload", lambda: all(uploaded(device) for device in running), seconds=600)

    def settled():
        snapshots = [device.sync_status() for device in running]
        return not any(isinstance(sync, dict) and sync.get("snapshot", {}).get("state") == "in_progress"
                       for sync in snapshots)

    wait("no snapshot upload to be in progress", settled, seconds=600)
    first = sum_usage(devices)
    time.sleep(1.5)
    while True:
        second = sum_usage(devices)
        if second == first:
            return second
        first = second
        time.sleep(1.5)


def require_complete(devices):
    """Every account began before its device's first entry (a home of this build), so its totals are the whole."""
    for device in devices:
        for account in device.all_usage():
            if account["entries"]:
                require(account["complete"] is True and account["since_ms"] > 0,
                        f"{device.name}: the account says it began after earlier uploads: {account!r}")


def hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# Entries ----------------------------------------------------------------------------------

def check_entries(relay, database, audit, streams_through):
    """Every stream holds exactly floor+1 .. position, intact. Returns the count and length of the stored entries."""
    reports = [stream for stream in audit["streams"] if stream["database_id"] == database]
    held = {stream["device_id"] for stream in reports}
    require(held == set(streams_through), f"the relay holds streams {sorted(held)}, the nodes put on {sorted(streams_through)}")
    count = size = 0
    seen = {}
    for stream in reports:
        device_id = stream["device_id"]
        require(not stream["problems"], f"stream {device_id[:8]}: {stream['problems'][:3]}")
        directory = next(path for path in stream_dirs(relay, database) if path.name == device_id)
        floor = relay.stream_floor(database, device_id)
        seqs = stored_seqs(directory)
        position = streams_through[device_id]
        require(stream["floor"] == floor, f"stream {device_id[:8]}: the floor is {stream['floor']} in the audit, {floor} on disk")
        require(seqs == list(range(floor + 1, position + 1)),
                f"stream {device_id[:8]} holds {seqs[:3]}..{seqs[-3:]} ({len(seqs)} entries), expected {floor + 1} to {position}")
        require(stream["count"] == len(seqs), f"stream {device_id[:8]}: the audit read {stream['count']} entries, disk has {len(seqs)}")
        length = sum((directory / f"{seq}.json").stat().st_size for seq in seqs)
        count += len(seqs)
        size += length
        seen[device_id] = {"floor": floor, "head": position, "entries": len(seqs), "bytes": length,
                           "kinds": stream["kinds"], "last_hash": stream["last_hash"]}
    return count, size, seen


def check_heads(relay, database, a, seen):
    """The hash of the last entry is the head the relay reports, and A's confirmed position is A's head."""
    node = node_hex(a)
    status, answer = relay_post(relay, a, database, node, "/relay/v1/log/streams",
                                {"database_id": database, "writer_key": node})
    require(status == 200, f"log/streams: {status} {answer!r}")
    listed = {item["device_id"]: item for item in answer["streams"]}
    require(set(listed) == set(seen), f"log/streams lists {sorted(listed)}, the relay holds {sorted(seen)}")
    for device_id, item in listed.items():
        require(item["head_seq"] == seen[device_id]["head"],
                f"stream {device_id[:8]}: log/streams head {item['head_seq']}, the nodes' position {seen[device_id]['head']}")
        if seen[device_id]["entries"]:
            require(item["head_hash"] == seen[device_id]["last_hash"],
                    f"stream {device_id[:8]}: the head hash differs from the hash of the last stored entry")
    return {device_id: item["head_hash"] for device_id, item in listed.items()}


def check_positions(devices, streams_through):
    """What each running node keeps on disk as confirmed is the position of its own stream; the status shows its account."""
    for device in devices:
        if device.node.process is None:
            continue
        confirmed = (device.checkpoint() or {"writer_seq": 0})["writer_seq"]
        mine = streams_through.get(device.device_id(), 0)
        require(confirmed == mine, f"{device.name}: confirmed position {confirmed}, its account says it put {mine}")
        account = device.usage()
        if account is None:
            require(mine == 0, f"{device.name}: no account file, but its stream holds {mine} entries")
            continue
        shown = device.sync_status()["databases"][device.database]["usage"]
        require(all(shown[field] == account[field] for field in FIELDS),
                f"{device.name}: the status shows {shown!r}, the account file {account!r}")


# Blobs ------------------------------------------------------------------------------------

def check_blobs(relay, audit, pending):
    """Every blob has its digest, and the relay holds exactly what its one snapshot and its entries name."""
    directory = relay.data / "blobs"
    blobs = {path.name: path.stat().st_size for path in directory.iterdir() if path.is_file()}
    for name in blobs:
        require(hash_file(directory / name) == name, f"the blob {name[:12]} does not hash to its name")
    require(len(audit["snapshots"]) == 1, f"the relay holds {len(audit['snapshots'])} snapshot entries, expected the newest alone")
    entry = audit["snapshots"][0]
    named = {entry["manifest_sha256"]: entry["manifest_bytes"]}
    require(entry["manifest_sha256"] in blobs, "the relay lacks the manifest of its snapshot")
    for file in json.loads((directory / entry["manifest_sha256"]).read_text())["files"]:
        named[file["sha256"]] = file["bytes"]
    for digest, length in named.items():
        require(blobs.get(digest) == length, f"the relay holds {blobs.get(digest)} bytes of {digest[:12]}, the snapshot names {length}")
    sealed = set(audit["named_files"]) - set(named)
    require(sealed <= set(blobs), f"the relay lacks {len(sealed - set(blobs))} files that stored entries name")
    # What a sweep must not leave: a blob that neither the snapshot nor a stored entry names.
    left = sorted(set(blobs) - set(named) - sealed)
    require(not left, f"{len(left)} blobs on the relay ({sum(blobs[name] for name in left)} bytes) are named by nothing: {left[:3]}")
    cut_off = {digest: length for digest, length in pending.items() if digest in blobs}
    return {"blobs": len(blobs), "bytes": sum(blobs.values()), "named_by_the_snapshot": len(named),
            "named_by_entries": len(sealed), "unexplained": 0, "cut_off_on_the_relay": len(cut_off),
            "cut_off_bytes": sum(cut_off.values())}


# Reference relay --------------------------------------------------------------------------

def reconcile_reference(relay, a, devices, label):
    """Hold the disk of the reference relay against the accounts of every device of the database."""
    started = time.monotonic()
    database = a.database
    total, streams_through, pending = stable_usage(devices)
    require_complete(devices)
    audit = run_audit(audit_binary(relay), "disk", str(relay.data))
    count, size, seen = check_entries(relay, database, audit, streams_through)
    check_heads(relay, database, a, seen)
    check_positions(devices, streams_through)
    blobs = check_blobs(relay, audit, pending)
    expect_entries = total["entries"] - total["released_entries"]
    expect_entry_bytes = total["entry_bytes"] - total["released_bytes"]
    require(count == expect_entries, f"the relay holds {count} entries, the accounts say {expect_entries} "
            f"(put {total['entries']}, released {total['released_entries']})")
    require(size == expect_entry_bytes, f"the stored entries weigh {size} bytes, the accounts say {expect_entry_bytes} "
            f"(put {total['entry_bytes']}, released {total['released_bytes']})")
    expect_blobs = total["blobs"] - total["released_blobs"] + blobs["cut_off_on_the_relay"]
    expect_blob_bytes = total["blob_bytes"] - total["released_blob_bytes"] + blobs["cut_off_bytes"]
    require(blobs["blobs"] == expect_blobs, f"the relay holds {blobs['blobs']} blobs, the accounts say {expect_blobs} "
            f"(put {total['blobs']}, released {total['released_blobs']})")
    require(blobs["bytes"] == expect_blob_bytes, f"the blobs weigh {blobs['bytes']} bytes, the accounts say {expect_blob_bytes} "
            f"(put {total['blob_bytes']}, released {total['released_blob_bytes']})")
    seconds = round(time.monotonic() - started, 1)
    print(f"{label}: the relay holds {count} entries ({size} bytes) on {len(seen)} streams, each exactly from its floor "
          f"to the acknowledged head with an intact hash chain, and {blobs['blobs']} blobs ({blobs['bytes']} bytes), "
          f"every one named by the snapshot or an entry; the accounts of {len(devices)} devices say the same ({total['entries']} entries put, "
          f"{total['released_entries']} released, {total['blobs']} blobs put, {total['released_blobs']} released, "
          f"{blobs['cut_off_on_the_relay']} cut off and on the relay); {seconds}s", flush=True)
    return {"label": label, "seconds": seconds, "streams": seen, "entries": count, "entry_bytes": size,
            "blobs": blobs, "accounts": total, "cut_off_blobs_recorded": len(pending)}


# Deployed relay ---------------------------------------------------------------------------

def read_stream(relay, a, database, node, device_id, head):
    """The entries a deployed relay still holds of one stream, and its floor (found by reading: 410 at or below it)."""
    def readable(seq):
        status, answer = relay_post(relay, a, database, node, "/relay/v1/log/read",
                                    {"database_id": database, "writer_key": node, "device_id": device_id,
                                     "from_seq": seq, "limit": 1})
        require(status in (200, 410), f"log/read of {device_id[:8]} at {seq}: {status} {answer!r}")
        return status == 200
    low, high = 1, head + 1
    while low < high:
        middle = (low + high) // 2
        if readable(middle):
            high = middle
        else:
            low = middle + 1
    floor = low - 1
    entries, seq = [], floor + 1
    while seq <= head:
        status, answer = relay_post(relay, a, database, node, "/relay/v1/log/read",
                                    {"database_id": database, "writer_key": node, "device_id": device_id,
                                     "from_seq": seq, "limit": 90})
        require(status == 200 and answer["entries"], f"log/read of {device_id[:8]} at {seq}: {status} {answer!r}")
        entries += answer["entries"]
        seq += len(answer["entries"])
    return floor, entries


def get_blob(relay, a, database, node, digest, length, what):
    """`blob.get`, then the download: it must be `length` bytes with SHA-256 `digest`."""
    status, answer = relay_post(relay, a, database, node, "/relay/v1/blob/get",
                                {"database_id": database, "sha256": digest})
    require(status == 200, f"blob.get of {what} {digest[:12]}: {status} {answer!r}")
    request = urllib.request.Request(answer["url"], headers=answer.get("headers") or {})
    try:
        with urllib.request.urlopen(request, timeout=120) as body:
            data = body.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"download of {what} {digest[:12]}: HTTP {error.code}") from error
    require(len(data) == length, f"{what} {digest[:12]} is {len(data)} bytes, named as {length}")
    require(hashlib.sha256(data).hexdigest() == digest, f"{what} {digest[:12]} does not hash to its name")
    return data


def account_id(a):
    """The account the relay bills: the first 16 bytes of the SHA-256 of the node public key, hex."""
    return hashlib.sha256(bytes.fromhex(node_hex(a))).digest()[:16].hex()


def reconcile_deployed(relay, a, devices, binary):
    """The routes of a deployed relay against the accounts of every device. Returns what Exemem's audit needs."""
    started = time.monotonic()
    require(binary is not None, "the deployed relay run needs --entry-audit, the entry_audit binary")
    database, node = a.database, node_hex(a)
    total, streams_through, pending = stable_usage(devices)
    require_complete(devices)
    status, answer = relay_post(relay, a, database, node, "/relay/v1/log/streams",
                                {"database_id": database, "writer_key": node})
    require(status == 200, f"log/streams: {status} {answer!r}")
    listed = {item["device_id"]: item for item in answer["streams"]}
    heads = {device_id: item["head_seq"] for device_id, item in listed.items()}
    require(heads == streams_through, f"log/streams heads {heads!r}, the nodes' positions {streams_through!r}")
    for device in devices:
        require((device.checkpoint() or {"writer_seq": 0})["writer_seq"] == heads.get(device.device_id(), 0),
                f"{device.name}: its confirmed position is not the head the relay reports")
    streams, held_entries, held_bytes, files, snapshots = {}, 0, 0, {}, []
    for device_id, head in sorted(heads.items()):
        floor, entries = read_stream(relay, a, database, node, device_id, head)
        require(len(entries) == head - floor, f"stream {device_id[:8]}: read {len(entries)} entries, expected {head - floor}")
        checked = run_audit(binary, "chain", text=json.dumps({"floor": None, "entries": entries}))
        require(not checked["problems"], f"stream {device_id[:8]}: {checked['problems'][:3]}")
        if entries:
            require(checked["last_hash"] == listed[device_id]["head_hash"],
                    f"stream {device_id[:8]}: the last entry hashes to {checked['last_hash']}, log/streams says "
                    f"{listed[device_id]['head_hash']}")
        length = sum(len(json.dumps(entry, separators=(",", ":"), ensure_ascii=False).encode()) for entry in entries)
        held_entries += len(entries)
        held_bytes += length
        streams[device_id] = {"floor": floor, "head": head, "entries": len(entries), "read_back_bytes": length,
                              "head_hash": listed[device_id]["head_hash"]}
        for entry in entries:
            if entry.get("file"):
                files[entry["file"]["sha256"]] = "a file of a retained entry"
            if entry["kind"] == "snapshot":
                snapshots.append(entry)
    expect_entries = total["entries"] - total["released_entries"]
    require(held_entries == expect_entries, f"the relay returns {held_entries} entries, the accounts say {expect_entries} "
            f"(put {total['entries']}, released {total['released_entries']})")
    # The files of the newest snapshot, and every file a retained entry names, downloaded and hashed.
    newest = max(snapshots, key=lambda entry: entry["writer_seq"])
    manifest_bytes = get_blob(relay, a, database, node, newest["manifest_sha256"], newest["manifest_bytes"], "the manifest")
    listed_files = json.loads(manifest_bytes)["files"]
    downloaded = len(manifest_bytes)
    for file in listed_files:
        downloaded += len(get_blob(relay, a, database, node, file["sha256"], file["bytes"], "a file of the newest snapshot"))
    for digest in files:
        if digest not in {file["sha256"] for file in listed_files}:
            # The sealed length is not in the entry: download it and hash it.
            status, answer = relay_post(relay, a, database, node, "/relay/v1/blob/get",
                                        {"database_id": database, "sha256": digest})
            require(status == 200 and answer.get("bytes"), f"blob.get of a file of a retained entry: {status} {answer!r}")
            downloaded += len(get_blob(relay, a, database, node, digest, answer["bytes"], "a file of a retained entry"))
    seconds = round(time.monotonic() - started, 1)
    print(f"reconcile: log/streams heads equal the positions of {len(devices)} devices on {len(heads)} streams; each stream "
          f"read from its floor is a chain of {held_entries} entries whose last hashes equal the heads; the manifest of snapshot "
          f"{newest['writer_seq']} and its {len(listed_files)} files and {len(files)} entry files downloaded ({downloaded} bytes) "
          f"match their lengths and SHA-256; the nodes' accounts: {total['entries']} entries ({total['entry_bytes']} bytes) put, "
          f"{total['released_entries']} released ({total['released_bytes']} bytes), {total['blobs']} blobs ({total['blob_bytes']} bytes) "
          f"put, {total['released_blobs']} released ({total['released_blob_bytes']} bytes); {seconds}s", flush=True)
    return {"database_id": database, "publisher_node_key": node, "account_id": account_id(a), "streams": streams,
            "accounts": total, "cut_off_blobs": len(pending), "entries_on_the_relay": held_entries,
            "entry_bytes_read_back": held_bytes, "entry_bytes_match": held_bytes == total["entry_bytes"] - total["released_bytes"],
            "newest_snapshot_seq": newest["writer_seq"], "downloaded_files": len(listed_files) + len(files) + 1,
            "downloaded_bytes": downloaded, "seconds": seconds}
