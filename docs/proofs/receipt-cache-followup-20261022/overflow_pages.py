"""Follow one known current directory key and its actual external-key locator pages."""

import hashlib
import struct
import uuid

PAGE = 16384


def number(value, offset, width=8):
    return int.from_bytes(value[offset:offset + width], "little")


def page(root, namespace, object_id, address):
    path = root / namespace / str(uuid.UUID(bytes=object_id)) / f"part-{address // 512:08x}"
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("A current native page does not name an exact regular part")
    with path.open("rb") as source:
        source.seek(address % 512 * PAGE)
        raw = source.read(PAGE)
    if (len(raw) != PAGE or raw[:8] != b"LDPAGE1\0" or raw[24:40] != object_id
            or number(raw, 40) != address
            or hashlib.sha256(raw[:80] + b"\0" * 32 + raw[112:]).digest() != raw[80:112]):
        raise RuntimeError("An actual current native page identity or checksum differs")
    return raw


def full_key(root, namespace, object_id, entry):
    packed = number(entry, 6, 2) >= 2
    length, inline = number(entry, 0, 4), number(entry, 4, 2)
    if length > 65535 or inline > length:
        raise RuntimeError("A current external key exceeds its finite format cap")
    start = 40 if packed else 8
    output = bytearray(entry[start:start + inline])
    address = number(entry, 168 if packed else 136)
    visited = set()
    while address:
        if address in visited or len(visited) >= 5:
            raise RuntimeError("A current key overflow chain cycles or exceeds five pages")
        visited.add(address)
        raw = page(root, namespace, object_id, address)
        used = number(raw, 48, 4)
        if raw[10] != 4 or used > PAGE - 128:
            raise RuntimeError("A current external key references an unsupported overflow page")
        output.extend(raw[128:128 + used])
        address = number(raw, 64)
    expected_digest = entry[8:40] if packed else entry[144:176]
    if len(output) != length or hashlib.sha256(output).digest() != expected_digest:
        raise RuntimeError("A current external key does not match its complete length and hash")
    return bytes(output), sorted(visited)


def locator(root, collection, group):
    current = (root / "CURRENT").read_bytes()
    object_id, address, height = current[48:64], number(current, 64), number(current, 72, 2)
    target = struct.pack("<H", len(collection.encode())) + collection.encode() + struct.pack("<H", group)
    if not 1 <= height <= 8:
        raise RuntimeError("The current directory height exceeds its format cap")
    for level in reversed(range(height)):
        raw = page(root, "directory", object_id, address)
        count, width = number(raw, 52, 2), number(raw, 54, 2)
        if raw[14] != level or width != (304 if level == 0 else 184):
            raise RuntimeError("The current directory path has an unsupported level or slot width")
        entries = [raw[128 + n * width:128 + (n + 1) * width] for n in range(count)]
        child = number(raw, 56)
        for entry in entries:
            key, _ = full_key(root, "directory", object_id, entry)
            if level == 0 and key == target:
                value = entry[176:304]
                if value[0] != 2 or number(value, 2, 2) != group:
                    raise RuntimeError("The named current directory value differs from its group")
                return value[8:24], number(value, 24), number(value, 32, 2)
            if level > 0 and key <= target:
                child = number(entry, 176)
        if level == 0:
            raise RuntimeError("The known generated collection/group is absent from the current directory")
        address = child
    raise RuntimeError("The current keyed directory path does not reach its known locator")


def inspect(root, collection, group, keys):
    object_id, address, height = locator(root, collection, group)
    if height < 2:
        raise RuntimeError("The generated external keys do not force a current parent page")
    wanted = {value.encode() for value in keys}
    stack, visited, rows, leaves = [(address, height - 1)], set(), [], set()
    while stack:
        address, level = stack.pop()
        if address in visited or len(visited) >= 256:
            raise RuntimeError("The selected locator path cycles or exceeds 256 pages")
        visited.add(address)
        raw = page(root, "locator", object_id, address)
        if raw[10] != (12 if level == 0 else 13) or raw[14] != level:
            raise RuntimeError("The selected current locator uses an unsupported page kind or level")
        count = number(raw, 52, 2)
        payload = raw[128:128 + number(raw, 48, 4)]
        if level > 0:
            stack.append((number(raw, 56), level - 1))
        for position in range(count):
            offset, length = struct.unpack_from("<HH", payload, position * 4)
            entry = payload[offset:offset + length]
            if number(entry, 6, 2) != 3 or number(entry, 0, 4) <= 16384:
                raise RuntimeError("The selected locator does not use the offered long external keys")
            key, chain = full_key(root, "locator", object_id, entry)
            if key not in wanted or not chain:
                raise RuntimeError("A current locator descriptor differs from the known generated keys")
            digest = hashlib.sha256(key).hexdigest()
            rows.append({"page": address, "level": level, "key_sha256": digest,
                         "key_bytes": len(key), "overflow_head": number(entry, 168), "overflow_pages": chain})
            if level > 0:
                stack.append((number(entry, 184), level - 1))
            else:
                leaves.add(key)
    if leaves != wanted:
        raise RuntimeError("The selected current leaves do not contain each known generated key")
    leaf_refs = {(row["key_sha256"], row["overflow_head"]) for row in rows if row["level"] == 0}
    parent_refs = {(row["key_sha256"], row["overflow_head"]) for row in rows if row["level"] > 0}
    shared = leaf_refs.intersection(parent_refs)
    if not shared:
        raise RuntimeError("The current parent and leaf do not share an actual external-key reference")
    return {"locator_object": str(uuid.UUID(bytes=object_id)), "locator_height": height,
            "current_locator_pages": len(visited), "external_leaf_keys": len(leaves),
            "external_parent_descriptors": len(parent_refs), "descriptors": rows,
            "shared_parent_leaf_overflow_references": [{"key_sha256": key, "overflow_head": head}
                                                       for key, head in sorted(shared)],
            "evidence": "Keyed current directory lookup and actual reachable locator/overflow pages"}
