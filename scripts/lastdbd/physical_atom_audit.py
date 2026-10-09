#!/usr/bin/env python3
"""Audit exact atom IDs in a bounded, stopped plain LastStore collection."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import struct
import time


def atom_id(key):
    """Accept native kind-partition and legacy flat/partition atom keys."""
    for prefix in (b'atom\0', b'atom:'):
        if key.startswith(prefix):
            candidate = key[len(prefix):].rsplit(b'\0', 1)[-1]
            if re.fullmatch(b'[0-9a-f]{64}', candidate):
                return candidate.decode()
    # Report scoped legacy bodies too: a namespace is not an absence claim.
    for marker in (b':atom:', b':atom\0'):
        offset = key.find(marker)
        if offset >= 0:
            return atom_id(key[offset + 1:])
    return None


def records(data, targets):
    """Parse every record, including nonmatching records and trailing bytes."""
    result, position = [], 0

    def take(length):
        nonlocal position
        if position + length > len(data):
            raise ValueError('truncated_plain_segment')
        value = data[position:position + length]
        position += length
        return value

    while position < len(data):
        operation = take(1)[0]
        key = take(struct.unpack('<H', take(2))[0])
        if operation not in (1, 2):
            raise ValueError('unsupported_plain_segment')
        length = struct.unpack('<I', take(4))[0] if operation == 1 else 0
        take(length)
        identity = atom_id(key)
        if identity in targets:
            result.append({'atom_uuid': identity,
                           'op': 'put' if operation == 1 else 'delete',
                           'body_bytes': length, 'key_hex': key.hex()})
    return result


def audit(root, deleted, controls):
    """Return physical Put absence plus a required retained-Put control."""
    deleted, controls = set(deleted), set(controls)
    if (not deleted or not controls or deleted & controls or
            len(deleted | controls) > 128 or
            any(not re.fullmatch(r'[0-9a-f]{64}', item)
                for item in deleted | controls)):
        raise ValueError('invalid_atom_selection')
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('invalid_atoms_root')
    root = root.resolve()
    started = time.monotonic()
    # Fixed depth: collection → shard → optional group/chunks → segment.
    files = sorted(set(root.glob('*/g/*/*.seg')) |
                   set(root.glob('*/g/*/chunks/*.seg')) |
                   set(root.glob('*/chunks/*.seg')) |
                   set(root.glob('*/*.seg')))
    if not files or len(files) > 100000:
        raise ValueError('segment_count')
    result = {'schema': 'lastdb.physical-atom-audit.v2', 'segments': 0,
              'bytes': 0, 'files': [], 'matches': [],
              'deleted': sorted(deleted), 'controls': sorted(controls)}
    for path in files:
        if time.monotonic() - started > 300:
            raise ValueError('time_budget')
        if any(p.is_symlink() for p in (path, *path.parents) if p != root.parent):
            raise ValueError('symlink_segment_path')
        before = path.stat()
        if before.st_size > 256 * 1024 * 1024:
            raise ValueError('file_budget')
        data = path.read_bytes()
        after = path.stat()
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) !=
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            raise ValueError('segment_changed_during_audit')
        result['bytes'] += len(data)
        if result['bytes'] > 100 * 1024 ** 3:
            raise ValueError('byte_budget')
        hits = records(data, deleted | controls)
        evidence = {'path': str(path.relative_to(root)), 'bytes': len(data),
                    'sha256': hashlib.sha256(data).hexdigest()}
        result['files'].append(evidence)
        if hits:
            result['matches'].append(dict(evidence, records=hits))
        result['segments'] += 1
    puts = {record['atom_uuid'] for match in result['matches']
            for record in match['records'] if record['op'] == 'put'}
    result.update(deleted_body_puts_absent=not bool(deleted & puts),
                  retained_body_puts_present=controls <= puts,
                  elapsed_seconds=round(time.monotonic() - started, 3))
    result['ok'] = (result['deleted_body_puts_absent'] and
                    result['retained_body_puts_present'])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--atoms-root', required=True)
    parser.add_argument('--deleted', action='append', required=True)
    parser.add_argument('--control', action='append', required=True)
    args = parser.parse_args()
    result = audit(args.atoms_root, args.deleted, args.control)
    print(json.dumps(result, indent=2))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
