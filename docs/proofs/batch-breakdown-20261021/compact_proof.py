#!/usr/bin/env python3
"""Retain exact large derived files as compressed evidence and label byte counters."""
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'atomic-change-feed-20261021'))
import preserve


def main():
    names = ('existing_result.json', 'profile_result_v1.json')
    archives = [preserve.archive_exact(HERE / name,
        preserve.slot(HERE / 'portable-analysis', name)) for name in names]
    preserve.save(HERE / 'analysis-archives.json', {'archives': archives,
        'all_gzip_streams_exactly_verified': True,
        'scope': 'Exact complete derived JSON files; originals remain locally available. No value, ID or timestamp is changed.'})
    source = HERE / 'profile_result.json'
    result = json.loads(source.read_text())
    rows = result['requests']
    groups = []
    for geometry in ('hash', 'hashrange'):
        for batch in (1, 32, 128):
            selected = [row for row in rows if row['geometry'] == geometry and row['batch'] == batch]
            written = sum(row['cpu_raw_delta']['writes'] for row in selected)
            groups.append({'geometry': geometry, 'records_per_call': batch,
                'calls': len(selected), 'node_write_bytes_total': written,
                'node_write_bytes_per_call': written / len(selected),
                'node_write_MiB_per_call': written / len(selected) / 1048576})
    preserve.save(HERE / 'write-byte-observations.json', {
        'source': {'path': str(source), **preserve.parts.identity(source)},
        'groups': groups, 'calls': len(rows),
        'logical_changes': sum(row['batch'] for row in rows),
        'node_write_bytes_total': sum(row['cpu_raw_delta']['writes'] for row in rows),
        'body_value_bytes_total': sum(row['batch'] * result['matrix']['body_bytes'] for row in rows),
        'unit': 'MiB is 1048576 bytes. Values are deltas of the node-process write-byte counter.',
        'scope': 'All node threads inside resource-counter brackets slightly wider than HTTP; metadata, logs and background node work are included.',
        'limit': 'The payload figure counts only body fields. This is not an exclusive client I/O or pure payload amplification ratio.'})
    print(json.dumps({'exact_analysis_archives': len(archives), 'write_byte_groups': len(groups)}))


if __name__ == '__main__':
    main()
