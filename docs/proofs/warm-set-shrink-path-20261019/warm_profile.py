#!/usr/bin/env python3
"""Seed + read 3,000 small records on a MallocStackLogging node, then run
malloc_history on it while the warm set is full. Stops the node and deletes
its temp home afterward."""
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import warm_ab  # noqa: E402

binary, out = Path(sys.argv[1]).resolve(), Path(sys.argv[2])
root = Path(tempfile.mkdtemp(prefix='wpr.', dir=os.environ.get('TMPDIR', '/tmp')))
node = warm_ab.Node(binary, root, None)
expected = {n: {'title': f'title-{n}', 'body': warm_ab.body(n, 200)} for n in range(3000)}
try:
    node.start()
    node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
        'source': 'system_seed', 'name': warm_ab.SCHEMA, 'descriptive_name': 'Warm AB Probe',
        'schema_type': 'HashRange', 'key': {'hash_field': 'bucket', 'range_field': 'id'},
        'fields': ['bucket', 'id', 'title', 'body'],
        'field_descriptions': {'bucket': 'generated hash', 'id': 'generated range',
                               'title': 'generated title', 'body': 'generated body'}}})
    for first in range(0, 3000, 64):
        muts = [{'type': 'mutation', 'schema': warm_ab.SCHEMA, 'mutation_type': 'create',
                 'key_value': {'hash': 'generated', 'range': warm_ab.ident(n)},
                 'fields_and_values': dict(expected[n], bucket='generated', id=warm_ab.ident(n)),
                 'durability': 'durable'} for n in range(first, min(first + 64, 3000))]
        node.call('/api/mutations/batch', {'mutations': muts})
    node.call('/api/db/flush', {})
    node.stop()
    os.environ['MallocStackLogging'] = '1'
    node.start()
    for first in range(0, 3000, 256):
        nums = list(range(first, min(first + 256, 3000)))
        r = node.call('/api/query', {'schema_name': warm_ab.SCHEMA, 'fields': warm_ab.FIELDS,
                                     'filter': {'HashRangeKeys': [['generated', warm_ab.ident(n)] for n in nums]},
                                     'limit': len(nums)})
        assert len(r['results']) == len(nums)
    status = node.call('/api/db/native-status')
    print(warm_ab.warm_paths(status), warm_ab.find_key(status, 'core_warm_bytes'), flush=True)
    with out.open('w') as f:
        subprocess.run(['malloc_history', str(node.proc.pid), '-callTree', '-invert'],
                       stdout=f, stderr=subprocess.STDOUT, timeout=1200)
finally:
    node.stop()
    shutil.rmtree(root)
