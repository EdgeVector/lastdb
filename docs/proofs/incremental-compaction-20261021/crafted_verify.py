#!/usr/bin/env python3
"""Read every key of a crafted home back and compare it with the model that `crafted_home.py` wrote.

  LDBIN=<bin dir> crafted_verify.py HOME EXPECTED.json

`lastdb-disk-home read` answers each key from the home as a user read would: a live key gives its body
(compared by SHA-256) and a deleted key gives nothing. A pack must keep both, so this check also tells a
tombstone from an empty body, which the record oracle cannot. Exits non-zero on any difference.
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

home = ldc.refuse_primary(sys.argv[1])
model = json.load(open(sys.argv[2]))
bad = 0
checked = 0
for start in range(0, len(model), 100):
    chunk = model[start:start + 100]
    request = {'coordinates': [{'collection': c, 'key': k} for c, k, _ in chunk],
               'budget': {'pages': 4096, 'blocks': 16384, 'bytes': 256 * 1024 * 1024, 'keys': 10000}}
    with tempfile.NamedTemporaryFile('w', suffix='.json', prefix='s1s2.read.', delete=False,
                                     dir=os.environ.get('TMPDIR') or '/tmp') as handle:
        json.dump(request, handle)
    try:
        done = subprocess.run([str(ldc.BIN / 'lastdb-disk-home'), 'read', '--home', str(ldc.pack_root(home)),
                               '--input', handle.name], capture_output=True, text=True)
    finally:
        os.unlink(handle.name)
    if done.returncode != 0:
        print('read failed:', done.stderr[-300:])
        sys.exit(1)
    results = json.loads(done.stdout)['results']
    for (collection, key, want), result in zip(chunk, results):
        checked += 1
        got = result.get('body_hex')
        digest = hashlib.sha256(bytes.fromhex(got)).hexdigest() if got is not None else None
        if digest != want:
            bad += 1
            if bad <= 5:
                print('DIFFERENT', collection, key[:40], want, digest)
print(json.dumps({'ev': 'crafted-verify', 'checked': checked, 'different': bad}))
sys.exit(1 if bad else 0)
