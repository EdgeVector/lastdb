#!/usr/bin/env python3
"""Parts for each request on a never-packed synthetic home.

  LDBIN=<bin dir> parts_per_request.py [--scenarios NAMES]

One fresh home for each scenario, automatic maintenance off. A scenario sends REQUESTS requests of OPS
`update` operations. After the node is quiet, the script reads the receipt of CURRENT before and after the
requests and prints, for each scenario, the added parts by kind for each request, the batches for each
request (the added journal data parts), and the added parts for each batch.
A batch always adds one journal data part, one journal map part and one directory part. The locator
part count is the number that the one-object change brings to one.
"""
import argparse
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ldc  # noqa: E402

SCENARIOS = {
    'durable-1': dict(requests=40, ops=1, durable=True, clients=1, spread=1),
    'default-1': dict(requests=100, ops=1, durable=False, clients=1, spread=1),
    'default-10-spread-10': dict(requests=60, ops=10, durable=False, clients=1, spread=10),
    'default-100-one-group': dict(requests=30, ops=100, durable=False, clients=1, spread=1),
    'default-100-64-groups': dict(requests=30, ops=100, durable=False, clients=1, spread=64),
    'default-10-8-clients': dict(requests=40, ops=10, durable=False, clients=8, spread=10),
    'durable-10-8-clients': dict(requests=20, ops=10, durable=True, clients=8, spread=10),
}

ap = argparse.ArgumentParser()
ap.add_argument('--scenarios', default=','.join(SCENARIOS))
ap.add_argument('--label', default='')
args = ap.parse_args()


def run(label, requests, ops, durable, clients, spread):
    home = ldc.new_home('parts')
    node = ldc.Node(home, {'LASTDB_AUTO_MAINTENANCE': '0'})
    node.start()
    ldc.seed(node, 'Cap')
    ldc.mutate(node, [ldc.put('Cap', 'b00', 'seed', 'x', durable=True)])
    time.sleep(3)
    _, deps0, kinds0, entries0 = ldc.receipt(home)

    def worker(client):
        for i in range(requests):
            muts = [ldc.put('Cap', 'b%02d' % ((client * 7 + i * ops + j) % spread), f'{client}-{i}-{j}',
                            'v' * 100, durable=durable) for j in range(ops)]
            ldc.mutate(node, muts)

    began = time.time()
    threads = [threading.Thread(target=worker, args=(c,)) for c in range(clients)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    seconds = time.time() - began
    time.sleep(3)
    _, deps1, kinds1, entries1 = ldc.receipt(home)
    node.stop()
    total = requests * clients
    delta = {k: kinds1.get(k, 0) - kinds0.get(k, 0) for k in kinds1}
    batches = delta.get('journal-data', 0)
    objects = len({e[1] for e in entries1 if e[0] == 'locator'})
    print(json.dumps({'label': args.label, 'scenario': label, 'requests': total, 'ops': ops, 'durable': durable,
                      'clients': clients, 'groups': spread,
                      'parts_per_request': round((deps1 - deps0) / total, 2),
                      'by_kind_per_request': {k: round(v / total, 2) for k, v in sorted(delta.items())},
                      'batches_per_request': round(batches / total, 2),
                      'parts_per_batch': round((deps1 - deps0) / batches, 2) if batches else None,
                      'locator_parts_per_batch': round(delta.get('locator', 0) / batches, 2) if batches else None,
                      'locator_objects_after': objects, 'secs': round(seconds, 1)}), flush=True)
    ldc.remove_owned(home)


for name in args.scenarios.split(','):
    run(name, **SCENARIOS[name])
