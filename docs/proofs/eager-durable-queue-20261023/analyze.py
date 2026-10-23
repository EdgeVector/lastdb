#!/usr/bin/env python3
"""Summarize shared commits without attributing their cost to exclusive requests."""

from collections import Counter
import gzip
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parent


def analyze(name):
    output = ROOT / name
    path = output / 'writes.json'
    writes = json.loads(path.read_text() if path.exists() else gzip.decompress(path.with_suffix('.json.gz').read_bytes()))
    samples = writes['samples']
    first, last = min(s['start_unix_us'] for s in samples), max(s['end_unix_us'] for s in samples)
    marker = 'LASTDB_SNAPSHOT_TIMING '
    events = [json.loads(line.split(marker, 1)[1]) for line in
              gzip.decompress((output / 'daemon.txt.gz').read_bytes()).decode().splitlines() if marker in line]
    scopes = [e for e in events if e['kind'] == 'scope' and not e['background']
              and first <= e['started_us'] <= e['ended_us'] <= last]
    commits = [e for e in events if e['kind'] == 'commit' and e['ok']]
    rows, used = [], {}
    for scope in scopes:
        matching = [c for c in commits if c.get('first_sequence', c['sequence']) <= scope['sequence'] <= c['sequence']
                    and scope['started_us'] <= c['started_us'] <= c['ended_us'] <= scope['ended_us']]
        if len(matching) != 1:
            raise RuntimeError('scope needs exactly one durable commit that covers its sequence')
        commit = matching[0]
        used[commit['sequence']] = commit
        rows.append({'sequence': scope['sequence'], 'commit_sequence': commit['sequence'],
                     'cohort': commit['requests'], 'scope_ms': scope['total_us'] / 1000,
                     'phases_ms': {k.removesuffix('_us') + '_ms': v / 1000
                                   for k, v in scope['phases_us'].items()}})
    if len(rows) != len(samples) or len({r['sequence'] for r in rows}) != len(rows):
        raise RuntimeError('foreground scope count differs from client call count')
    summary = {'client': writes['summary'], 'scopes': len(rows), 'physical_commits': len(used),
               'cohort_histogram': dict(Counter(c['requests'] for c in used.values())),
               'mean_scope_ms': statistics.mean(r['scope_ms'] for r in rows),
               'mean_request_phase_ms': {k: statistics.mean(r['phases_ms'][k] for r in rows)
                                        for k in rows[0]['phases_ms']},
               'mean_physical_commit_ms': statistics.mean(c['total_us'] / 1000 for c in used.values()),
               'physical_commit_counts': dict(Counter({k: sum(c['counts'].get(k, 0) for c in used.values())
                                                       for k in next(iter(used.values()))['counts']})),
               'shared_flush_cost_is_not_exclusive_per_client': True}
    (output / 'timing-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({'name': name, **summary}, indent=2))


if __name__ == '__main__':
    for name in sys.argv[1:]:
        analyze(name)
