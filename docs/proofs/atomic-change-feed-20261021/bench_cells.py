"""Fixed generated write cells with complete value and notice checks."""
import base64
import hashlib
import json
import math
import statistics
import time

from bench_support import logical, measure
import bench_notices

FAILED_BODY_LIMIT = 1024 * 1024


def raw_certificate(raw, retain_prefix=False):
    result = {'response_body_sha256': hashlib.sha256(raw).hexdigest(), 'response_body_bytes': len(raw)}
    if retain_prefix:
        result.update(response_body_base64=base64.b64encode(raw[:FAILED_BODY_LIMIT]).decode(),
                      response_body_truncated=len(raw) > FAILED_BODY_LIMIT,
                      retained_response_body_limit=FAILED_BODY_LIMIT)
    return result


def matrix(samples):
    first, cells = 0, []
    for batch in (1, 32, 128):
        cells.append({'first': first, 'batch': batch, 'writers': 1,
                      'body_bytes': 1024, 'requests': samples})
        first += batch * samples
    return cells, first


def payloads(spec, cell, revision):
    requests = [{'mutations': [logical.mutation(spec, number, revision,
        action='create' if revision == 0 else 'update', durability='durable', full=True,
        body_bytes=cell['body_bytes']) for number in range(first, first + cell['batch'])]}
        for first in range(cell['first'], cell['first'] + cell['batch'] * cell['requests'], cell['batch'])]
    return requests, [json.dumps(value, separators=(',', ':')).encode() for value in requests]


def send(node, raw, ordinal):
    connection = measure.profile.UnixConnection(node.socket)
    identity = f'atomic-notice-{node.run.identity}-{ordinal}'
    headers = {'Content-Type': 'application/json', 'X-LastDB-Client': 'atomic-notice-benchmark',
               'X-LastDB-Request-Id': identity}
    started, response = time.perf_counter_ns(), None
    try:
        connection.request('POST', '/api/mutations/batch', raw, headers)
        response = connection.getresponse()
        body = response.read()
        finished = time.perf_counter_ns()
        return {'request_id': identity, 'http_status': response.status, 'start_ns': started,
                'finish_ns': finished, 'elapsed_ns': finished - started, 'raw': body}
    except BaseException as error:
        failed = time.perf_counter_ns()
        partial = getattr(error, 'partial', b'')
        partial = partial if isinstance(partial, bytes) else b''
        node.remember(operation='timed-transport-error', request_id=identity,
            http_status=None if response is None else response.status,
            start_ns=started, error_ns=failed, elapsed_ns=failed - started,
            error=f'{type(error).__name__}: {error}', partial_bytes_from_exception=True,
            **raw_certificate(partial, retain_prefix=True))
        raise
    finally:
        connection.close()


def capture_responses(node, requests, responses, cell, failed=False):
    """All complete replies reach memory receipts before any response check can fail."""
    for request, response in zip(requests, responses):
        raw = response.pop('raw')
        stamp = raw_certificate(raw, retain_prefix=failed)
        error = None
        try:
            response['result'] = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as problem:
            response['result'], error = None, f'{type(problem).__name__}: {problem}'
            stamp = raw_certificate(raw, retain_prefix=True)
        node.remember(operation='measured-write', **cell, request=request, **response,
                      decode_error=error, cell_transport_failed=failed, **stamp)


def send_all(node, requests, bodies, cell):
    responses, failed = [], False
    try:
        for index, body in enumerate(bodies):
            responses.append(send(node, body, node.sequence + index + 1))
        return responses
    except BaseException:
        failed = True
        capture_responses(node, requests, responses, cell, failed=True)
        raise
    finally:
        node.sequence += len(responses) + int(failed)


def decode(node, requests, responses, cell):
    capture_responses(node, requests, responses, cell)
    wanted = {}
    for request, response in zip(requests, responses):
        result, mutations = response['result'], request['mutations']
        if not isinstance(result, dict):
            raise RuntimeError('A complete timed response is not a JSON object')
        identities = result.get('mutation_ids', [])
        if (response['http_status'] != 200 or result.get('count') != cell['batch']
                or type(result.get('count')) is not int
                or result.get('durability') != 'durable' or not isinstance(identities, list)
                or len(identities) != len(mutations)
                or any(not isinstance(identity, str) or not identity for identity in identities)
                or len(set(identities)) != len(identities)):
            raise RuntimeError('The timed write lacks complete unique IDs and a durable ack')
        for identity, mutation in zip(identities, mutations):
            if identity in wanted:
                raise RuntimeError('Two timed calls returned the same mutation ID')
            wanted[identity] = {'schema_name': mutation['schema'],
                                'key': {name: mutation['key_value'].get(name) for name in ('hash', 'range')},
                                'operation': mutation['mutation_type']}
    if set(wanted) & node.run.mutation_ids:
        raise RuntimeError('A later timed cell reuses an earlier mutation ID')
    node.run.mutation_ids.update(wanted)
    return wanted


def barrier_delta(before, after):
    left, right = before.get('file_barriers'), after.get('file_barriers')
    if not isinstance(left, dict) or not isinstance(right, dict) or left.get('scope') != right.get('scope'):
        raise RuntimeError('Both benchmark binaries must expose identical barrier counter scope')
    delta = {'scope': left['scope']}
    for name, value in left.items():
        if name == 'scope':
            continue
        if not isinstance(value, int) or not isinstance(right.get(name), int) or right[name] < value:
            raise RuntimeError('A process file barrier counter regressed or disappeared')
        delta[name] = right[name] - value
    return delta


def read_all(node, spec, first, records, revision, phase):
    width = 64 if spec.kind == 'hash' else 256
    for start in range(first, first + records, width):
        expected = {number: revision for number in range(start, min(start + width, first + records))}
        logical.read(node, spec, expected, phase, zero=False, body_bytes=1024)
    if spec.kind == 'hashrange':
        expected = {number: revision for number in range(first + records - min(32, records), first + records)}
        logical.read(node, spec, expected, phase + '-bounded', bounded=True, body_bytes=1024)
    node.remember(operation='all-key-readback', schema=spec.schema, geometry=spec.kind,
        first=first, records=records, revision=revision, phase=phase,
        all_fields_and_membership_match=True, expected_fields=list(logical.FIELDS))


def timed_cell(node, spec, cell, revision, observer, cursor, candidate, quiet_timeout):
    requests, bodies = payloads(spec, cell, revision)
    label = f'{node.variant}/{spec.kind}/batch-{cell["batch"]}/revision-{revision}'
    with observer.window(label, timeout_seconds=quiet_timeout) as quiet:
        before = node.status('before-timed-writes')
        captured = dict(cell, revision=revision, geometry=spec.kind, schema=spec.schema)
        responses = send_all(node, requests, bodies, captured)
        wanted = decode(node, requests, responses, captured)
        after = node.status('after-timed-writes-before-notice-observation')
        cursor, notices = bench_notices.observe(node, cursor, wanted, candidate,
                                               responses[-1]['finish_ns'])
    elapsed = responses[-1]['finish_ns'] - responses[0]['start_ns']
    times = sorted(row['elapsed_ns'] / 1e6 for row in responses)
    summary = dict(cell, geometry=spec.kind, schema=spec.schema, revision=revision,
        records=cell['batch'] * cell['requests'], wall_ns=elapsed,
        records_per_second=cell['batch'] * cell['requests'] * 1e9 / elapsed,
        median_ms=statistics.median(times), p95_ms=times[math.ceil(.95 * len(times)) - 1],
        request_bytes=sum(map(len, bodies)), before=before, after=after, notices=notices,
        barrier_delta_until_post_write_status=barrier_delta(before, after),
        barrier_delta_until_notice_drain=barrier_delta(before, notices['after_notice_drain']),
        quiet_certificate=quiet)
    node.remember(operation='write-summary', **summary)
    read_all(node, spec, cell['first'], summary['records'], revision, 'all-values-after-timed-cell')
    return summary, cursor
