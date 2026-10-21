"""One optional candidate-only forensic status observation before normal stop."""
import time


def capture(node, calls):
    path = '/api/status?recent=1'
    wanted = {call['request_id'] for call in calls}
    try:
        response, elapsed, _, request_id = node.call(path)
        received = time.time_ns() // 1000000
        payload = response.get('data', response) if isinstance(response, dict) else {}
        status = payload.get('status', {}) if isinstance(payload, dict) else {}
        status = status if isinstance(status, dict) else {}
        operations = status.get('request_ops')
        recent = operations.get('recent', []) if isinstance(operations, dict) else []
        recent = recent if isinstance(recent, list) else []
        available_ids = {row['request_id'] for row in recent
                         if isinstance(row, dict) and isinstance(row.get('request_id'), str)}
        sampled = status.get('sampled_at')
        node.remember(operation='candidate-request-ops-observation', path=path,
            status_request_id=request_id, elapsed_nanos=elapsed,
            request_ops_available=isinstance(operations, dict),
            forensic_recent_available=bool(recent),
            # This field deliberately bypasses BufferedRun's request/result certificates.
            request_ops_snapshot=operations, status_sampled_at=sampled,
            status_process_start_ts=status.get('process_start_ts'),
            client_receive_unix_ms=received,
            snapshot_age_at_receive_ms=received - sampled * 1000 if type(sampled) is int else None,
            sampler_status=status.get('sampler'), daemon_build=status.get('build'),
            operational_write_request_ids=sorted(wanted),
            matching_recent_write_request_ids=sorted(wanted & available_ids),
            writes_missing_from_recent_snapshot=sorted(wanted - available_ids),
            scope='Candidate-only operational observation; not part of the paired benchmark or a causal before/after comparison.',
            limitations=['The status route returns a cached sampler snapshot, which can precede the latest calls.',
                'The recent ring is bounded at the node; missing request IDs do not mean missing product work.',
                'A phase omitted by the response remains unreported, not an assumed zero.',
                'Native barrier totals and detailed request phases have different scopes.'])
    except Exception as error:
        node.remember(operation='candidate-request-ops-observation', path=path,
            request_ops_available=False, diagnostic_error=f'{type(error).__name__}: {error}',
            operational_write_request_ids=sorted(wanted),
            scope='Optional candidate telemetry; failure does not change the ordinary value/notice proof.')
