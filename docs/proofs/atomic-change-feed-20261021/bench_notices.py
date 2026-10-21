"""Observe the ordinary paged application feed without a storage flush."""
import time


def page_pass(node, cursor):
    events, started, pages = [], time.perf_counter_ns(), 0
    while True:
        request = {'since_cursor': cursor, 'limit': 500} if cursor else {'limit': 500}
        response, _, _, identity = node.call('/api/app/changes', request)
        data = response.get('data', response)
        changes, following = data.get('changes'), data.get('next_cursor')
        if (not isinstance(changes, list) or not isinstance(following, str)
                or data.get('gap') is not False or not isinstance(data.get('has_more'), bool)):
            raise RuntimeError('The ordinary feed page lacks its complete controls')
        node.remember(operation='notice-page', request_id=identity, cursor_before=cursor,
            cursor_after=following, has_more=data['has_more'], events=changes)
        events.extend(changes)
        pages += 1
        if not data['has_more']:
            return following, events, {'started_ns': started,
                'finished_ns': time.perf_counter_ns(), 'pages': pages}
        if following == cursor or pages >= 128:
            raise RuntimeError('The ordinary notice pagination does not advance within its bound')
        cursor = following


def check_events(events, wanted, seen):
    for event in events:
        identity = event.get('mutation_id')
        if identity not in wanted:
            continue
        if identity in seen:
            raise RuntimeError('The ordinary feed repeats a measured mutation ID')
        expected = wanted[identity]
        if (event.get('schema_name') != expected['schema_name']
                or event.get('key') != expected['key']
                or event.get('operation') != expected['operation']
                or event.get('background_tasks_drained') is not False
                or event.get('convergence_pending') is not True):
            raise RuntimeError('A measured notice has different schema, key, operation, or convergence flags')
        seen.add(identity)


def observe(node, cursor, wanted, candidate, last_reply_ns, timeout=60.):
    started, deadline, seen, passes = time.perf_counter_ns(), time.monotonic() + timeout, set(), []
    while True:
        cursor, events, timing = page_pass(node, cursor)
        check_events(events, wanted, seen)
        passes.append(dict(timing, events=len(events), matched=len(seen), missing=len(wanted) - len(seen)))
        if candidate and len(passes) == 1 and seen != set(wanted):
            raise RuntimeError('A combined-commit notice is absent on the first pagination pass after ack')
        if seen == set(wanted):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError('The acknowledged mutations do not reach the ordinary feed before the deadline')
        time.sleep(.05)
    native = node.status('after-observed-notices')
    while (native['accepted_sequence'] != native['completed_sequence']
           or native['accepted_batch'] is not False):
        if time.monotonic() >= deadline:
            raise RuntimeError('The observed notices leave native work before the next quiet gate')
        time.sleep(.05)
        native = node.status('wait-for-observed-notice-completion')
    summary = {'required_ids': sorted(wanted), 'passes': passes,
        'immediate_presence': passes[0]['missing'] == 0,
        'observation_start_after_last_reply_ns': started - last_reply_ns,
        'observed_complete_after_last_reply_ns': time.perf_counter_ns() - last_reply_ns,
        'all_notice_ids_and_coordinates_match': True,
        'scope': 'First pagination pass has no sleep or retry. Baseline later polls are outside write timing.',
        'after_notice_drain': native}
    node.remember(operation='notice-observation', **summary)
    return cursor, summary
