"""Enrolled ordinary writes, source-rule refusals, and real aggregate recovery."""

from concurrent.futures import ThreadPoolExecutor

from aggregate_requests import (SOURCE, TOKEN, durable, records, repeat, request, rpc,
                                source_write, summary)


def ordinary(group, number, value):
    body = source_write(group, number, value)
    body.pop("aggregate_set")
    return body


def commit_members(node, group, values, changed, create=False):
    # AggregateSet has no batch route. Collect all source writes before submission.
    bodies = [source_write(group, number, values[number], create) for number in changed]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(lambda body: rpc(node, "/api/mutation", body)[0], bodies):
            durable(result)


def repair_finalize(node, group, values):
    token = summary(node, group, values, 0)[TOKEN]
    result = rpc(node, "/api/aggregate/repair", request(group, token))[0]
    durable(result)
    token = result["repair_token"]
    summary(node, group, values, 0, token)
    durable(rpc(node, "/api/aggregate/finalize", request(group, token))[0])
    summary(node, group, values, 1, token)
    return group, values, token, records(node, group, values)


def check_refusals(node, group, size, values, token):
    key = {"hash": group, "range": "member-0000"}
    for verb in ("delete", "purge"):
        body = {"type": "mutation", "schema": SOURCE, "mutation_type": verb,
                "durability": "durable", "key_value": key, "fields_and_values": {}}
        result = rpc(node, "/api/mutation", body, (400,))[0]
        if "enrolled aggregate sources reject Delete and Purge" not in result["error"]:
            raise RuntimeError("An enrolled erase failed for an unrelated reason: " + result["error"])
        summary(node, group, values, 1, token)
        records(node, group, values)
    body = ordinary(group, 0, values[0])
    body["fields_and_values"] = {"state": {"_fold_tombstone": {"reason": "generated refusal"}}}
    result = rpc(node, "/api/mutation", body, (400,))[0]
    # The public request gate rejects this shape before aggregate enrollment is read.
    if "rejects the reserved tombstone value on create or update" not in result["error"]:
        raise RuntimeError("A tombstone failed for an unrelated reason: " + result["error"])
    summary(node, group, values, 1, token)
    records(node, group, values)
    if size > 1:
        erasure = {"type": "mutation", "schema": SOURCE, "mutation_type": "delete",
                   "durability": "durable", "key_value": {"hash": group, "range": "member-0001"},
                   "fields_and_values": {}}
        body = {"mutations": [ordinary(group, 0, values[0] + 1), erasure]}
        result = rpc(node, "/api/mutations/batch", body, (400,))[0]
        if "enrolled aggregate sources reject Delete and Purge" not in result["error"]:
            raise RuntimeError("The mixed batch failed for an unrelated reason: " + result["error"])
        summary(node, group, values, 1, token)
        records(node, group, values)


def cycle(node, group, size, values, old_token, timed):
    changed = [0] if size == 1 else [0, 1]
    previous = dict(values)
    for number in changed:
        values[number] += 1
    bodies = [ordinary(group, number, values[number]) for number in changed]
    if size == 1:
        result = timed("ordinary-single/1", "/api/mutation", bodies[0])
        ids = [result["mutation_id"]]
    else:
        result = timed("ordinary-batch/2-of-32", "/api/mutations/batch", {"mutations": bodies})
        ids = result["mutation_ids"]
        if result.get("count") != 2 or len(ids) != 2:
            raise RuntimeError("The ordinary batch lacks two source acknowledgements")
    durable(result)
    token = summary(node, group, previous, 0)[TOKEN]
    if token != ids[-1]:
        raise RuntimeError("The ordinary batch marker does not name its latest source")
    records(node, group, values, previous)
    result = timed(f"stale-finalize/{size}", "/api/aggregate/finalize", request(group, old_token), (400,))
    if "requires a durable repair grant" not in result["error"]:
        raise RuntimeError("The invalidated target still has a different finalize authority: " + result["error"])
    summary(node, group, previous, 0, token)
    result = timed(f"stale-member-repair/{size}", "/api/aggregate/repair", request(group, token), (400,))
    if "does not match the latest source winner" not in result["error"]:
        raise RuntimeError("Repair did not refuse the stale member winner: " + result["error"])
    token = summary(node, group, previous, 0)[TOKEN]
    if not token.startswith("repair-in-progress:"):
        raise RuntimeError("The failed repair lacks its durable invalidation marker")
    for number in changed:
        values[number] += 1
    commit_members(node, group, values, changed)
    token = summary(node, group, values, 0)[TOKEN]
    records(node, group, values)
    result = timed(f"repair/{size}", "/api/aggregate/repair", request(group, token))
    durable(result)
    token = result["repair_token"]
    summary(node, group, values, 0, token)
    durable(timed(f"finalize/{size}", "/api/aggregate/finalize", request(group, token)))
    summary(node, group, values, 1, token)
    return token


def exercise_group(node, size, samples, timed):
    group, values = f"members-{size}", {number: number + 1 for number in range(size)}
    commit_members(node, group, values, list(values), create=True)
    _, _, token, _ = repair_finalize(node, group, values)
    for _ in range(samples):
        token = cycle(node, group, size, values, token, timed)
    check_refusals(node, group, size, values, token)
    repeat(node, group, token)
    return group, values, token, records(node, group, values)


def cross_targets(node, groups):
    previous = [dict(values) for _, values, _, _ in groups]
    bodies = []
    for group, values, _, _ in groups:
        values[0] += 1
        bodies.append(ordinary(group, 0, values[0]))
    result = rpc(node, "/api/mutations/batch", {"mutations": bodies})[0]
    durable(result)
    ids = result["mutation_ids"]
    if result.get("count") != 2 or len(ids) != 2:
        raise RuntimeError("The two-target ordinary batch lacks complete acknowledgement")
    for index, (group, values, token, _) in enumerate(groups):
        summary(node, group, previous[index], 0, ids[index])
        records(node, group, values, previous[index])
        rpc(node, "/api/aggregate/finalize", request(group, token), (400,))
    bodies = []
    for group, values, _, _ in groups:
        values[0] += 1
        bodies.append(source_write(group, 0, values[0]))
    with ThreadPoolExecutor(max_workers=2) as pool:
        for result in pool.map(lambda body: rpc(node, "/api/mutation", body)[0], bodies):
            durable(result)
    with ThreadPoolExecutor(max_workers=2) as pool:
        return list(pool.map(lambda group: repair_finalize(node, group[0], group[1]), groups))
