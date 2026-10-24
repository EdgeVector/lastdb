"""Establish immutable generated values before an adaptive tier capture."""

from concurrent.futures import ThreadPoolExecutor
import hashlib

RECORDS_PER_SCHEMA = 32
VALUE_BYTES = 4096


def seed(node, schemas, save, output):
    def one(number):
        name = schemas.schema_name(number)
        mutations, expected = [], {}
        for ordinal in range(RECORDS_PER_SCHEMA):
            key = f"base-{ordinal:03}"
            value = hashlib.shake_256(f"{name}-{key}".encode()).hexdigest(VALUE_BYTES // 2)
            expected[key] = hashlib.sha256(value.encode()).hexdigest()
            mutations.append({"type": "mutation", "schema": name, "mutation_type": "create",
                              "key_value": {"hash": name, "range": key},
                              "fields_and_values": {"bucket": name, "id": key, "value": value}})
        receipt = node.call('/api/mutations/batch', {'mutations': mutations})
        if (receipt.get('durability') != 'durable'
                or len(receipt.get('mutation_ids', [])) != len(mutations)):
            raise RuntimeError('tier base lacks complete durable mutation receipts')
        return name, expected, receipt

    # Automatic maintenance is disabled for this explicit adaptive-tier case.
    # The automatic and full cases separately retain their production worker.
    if node.maintenance:
        raise RuntimeError('a seed-era automatic copy can replace the intended tier base')
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(one, range(64)))
    expected = {name: values for name, values, _ in rows}
    node.base_mutations = sum(len(values) for values in expected.values())
    save(output / 'tier-base.json', {
        'records_per_schema': RECORDS_PER_SCHEMA, 'value_bytes': VALUE_BYTES,
        'records': node.base_mutations, 'expected_sha256': expected,
        'receipts': {name: receipt for name, _, receipt in rows},
        'automatic_maintenance': False, 'measured_keys_are_disjoint': True})
    receipt = node.call('/api/db/compact', {
        'dry_run': False, 'full_copy': True, 'grace_ms': 0, 'report_usage': True})
    save(output / 'tier-base-compaction.json', receipt)
    packed = receipt['compact']['native_disk']
    if (not packed['executed'] or packed['tier_boundary'] != 0
            or packed['records_copied'] < node.base_mutations):
        raise RuntimeError('tier setup did not establish a full compacted base')
    values = schemas.check_values(node, expected)
    save(output / 'tier-base-readback.json', values)
    if not values['all_values_match']:
        raise RuntimeError('immutable tier base differs before measurement')
    # Do not restart: the production tier policy remembers this base in memory.
    return expected
