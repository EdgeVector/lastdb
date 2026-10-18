"""Declare two generated key layouts and write shared fields through one layout."""
import logical


def install(node, spec):
    node.call('/api/schemas/seed-system', {'isolated_copy': True, 'schema': {
        'source': 'system_seed', 'name': spec.schema, 'descriptive_name': spec.schema,
        'schema_type': 'Hash' if spec.kind == 'hash' else 'HashRange',
        'key': {'hash_field': 'id'} if spec.kind == 'hash' else
               {'hash_field': 'bucket', 'range_field': 'id'},
        'fields': logical.FIELDS, 'field_descriptions': {
            field: 'CoreOwnedProtein same product shared ' + field for field in logical.FIELDS}}})


def same_atoms(left, right):
    a, b = left['results'], right['results']
    if len(a) != 1 or len(b) != 1 or any(
            a[0]['metadata'][field]['atom_uuid'] != b[0]['metadata'][field]['atom_uuid']
            for field in logical.FIELDS):
        raise RuntimeError('The protein members do not return the same shared atom UUIDs')


def run(node):
    source = logical.Spec('hash', 'CoreOwnedProteinHash', 'core-owned-protein-before')
    target = logical.Spec('hashrange', 'CoreOwnedProteinHashRange', source.hash)
    install(node, source)
    install(node, target)
    logical.read(node, source, {0: None}, 'protein-empty-source')
    logical.read(node, target, {0: None}, 'protein-empty-target')
    logical.write(node, [logical.mutation(spec, 0, 10, 'create') for spec in (source, target)],
                  'protein-create-both-key-layouts')
    node.status('protein-create-after-ack-before-first-value-read')
    right = logical.read(node, target, {0: 10}, 'protein-target-first-value-read', zero=True)
    left = logical.read(node, source, {0: 10}, 'protein-source-first-value-read', zero=True)
    same_atoms(left, right)
    logical.write(node, [logical.mutation(source, 0, 11)], 'protein-update-source-only')
    right = logical.read(node, target, {0: 11}, 'protein-target-after-update', zero=True)
    left = logical.read(node, source, {0: 11}, 'protein-source-after-update', zero=True)
    same_atoms(left, right)
    moved_source = logical.Spec('hash', source.schema, 'core-owned-protein-after')
    moved_target = logical.Spec('hashrange', target.schema, moved_source.hash)
    logical.read(node, moved_target, {0: None}, 'protein-new-hash-absence-before-move')
    logical.write(node, [logical.mutation(moved_source, 0, 12, full=True)],
                  'protein-hash-field-move-source-only')
    node.status('protein-move-after-ack-before-value-read')
    logical.read(node, target, {0: None}, 'protein-old-key-after-move', zero=True)
    right = logical.read(node, moved_target, {0: 12}, 'protein-new-key-after-move', zero=True)
    left = logical.read(node, moved_source, {0: 12}, 'protein-source-after-key-move', zero=True)
    same_atoms(left, right)
    logical.drained(node, 'protein-key-move-flushed')
    node.stop()
    node.start()
    logical.read(node, target, {0: None}, 'protein-old-key-normal-reopen')
    logical.read(node, moved_target, {0: 12}, 'protein-new-key-normal-reopen')
    logical.write(node, [logical.mutation(moved_source, 0, 0, 'delete')],
                  'protein-delete-source-only')
    logical.read(node, moved_source, {0: None}, 'protein-source-after-delete', zero=True)
    logical.read(node, moved_target, {0: 12}, 'protein-independent-target-after-delete', zero=True)
    logical.drained(node, 'protein-delete-flushed')
    node.remember(operation='core-owned-protein-complete',
        source_schema=source.schema, sibling_schema=target.schema,
        scope='Normal schema load binds shared per-field molecules across different keys. Both key layouts receive initial creates; this cohort does not claim record-envelope creation. Later updates and moves address only the source. Hash Delete preserves the differently shaped HashRange membership, per mutation_manager/cas.rs::protein_delete_peers.')
