# Selected-key conflict metadata

`POST /api/conflicts/keys` reads conflict metadata on the owner socket.
It accepts one batch of exact personal HashKey selections.
It reads no atom body and returns no field value or conflict payload.
It does not change the normal query conflict flags.

Collect the keys first. Send one request:

```json
{
  "keys": [
    {"schema_name": "app/Name", "fields": ["key", "title"], "hash": "exact-key"}
  ]
}
```

The request accepts at most 64 keys and 32 fields per key.
The complete request accepts at most 64 distinct field molecules.
Each schema uses the normal name resolver and must be available in the local cache.
A blocked schema does not redirect to a successor on this read.
Each field must exist and use the HashKey layout.
Org and named DB scopes are refused.
Empty, duplicate, unknown, or excessive fields are refused.

The response uses the normal `ok` envelope.
It contains `version: 1`, `scope: selected_field_molecules`, `count`, and `results`.
The result order matches the request order.
The field order matches each requested field list.
Each field result contains its `molecule_uuid` and a `state`.
The state is `clean`, `conflict`, or `unknown`.
Each key result also contains the combined `state` and the ordered `fields`.

CAUTION: A field result covers its complete molecule.
Any unresolved conflict at another key in that molecule also prevents a clean result.
The hash selects an exact HashKey request shape; the API does not infer conflict keys from legacy display strings.
Legacy conflict display strings can lose the HashRange key boundary.
A consumer must compare the returned molecule identity with the metadata from its normal keyed read.
The response contains no raw key, atom identifier, or conflict identifier.

The read uses the existing serving mutation fence.
Core fence admission and all conflict reads share a ten-second deadline.
Host QoS admission and schema resolution precede this deadline.
Cancellation can leave a read-only file task active until that task returns.
The read sends no flush, cloud request, or mutation.
It reads each distinct molecule once, with at most four concurrent molecule reads.
Each molecule read checks both current and legacy conflict key forms.
It accepts at most 128 conflict records per form and 4096 bytes per record.
It checks each record's molecule, identifier, and stored key.
It does not trust or repair the advisory `mcc` list.

A missing serving fence, deadline, storage error, invalid record, or incomplete page returns `unknown`.
A missing molecule identity also returns `unknown`.
A complete empty authoritative conflict prefix returns `clean`.
A valid complete prefix with an unresolved record returns `conflict`.
A consumer must refuse a write that requires clean conflict metadata when any result is unknown or conflicted.

The result describes one read. It is not a mutation guard or a conditional-write token.
Consumers must retain their normal conditional-write and partial-result checks.
This API does not establish whole-record, historical, or backup erasure.
