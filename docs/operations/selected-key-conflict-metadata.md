# Selected-key conflict metadata

`POST /api/conflicts/keys` reads the kept conflicts of selected record fields on the owner socket.
It accepts one batch of exact personal record keys.
A format 7 home keeps the conflicts of a record next to that record: one marker (`rcm:`) for each record and one row (`rcf:`) for each kept losing value.
The reply returns each kept losing value of each selected field.
It does not change the normal query conflict flags.

Collect the keys first. Send one request:

```json
{
  "keys": [
    {"schema_name": "app/Name", "fields": ["key", "title"], "hash": "exact-key"},
    {"schema_name": "app/Log", "fields": ["body"], "hash": "day", "range": "0001"}
  ]
}
```

The request accepts at most 64 keys and 32 fields per key.
Each schema uses the normal name resolver and must be available in the local cache.
A blocked schema does not redirect to a successor on this read.
Each field must exist in the schema.
A key has a `range` exactly when its fields use the HashRange layout.
Org and named DB scopes are refused.
Empty, duplicate, unknown, or excessive fields are refused.

The response uses the normal `ok` envelope.
It contains `version: 1`, `scope: selected_record_fields`, `count`, and `results`.
The result order matches the request order.
The field order matches each requested field list.
Each field result contains its `field`, a `state`, and its `conflicts`.
The state is `clean`, `conflict`, or `unknown`.
Each conflict entry contains the losing `mutation_id`, its `value`, the `loser` and `winner` stamps, `detected_at`, and `expires_at`.
Each key result also contains the combined `state` and the ordered `fields`.

CAUTION: The reply contains the losing field values. Send it only to the owner.

The read uses the existing serving mutation fence.
The fence admission and the conflict reads share a ten-second deadline.
Host QoS admission and schema resolution precede this deadline.
The read sends no flush, cloud request, or mutation.
It makes one read by key of the markers and one read by key of the rows that the markers name.
A marker entry that expired at the retention clock is not returned.
A row that the retention janitor released after the marker read is not returned.
Each row must name its record, its field, and its losing mutation, or the read fails.

A missing serving fence, a deadline, a storage error, or an invalid marker or row returns `unknown` for each field of the request.
A field with no kept losing value returns `clean`.
A field with one or more kept losing values returns `conflict`.
A consumer must refuse a write that requires clean conflict metadata when any result is unknown or conflicted.

The result describes one read. It is not a mutation guard or a conditional-write token.
Consumers must retain their normal conditional-write and partial-result checks.
This API does not establish whole-record, historical, or backup erasure.
