# Compact replies and shared record reads

The normal query reply keeps all field metadata. An optional `response_format`
selects `full`, `values`, or `count` on `/api/query` and each `/api/queries/batch`
item. The default is `full`.

`values` keeps keys, fields, cursors, and all page integrity flags. It omits
field provenance and the author field. Each record keeps a conservative
`has_conflicts` flag. The page retains `conflict_flags` when coverage is unknown.
Access checks, layout membership, predicates, and order use the normal read.

`count` requires a keyed live read without conditions, value order, or a
continuation. It returns `total_count`, `exact`, `unresolved_rows`, and
`tombstoned_rows`. It uses the existing native count and its access checks.
It creates no probe record. An unavailable native count fails the request.
An expected count mismatch returns 409. The count keeps the normal read budget.

A record batch resolves each distinct identity once within its captured namespace.
It retains the result order and absent slots. Each selected tip still receives
its own metadata and logical path admission. A shared record does not authorize
another namespace or another logical path.

These changes add no disk records or alternate cache. The manual Brain spike
uses exact CI program artifacts, fresh generated homes, and both Brain layouts.
It compares read latency, durable writes, disk size, and sampled memory. It
requires the candidate V2 disk size to stay within the accepted V2 size.
The primary and its archive stay outside the operation.

Current write preparation loads the addressed schema's field key contracts before the native transaction.
An erasure loads the contracts for its exact selected keys too.
Named alternate keys resolve that schema's primary records and add no payload field copies.
Stored profile and namespace checks remain required.
