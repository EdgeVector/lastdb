# Manifest-declared TTL (`retention`)

An app manifest can give each of its schemas a TTL. The node applies it when
the schema is declared, before the first write, and deletes records that are
older than the TTL. A schema with no `retention` key gets no policy, and
nothing is ever deleted.

## The manifest key

```json
{
  "name": "myapp/DayLog",
  "schema_type": "HashRange",
  "key": { "hash_field": "day", "range_field": "at" },
  "fields": ["day", "at", "text"],
  "retention": { "ttl_days": 90 }
}
```

- Give `ttl_days` or `ttl_seconds`, not both. The value must be above zero and
  at most 3650 days. Any other key inside `retention` is an error.
- `retention` is node-local policy. `lastdb app check` sends it beside the
  schema in the declare request, never inside it. It is not part of the schema
  artifact, the schema identity hash, or canonicalization. The identity hash
  covers the owner app, the readable name, the sorted field names and the key
  layout, so a manifest with and without `retention` declares the same
  identity.
- Only the owner socket can declare a `retention`. An app on the app socket
  that sends one gets a 403.

## When the policy applies

| Command | What it does with `retention` |
|---|---|
| `lastdb app check --manifest m.json` | Read-only. Prints the policy it would apply, the policy in force now, and a warning when the manifest shows the range key cannot carry the time prefix. Writes nothing. |
| `lastdb app check --manifest m.json --sync` | Applies the policy before the declare returns, so the first write already sees it. Running it again with the same manifest changes nothing. |

A policy covers writes made after it exists. There is no backfill: a record
written before the declare is never expired.

Removing `retention` from a later manifest does not clear the policy. Use
`lastdb schema-retention clear`.

## Both installed names share one policy

An app schema has two installed names that share records: the manifest name
(`myapp/DayLog`) and the catalog schema, named by its identity hash. The
retention state (policy, age index, HashRange partition registry) is kept
under one anchor name, the catalog schema. A declare with `retention` records
the manifest name as an alias of the catalog schema, so a write through either
name is tracked and expired under the same policy. `lastdb schema-retention
get` on either name prints the same policy.

The policy follows the data, not the app. If the catalog merges two manifest
schemas of the same shape onto one catalog schema, they are one dataset. Declare
`retention` on each manifest name that writes to it.

A policy set with `lastdb schema-retention set` on a schema that no manifest
declared a `retention` for covers only the schema name given.

## Source of the policy

`lastdb schema-retention get` and `lastdb app check` print the source of the
policy in force:

- `manifest`: applied from the manifest `retention` key.
- `operator`: set with `lastdb schema-retention set`.

An operator policy wins over the manifest policy. The node keeps the manifest
TTL beside it. `lastdb schema-retention clear` removes the operator policy and
the manifest policy is back in force at once. If a manifest policy is in force
when you clear, it is removed, and the next declare applies it again.

## What the app author must do

- **Range and HashRange.** Begin every range key with the entry time as unix
  seconds, zero-padded to 20 digits, for example `00000000001791863226-0007`.
  A range key that does not begin with 20 digits is never expired. The range
  field must hold a string. A manifest does not contain the key values, so
  `lastdb app check` can warn only when `field_types` makes the range field a
  non-string type.
- **HashRange hash.** Use a day slice or a coarser one. The node keeps one
  registry entry for each hash it has seen, and every sweep queries all of
  them. A finer slice makes each sweep slower.
- **Hash and Single.** Nothing. A record expires by the time since its last
  write, and a rewrite resets the age.
- **One record for each entry.** A rewrite of a Range or HashRange record does
  not reset its age. Never edit an entry to keep it alive.

## Several devices

The policy is node-local state. It is not synced. Each device that holds the
data applies the TTL when it declares the schema, so every device must run
`lastdb app check --manifest ... --sync` with the same manifest. A device that
did not declare it does not expire anything. A TTL delete is an ordinary
delete and syncs to the other devices like one.

## Cost

The sweep runs on the self-metrics sampler tick (60 seconds by default) and
deletes in batches of 64 records. Each batch holds the write lock, so writes
wait during a batch. A log that adds more entries than the sweep can delete is
not kept down by a TTL. `LASTDB_TTL_SWEEP=0` turns the sweep off.
