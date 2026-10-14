# Manifest-declared TTL (`retention`)

An app manifest can give each of its schemas a TTL. The node applies it when
the schema is declared, before the first write, and deletes records that are
older than the TTL. **A TTL deletes records for good.** A schema with no
`retention` key gets no policy from the manifest.

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

- Give `ttl_days` or `ttl_seconds`, not both. The value must be at least one
  hour and at most 3650 days. A smaller value is refused, because it is
  usually a unit slip (`ttl_seconds: 90` meant as days). Set
  `LASTDB_MIN_MANIFEST_TTL_SECONDS` to lower the floor, on the node and on
  the machine that runs the CLI.
- `confirm_expire_existing: true` is the only other key. See "Existing
  records" below. Any other key is an error.
- `retention` is node-local policy. `lastdb app check` sends it beside the
  schema in the declare request, never inside it. It is not part of the schema
  artifact, the schema identity hash, or canonicalization. The identity hash
  covers the owner app, the readable name, the sorted field names and the key
  layout, so a manifest with and without `retention` declares the same
  identity.
- Only the owner socket can declare a `retention`. A caller that is not the
  owner gets a 403.

## What a TTL deletes

A TTL is **not** limited to records written after the declare. The sweep runs
every 60 seconds (`LASTDB_TTL_SWEEP=0` turns it off).

| Layout | What expires | Records written before the declare |
|---|---|---|
| Range | Every record whose range key starts with 20 digits of unix seconds older than the TTL, whichever name wrote it. | **Deleted at the next sweep.** |
| HashRange | In each hash partition the node has seen written since the policy exists, every record whose range key is older than the TTL. A partition that gets no write after the declare is not swept. | **Deleted in any partition that receives a later write.** |
| Hash, Single | A record whose last write through the policy is older than the TTL. A rewrite resets the age. | Kept. There is no backfill: only writes after the declare are tracked. |

A range key that does not begin with 20 digits is never expired.

## Guards at declare time

The declare refuses a TTL, before it binds the schema, when:

1. **The records are shared.** A TTL purge runs through the catalog schema the
   declare resolved to. It erases a record from every molecule that schema
   names, so it reaches every schema bound to it, whoever wrote the record. The
   declare refuses with `shared_with=[...]` when another loaded schema is bound
   to the catalog schema (by field mapper, retention alias or shared molecule),
   or when another app owns the catalog schema or a schema it was expanded
   from. The same check runs again when the policy is written. Two manifest
   schemas of the same shape can resolve to one catalog schema: `lastdb app
   check` flags them, and fails when one of them declares a `retention`. Give
   schemas with different lifecycles different shapes. You can still set a
   policy on purpose with `lastdb schema-retention set`.
2. **Existing records may expire** (`existing_records_may_expire`). A new TTL on
   a Range or HashRange schema that already holds data needs confirmation:
   `lastdb app check --sync --confirm-expire-existing` for one run, or
   `retention.confirm_expire_existing: true` in the manifest. A value kept in
   the manifest confirms every later change as well, so prefer the flag. A
   re-declare of the TTL that already runs needs no confirmation.
3. **The TTL gets shorter.** A TTL shorter than the manifest TTL running needs
   the same confirmation, for every layout: records between the two TTLs
   expire.
4. **The declare composes several catalog schemas.** A retention needs one.
5. **A schema without a retention would join a retained catalog schema.** A
   declare with no `retention` is refused when it would bind the schema to a
   catalog schema that another manifest name put a TTL on: the purge would
   delete the new schema's records too. The refusal names the manifest names
   that declared the TTL. This also stops a different app whose schema the
   catalog merges onto yours.

A refusal in 1 to 3 or 5 happens before the schema is bound: nothing is bound,
no alias or policy is written. If the policy cannot be written after the bind
(for example the audit log is not writable), the schema stays declared and
bound, no policy changes, and the same command completes the job: a declare is
idempotent.

## What `lastdb app check` prints

`lastdb app check` (read-only) prints the policy it would apply, the policy in
force now, `shared_with`, whether the TTL needs confirmation, and a warning when
the manifest shows the range key cannot hold the time prefix. A TTL a sync
would refuse is printed as `REFUSED` and the command fails. `--sync`,
`lastdb app promote` and `lastdb app register-schemas` print every change:

- `new`, `shortened from Ns`, `lengthened from Ns`, `unchanged`;
- an operator policy that wins over the manifest TTL;
- **a removal.** A manifest that no longer declares a `retention` does not clear
  the policy: the manifest TTL keeps running. The check says so and names the
  command that removes it: `lastdb schema-retention clear --schema <name>`.

Every change to a policy writes a durable audit row to
`<home>/logs/retention-audit.jsonl`: who asked, when, the source, and the
policy before and after. The row is written BEFORE the change (`status:
"applying"`). If it cannot be written, the policy does not change. A second row
with the same `event_id` closes it (`"applied"` or `"failed"`). This covers a
manifest apply, a move to a new anchor, and `schema-retention set` and `clear`.

## Both installed names share one policy

An app schema has two installed names that share records: the manifest name
(`myapp/DayLog`) and the catalog schema, named by its identity hash. The
retention state (policy, age index, HashRange partition registry) is kept under
one anchor name, the catalog schema. A declare with `retention` records the
manifest name as an alias of the catalog schema, so a write through either name
is tracked and expired under the same policy. `lastdb schema-retention get` on
either name prints the same policy and the anchor.

When a re-declare binds the manifest name to a new catalog schema (the app added
a field), the policy follows it: the policy, an operator override included, and
the tracking move to the new anchor. A policy is never left on the old
anchor. The move is subject to the same guards, and it refuses if another
manifest name still uses the old anchor.

A policy set with `lastdb schema-retention set` on a schema that no manifest
declared a `retention` for covers only the schema name given, and does not
follow the schema to a new catalog schema.

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
  The range field must hold a string. A manifest does not contain the key
  values, so `lastdb app check` can warn only when `field_types` makes the
  range field a non-string type.
- **HashRange hash.** Use a day slice or a coarser one. The node keeps one
  registry entry for each hash it has seen, and every sweep queries all of
  them. A finer slice makes each sweep slower.
- **Hash and Single.** Nothing. A record expires by the time since its last
  write, and a rewrite resets the age.
- **One record for each entry.** A rewrite of a Range or HashRange record does
  not reset its age. Never edit an entry to keep it alive.
- **Different shapes for different lifecycles.** A log with a TTL and a ledger
  with none must not have the same fields and key.

## Multi-device

- The policy is node-local state. It is not synced. Each device that holds the
  data applies the TTL when it declares the schema, so every device must run
  `lastdb app check --manifest ... --sync` with the same manifest. A device
  that did not declare it expires nothing by itself.
- The sweep deletes with `MutationType::Purge`: a hard erase. It is not a
  tombstone. A TTL purge syncs to the peers as a purge, and **a peer applies it
  whether or not that peer has the TTL**. A peer that never held the record
  skips it without an error.
- The purge erases the value and the history rows of the record. It does not
  erase `conflict:` rows: the database layer keeps conflicts, and the app owns
  the shape of its data.
- A synced device that is not told about a manifest still loses the expired
  records, because the purge arrives from the device that has the TTL.

## Cost

The sweep deletes in batches of 64 records. Each batch holds the write lock, so
writes wait during a batch. A log that adds more entries than the sweep can
delete is not kept down by a TTL.
