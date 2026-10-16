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
every 60 seconds (`LASTDB_TTL_SWEEP=0` turns it off). How a pass erases, the
verbs and the knobs are in [`lastdb-ttl-sweep.md`](lastdb-ttl-sweep.md).

| Layout | What expires | Records written before the declare |
|---|---|---|
| Range | Every record whose range key starts with 20 digits of unix seconds older than the TTL, whichever name wrote it. | **Deleted at the next sweep.** |
| HashRange | In each hash partition the node has seen written since the policy exists, every record whose range key is older than the TTL. A partition that gets no write after the declare is not swept. | **Deleted in any partition that receives a later write.** |
| Hash, Single | A record whose last write through the policy is older than the TTL. A rewrite resets the age (see below). | Kept. There is no backfill: only writes after the declare are tracked. |

A range key that does not begin with 20 digits is never expired.

**A rewrite resets the age of a Hash or Single record.** The sweep re-reads the
age of its keys before each batch and drops a key that a client rewrote since
the pass listed it. The erase then compares the write time of the tip with the
cutoff under the tip lock, so a rewrite that was acked and not yet indexed is
spared too. The limits are in
[`lastdb-ttl-sweep.md`](lastdb-ttl-sweep.md) (What a pass does, The write-time
guard). For a schema with a manifest `retention`, a write through either installed
name is tracked (see "Both installed names share one policy"). A Range or
HashRange record is never kept alive by a rewrite: its age is the time in its
range key.

## Guards at declare time

The declare refuses a TTL, before it binds the schema, when:

1. **The records are shared.** The TTL sweep runs through the catalog schema the
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
   declare with no `retention` is refused when it would bind a NEW schema to a
   catalog schema that a TTL reaches. This is the same closure as guard 1: a TTL
   on the catalog schema, on a schema it was expanded from, or on any schema
   bound to either, whether a manifest declared it or an operator set it. A
   plain `app/LogExt` that adds one field to the readable name of a TTL'd
   `app/Log` is expanded from `app/Log`'s catalog schema, so it is refused. The
   refusal names the holders and says what to do: give the schema its own
   shape, or clear the TTL. A schema that is **already bound** is only warned:
   an app that declares its schemas on every start must keep starting, and the
   warning says its records are under the TTL. A refused declare drops the
   catalog schema it loaded when nothing uses it.

A refusal in 1 to 3 or 5 happens before the schema is bound: nothing is bound,
no alias or policy is written. If the policy cannot be written after the bind
(for example the audit log is not writable), the schema stays declared and
bound, no policy changes, and the same command completes the job: a declare is
idempotent.

**One declare at a time for a catalog schema.** The guards read the node before
the bind, and the TTL is written after it. A declare takes a lock on its
catalog schema before the pre-check and keeps it until it returns, so the
pre-check, the bind and the retention step of one declare cannot interleave
with another declare of the same catalog schema. The lock is in the `lastdbd`
process. One home has one `lastdbd` (a second process is refused at the socket),
and every caller reaches the node through it. If the state still changes under
a declare, the retention step fails with an error (a 409), never a 200 with a
note.

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

The catalog schema can carry other field names than your manifest: the Schema
Service maps a field to its canonical name (a range field `tick` can come back
as `stamp`). A write through the catalog name uses the catalog's field names. The
sweep reads the key from the catalog schema, so the TTL does not depend on which
names a writer used.

When a re-declare binds the manifest name to a new catalog schema (the app added
a field), the policy follows it: the policy, an operator override included, and
the tracking move to the new anchor. A policy is never left on the old
anchor. The move is subject to the same guards, and it refuses if another
manifest name still uses the old anchor.
The move copies the policy and the tracking, deletes the old policy, then points
the alias at the new anchor. If it is interrupted, the alias is still on the old
anchor and the old policy is gone, so the next declare finishes the move; the
old TTL cannot keep sweeping. Until then `schema-retention get` through the
manifest name can read `unset`: read it through the catalog name.

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
the manifest policy is back in force at once. **When the manifest TTL is
shorter, that clear needs `--confirm-expire-existing`**: records between the two
TTLs expire for good (a Range schema's at the next sweep). Without it the clear
is refused, and the output says `existing_records_may_expire`. If a manifest
policy is in force when you clear, it is removed, and the next declare applies
it again. A clear that leaves no policy also removes the manifest-name aliases
that pointed at it.

If the node cannot read a policy, `schema-retention set` and `clear` fail. A
read error is never treated as "no policy".

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
- **The sweep verb.** `purge` is the default. `delete` is opt-in with
  `LASTDB_TTL_SWEEP_VERB=delete`. `delete` leaves two `rdel:v2:` barrier rows
  for each field of each key, about 3,170 B for each key and field, and nothing
  prunes them. What each verb erases and leaves is in
  [`lastdb-ttl-sweep.md`](lastdb-ttl-sweep.md) (Verbs, Records that stay).
  Both verbs leave the conflict records: the database layer keeps conflicts, and
  the app owns the shape of its data.
- A TTL erase syncs to the peers as an erase of that key, whichever verb runs,
  and **a peer that never declared the TTL still applies it**. A peer that never
  held the record skips it without an error.
- An erase is ordered by write clock like any other write, and the sweep stamps
  it at the time of the sweep. A peer rewrite with an older author clock can
  lose to that stamp, on both verbs. Without the barrier (`purge`) an older write
  that arrives later can also bring the key back. See Last writer wins in
  [`lastdb-ttl-sweep.md`](lastdb-ttl-sweep.md).
- A synced device that is not told about a manifest still loses the expired
  records, because the erase arrives from the device that has the TTL.
- **The collision guard sees only the schemas loaded on the node that declares.**
  A TTL erase replays on every peer as an erase of a key in the catalog
  schema it names. A schema on a peer that is bound to that catalog schema loses
  its records, even though this node saw no collision. Declare the same
  manifests on EVERY device before any TTL is set, and run `lastdb app check` on
  each device: its `shared_with`, its collision flags and its refusals are
  computed from the schemas that device has loaded.

## Downgrade

An older build does not know the manifest-name aliases that a manifest
`retention` creates. By reading its code:

- It keys the retention tracking by the exact name a write used. It stops
  tracking Hash and Single writes, and HashRange partitions, that are written
  through the manifest name, because the policy sits under the catalog name.
  The records written after the downgrade through the manifest name are never
  expired.
- `schema-retention clear --schema <manifest name>` clears nothing there: the
  policy is not under that name.
- A replay on the older build (a restore, or a peer sync that needs the schema
  catalog) reads every row of the schema-states plane and fails on an alias row
  with "unreadable replay schema authority value".

The safe procedure: before you downgrade, run
`lastdb schema-retention clear --schema <manifest name>` for every schema whose
manifest declared a `retention`, and clear an operator policy that sits over a
manifest policy a second time, with `--confirm-expire-existing` when it asks.
Check that `lastdb schema-retention get` reads `unset` for each. A clear that
leaves no policy also removes the alias rows, so no row remains that the older
build cannot read. Then downgrade. After it, declare the manifests again on the
new build to bring the TTLs back.

## Cost

The sweep paces itself. It erases one batch of keys (16 by default), then rests
so that it spends a share of wall time (25 percent by default, and at least
25 ms). The rest is at most 10 minutes. A batch holds the local mutation
admission while it runs, and a `purge` batch also occupies the persist lane of the
swept schema, so a durable write to that schema waits for the batch. How long a
batch takes depends on the verb and on the number of keys in the batch, not on the
size of the schema. The measured numbers and the
knobs (`LASTDB_TTL_SWEEP_BATCH`, `LASTDB_TTL_SWEEP_DUTY_PCT`,
`LASTDB_TTL_SWEEP_MIN_PAUSE_MS`) are in
[`lastdb-ttl-sweep.md`](lastdb-ttl-sweep.md). A log that adds more entries than
the sweep can erase is not kept down by a TTL. `LASTDB_TTL_SWEEP=0` turns the
sweep off.
