# Field history

A record holds the current value of each field. Optional field history keeps
copies of old field values. Each history entry holds its own copy of one old
field value and the stamp of that value. A file blob that the copy names holds
one blob reference while the entry lives.

History does not copy the whole record or an unchanged field. History reads use
the known schema, field, and key. They do not change normal queries or provide a
complete record at a past time.

## Field policy

Each database sets a history policy for each canonical field identity.
Related schemas with the same canonical field use the same policy.
The default is `off`.

| Mode | Result |
| --- | --- |
| `off` | Future changes create no history. |
| `keep` | Retain each replaced value until an explicit purge. |
| `ttl` | Retain each replaced value for the configured positive number of seconds. |

The TTL starts when the value leaves the record. It does not expire a current
value. A policy change affects future entries only. Existing entries keep their
original expiry.

A policy that enables history writes the home entry `layout:field-history`
with the value `on`. A later change to `off` does not remove that entry.

```sh
lastdb history Notes body policy
lastdb history Notes body policy keep
lastdb history Notes body policy ttl --seconds 2592000
lastdb history Notes body policy off
```

Pass `--database lastdb://db/<hash>` before the action for a named database.
The policy uses the canonical field identity, rather than a field name alone.

## Fetch and restore

```sh
lastdb history Notes body --key-hash note-one list
lastdb history Notes body --key-hash note-one list --cursor <cursor>
lastdb history Notes body --key-hash note-one get <entry-id>
lastdb history Notes body --key-hash note-one restore <entry-id> --expected-mutation <current-mutation>
lastdb history Notes body --key-hash note-one restore <entry-id> --absent
```

Add `--key-range` for a key with a range. The commands use the owner route
`POST /api/field-history` and return JSON. The list response contains the
current value, the entries, and a continuation cursor. The default page holds at
most 32 entries. The maximum page holds 100 entries. Entry order has no time
guarantee.

Restore creates a normal new mutation with the copied value.
The mutation checks the expected current mutation or an absent field inside the
normal conditional write path. A concurrent change returns a conflict and writes
nothing.

An expired entry is unavailable, also before the expiry worker deletes it.
Only the owner socket exposes the history API. Current database access
controls the restore. History does not grant another app access.

## Delete and purge

A `delete` copies the current value of each field with a history policy into a
history entry. It keeps the earlier history. The tombstone of the record holds
the delete floor. Kept conflict records keep their separate retention rules.

A `purge` of a key releases every history entry of the record in its own batch.
One purge releases at most 1,024 history entries of one record. A record with
more refuses the purge until its field history is purged. A schema drop releases
the history entries of the schema (`docs/lastdb-schema-drop.md`).

To remove history while the current value stays:

```sh
lastdb history Notes body --key-hash note-one purge
lastdb history Notes body --key-hash note-one purge --cursor <cursor>
```

Repeat with the returned cursor until `complete` is true.
Each call changes one bounded page. A page can contain no live entries and
still return a cursor after deleted keys.

Purge does not remove the history of another scope, revoke earlier backups, or
change the replay rules.

## Keys and one batch

| Entry | Key |
| --- | --- |
| History entry | `rh:{S}:{sha256(record key)}:{field identity}:{id}` |
| Field policy | `ahp:{field identity}` |
| Entry count of one record | `ahk:{sha256(record key)}` |
| Expiry queue and its directory | `ahq`, `ahb` (a time-bucketed queue with one-hour slices, `docs/lastdb-time-queue.md`) |

A storage scope prefix comes before each key. `S` is the record space of the
schema. The `id` is a hex SHA-256.

The record write, the history entry, its blob counter changes, its expiry queue
entry, and the entry count of the record join one native batch. A failure fails
the write. A crash keeps the complete batch or none. A repeated change adds no
reference and resets no expiry. Expiry and purge release blob references through
the same planner. A purge reads the entry counts in one batch, also after a
delete changed the record into a tombstone.

The expiry worker keeps bounded cursors and stops with the database. It waits,
with no timer and no storage reads, until a `keep` or `ttl` policy wakes it. A
home with `layout:field-history` set to `on` starts expiry at boot.

## Backup and size

A V2 backup holds the complete home, with the history entries, the policies, and
the expiry queue. Restore keeps the expiry times. It does not grant another TTL
period.

History stays local to the changes that this node observes. Another device does
not get the same history. History starts after the policy is enabled. It cannot
recover values that were replaced before.

History consumes disk space. A `keep` policy has no automatic size cap.
The normal storage limit rejects a batch that cannot keep the required history.
It does not silently remove that history.

The offline recount counts the file blob references of the history entries
(`docs/lastdb-blob-counter.md`).

## Product proof

The workflow `.github/workflows/field-history.yml` builds the CLI and daemon in
GitHub Actions. It uses a fresh generated home and runs the public commands and
the owner route. The required check `ci-required` calls this proof. A pull
request that does not change the product still reports success, and the long
proof does not run. The run summary and uploaded artifacts contain the result
and the source revision. The primary stays unchanged.
