# Atom history sidecar

> **Model note (2026-11-01).** This page was written for the molecule model:
> schema → field → molecule → atom → file. Since home format 7 (the record model)
> a schema stores one record per primary key, and field molecules, tips and atoms
> do not exist. The [canonical model](lastdb-canonical-model.md) wins where this
> page differs.

LastDB keeps the current tip in each molecule. Optional history records retain
the atoms that earlier tips used. Each history record owns one atom reference
and the file references held only by its old tip metadata.

The sidecar does not copy a molecule, an unchanged field, or an atom body.
History reads use the known schema, field, and key. They do not change normal
queries or provide a complete record at a past time.

## Field policy

Each database sets a history policy for each canonical field identity.
Related schemas with the same canonical field use the same policy.
The default is `off`.

| Mode | Result |
| --- | --- |
| `off` | Future changes create no ordinary history. |
| `keep` | Retain each replaced value until an explicit purge. |
| `ttl` | Retain each replaced value for the configured positive number of seconds. |

The TTL starts when the value leaves the current tip. It does not expire a
current value. A policy change affects future entries only. Existing entries
keep their original expiry.

CAUTION: A policy that enables history raises the logical format to
`8;typed-ranges=v1;atom-history=v1`. Older binaries refuse the home.
A later change to `off` does not lower the format.

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

Add `--key-range` for a key with a range. The commands return JSON.
The list response contains entries, a continuation cursor, and the current
atom and mutation identities. The default page holds at most 32 entries.
The maximum page holds 100 entries. Entry order has no time guarantee.

Restore creates a normal new mutation. It retains the old file metadata.
The mutation checks the expected current write identity or an absent field
inside the normal conditional write path. A concurrent change returns a
conflict and writes nothing. The write updates the related molecules.

An expired entry is unavailable even while disk cleanup waits.
Only the owner socket exposes the history API. Current database access
controls the restore. History does not grant another app access.

## Delete and purge

An ordinary delete archives the current value under its field policy.
It keeps earlier ordinary history and the normal delete barrier.
Conflict records keep their separate delete and retention rules.

A key purge releases the current and historical references of its selected
paths. A large purge uses the existing bounded directory drain.
Schema removal releases paths that lose their final binding. Another schema
or database can still hold a shared atom.

To remove history while the current value stays:

```sh
lastdb history Notes body --key-hash note-one purge
lastdb history Notes body --key-hash note-one purge --cursor <cursor>
```

Repeat with the returned cursor until `complete` is true.
Each call changes one bounded page. A page can contain no live entries and
still return a cursor after deleted keys.

Purge does not remove another scope's references, revoke prior backups, or
change the existing replay rules.

## Atomic reference changes

The final tip decision, archive entry, file holds, expiry queue entry, and
reference counters join one native request batch.
A capture failure fails the write. A crash keeps the complete batch or none.
A repeated transition adds no reference and resets no expiry.

Atom references equal current tips plus counted conflict records plus
ordinary history records. Each history record also counts once in its
molecule header. Expiry and purge release references through the same planner.
An atom leaves only after its final reference goes.

The sidecar uses tagged `history\0` records and the existing slot directory.
It uses separate `ahq:v1` and `ahb:v1` queue families for expiry.
The queue and directory hold pointers, with no atom reference.
An `ahk:v1` record counts the history entries for one molecule key.
The count joins each capture or release batch.
Purge checks these counts in one batch, even after delete removes every tip.
The expiry worker keeps bounded cursors and stops with the database.
Before the first activation attempt, the worker waits without a timer or storage reads.
A keep or TTL policy attempt wakes it.
A reopened history home starts expiry at boot.
The format check prevents expiry before activation is visible.

## Backup and size

Complete database backups include the sidecar, policies, expiry queue, atoms,
and file dependencies. Restore preserves expiry times. It does not grant
another TTL period. Backup metadata checks include archived tip file references.

History stays local to the transitions that this node observes.
Mutation sync does not promise the same history on each device.
History starts after the policy is enabled; it cannot recover removed atoms.

History consumes disk space. A `keep` policy has no automatic size cap.
The normal storage limit rejects a batch that cannot retain required history.
It does not silently remove that history.

The reference verifier and offline recount include ordinary atom and file
references. Existing conflict queue diagnostics remain specific to conflicts.

## Product proof

`Atom history product proof` builds the CLI and daemon in GitHub Actions.
It uses a fresh generated home and exercises the public commands and owner API.
The required check `ci-required` calls this proof.
A pull request that does not change the product still reports success, and the long proof does not run.
The run summary and uploaded artifacts contain the result and source revision.
The primary remains unchanged.

The proof checks the public commands.
It does not drive generation activation, a tombstone with a visible generation, or a spared compliance purge.
The format marker has no owner read.
A restart after `Off` still serves the kept entries. It does not read the marker bytes.
