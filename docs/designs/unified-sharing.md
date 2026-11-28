# Design: one sharing path for personal, share and org (draft)

Date: 2026-11-28. Author: Claude, for Tom. Status: draft. Stage 1 is in #1955.
Related: `signed-history-sharing.md` (shared databases as built), `signed-log-on-local-machinery.md`
and `personal-database-signed-log.md` (the personal database as built), `write-leases.md` (leases),
`relay-protocol.md` (the relay).

## 1. The idea

Every database is the same thing: a store, one signed stream for each device that writes to it,
and grants signed by the owner. Personal, share and org differ only in their grants. Each is a
set of database writes, and permissions decide two things:

- **Who reads the database.** The relay serves its streams to the readers the grants name, and
  the read key opens what counts.
- **Whose entries count.** Every node decides that for itself from the signed grants and leases.

| Kind | Grants | Who writes directly | Who proposes |
|---|---|---|---|
| Personal | None. Every device of the owner signs under the one node key | Every device of the owner | Nobody |
| Share | One grant for each person, chosen by the owner | The owner, and `write` members under a live lease | `submit` members, and `write` members with no live lease |
| Org | An owner, and the same grants, set from People's Org preset, for many members | The same | The same |

Org is a share with more members and a preset of roles. It needs no code path of its own. What it
does need beyond a share is in section 6.

## 2. Trust

| Party | Trusted with | Not trusted with |
|---|---|---|
| Relay | Routing: who may deposit on a stream, and who may read the database | What counts. It cannot forge, change or count an entry. At worst it withholds |
| Node | Deciding what counts, by `AcceptEntry` over the signed grants and leases | — |
| Encryption | Keeping content from anyone who should not read it, including proposals from other members | — |

Two keys seal content:

- **The database read key** seals every entry that counts. Every reader holds it.
- **The owner's node key** seals every proposal (`cloud-api.md`, "Proposals are sealed to the
  owner"). Only the owner's devices open it, so no member reads a proposal before the owner
  decides it.

## 3. One path for every database

These are the personal database's mechanisms as built. Every database would use them.

1. **Write.** The store validates the write and, in the same transaction, makes it the next entry
   of this device's stream, sealed and signed, in the spool. Nothing commits without its entry.
2. **Send.** The sender reads the spool past the acknowledged position, sends the entries and
   their files, writes the head row of its own stream, and retires them.
3. **Receive.** For each stream this node may read, it reads on from the stream's head row. It
   checks each entry with `AcceptEntry` against the database's grants and leases. It applies an
   entry that counts with the stamps the store keeps for each part of a record (fields, file,
   inline file), and writes the head row in the same transaction.
4. **Moderate.** On the owner's devices, a received proposal is opened with the owner key and
   decided. An accepted proposal is written through step 1 as a direct entry that cites it. A
   declined one gets a `heads` entry that cites it, also through step 1.
5. **Snapshot and reclaim.** Each database freezes a snapshot of its store, names it in a signed
   entry, and asks the relay to drop what the snapshot covers.

Each signed snapshot identifies covered streams by writer key and device ID. Its publisher's key
is the default. Two different writers can use the same device ID and retain independent sequence
numbers, hashes and replay positions. An owner-only snapshot keeps its original signing bytes.
Restore binds each stored head to its signed writer, device, sequence, hash and order. A cleanup
request must account for all writers' streams before the relay can release any files.

A personal database is this path with no grants: `AcceptEntry` counts only the owner's key.

## 4. What changes for shared databases

| Today (shared) | Unified |
|---|---|
| `authored.json`, `uploaded.json`, `received.json` under `log-sync/<id>/` | The spool, and one head row per stream in the store |
| The whole history in memory, replayed at each restart | Records, stamps and head rows in the store. A restart replays nothing |
| The store commits, then the node signs. A crash in between leaves a record with no entry | Signed in the commit. No window |
| The whole record is the latest entry's | The latest stamp wins for each part of a record, the same rule as personal |
| Creator worked out from the history | `created_by` kept in the store beside the record, set by the first accepted write |
| No snapshot, and the relay keeps every entry | Snapshot and reclaim, so the relay holds about what the store holds |
| Files refused | Files, through the spool as in personal |

What stays the same: the wire format of an entry, `AcceptEntry`, grants, leases, proposals and
their statuses, the relay API, and the app API. What an entry holds changes: a shared write
carries the personal payload, cites only the entries the store keeps (birth, the grant or lease
it relies on, the proposal it decides), and a proposal names the stamp of the record it expects,
not an entry hash. No shared database is in use, so there is no migration (Tom, 2026-11-28).

The full history goes away. The node keeps, as signed-log rows in the store: the birth, every
grant, every lease, each undecided proposal, each decision, the direct entries under a lease that
is not closed, and the latest `relays` and `heads`. A revoke still works, because a revoked
member's later entries do not count.

A closed lease takes nothing back. A member's node signs a direct entry only while its lease is
live, and a node that delivers its entries before the lease closes has every one of them count.
An entry the publisher had not received when it closed the lease counts nowhere it arrives
later; its writer's node proposes it again. Until the owner decides, only that node shows the
value (Tom, 2026-11-28).

## 5. Moderation

Members do not all write directly. Moderation is a role, not a second path:

| Role | Its stream holds | Counts on every node |
|---|---|---|
| `read` | Nothing | — |
| `submit` | Proposals | Only the owner's accepting entry |
| `write` | Direct entries under a live lease, proposals otherwise | Its direct entries under the lease, and the owner's accepting entries |

Every reader receives every stream, because the owner's decision on a proposal cites the
proposal, and a node holds an entry until it has every entry it cites. A proposal is sealed to
the owner, so other members learn only that it exists, when, and its size.

Hiding a proposal stream from other members entirely was tried in #1955 and dropped (Tom,
2026-11-28): members then stall on the owner's decision. It would need nodes to accept an owner
entry that cites a proposal they cannot hold, on the owner's signature alone. That is a change to
`AcceptEntry`, and is not planned.

### Who checks a member's direct write

| Layer | Checked by | What it holds |
|---|---|---|
| Who may write | The owner's signed grant (`write`) | Only members the owner granted |
| Until when | The lease, on every node | A revoke or role change takes effect when the lease ends, with no emergency revoke |
| Shape | The node that writes and every node that applies | The schema, its keys, the record size limit |
| What it may say | The app on the member's node | The owner's rules, when the member runs the same app |
| Changes the owner approves | Proposals | `submit`, and `write` with no live lease |

A direct write is trusted to the member's app (Tom, 2026-11-28). A member who should not carry that
trust gets `submit`.

## 6. What org needs beyond share

1. **Many members.** This already works. A grant is one entry per member, and one lease covers
   every `write` member, so renewing does not grow with the number of members.
2. **An owner, who is the only moderator** (Tom, 2026-11-28). An org has an owner, as every
   database does: the node whose key signed its birth. The owner's devices decide every
   proposal, as in a share. Proposals wait while every owner device is offline, so an org's
   owner should keep one device that is usually on. Permissions can widen later. One way, if
   it is needed: a `moderate` role the owner grants to a few members, with a proposal sealed to
   the owner and to each moderator (one content key, wrapped for each), and `AcceptEntry`
   counting a decision signed by a current moderator. That changes the entry format and `AcceptEntry`, so it would be a stage of
   its own.
3. **Removing a member.** ExeMem applies the publisher-signed revoke to every new read and write by
   that identity, including publisher-stream reads and blob requests. LastDB retains the database
   read key. A former member keeps the data it already downloaded (Tom, 2026-11-28).
4. **Groups.** Not needed. People's preset sets a role on each member's grant, and the node and
   relay see only the grants.

## 7. Stages

Each stage is one PR, built and proved in CI (`shared_database_proof.py`,
`personal_database_proof.py`, the lease proofs and the signed-history DEV proofs). A stage that
changes the home copies records into a fresh home and updates `../home-format.md` in the same PR.

1. Proposals sealed to the owner. In #1955.
2. Shared writes through the spool and head rows. Removes `authored.json`, `uploaded.json`,
   `received.json` and the in-memory history. Four PRs:
   1. Store support: signed-log rows, entries that are not record writes, a note on a signed
      write, and a read of a record's winning stamp. No change in behaviour.
   2. `mutation_log`: the lease and grant counting rules as functions the node and the history
      both call.
   3. The personal sync engine over any store. No change in behaviour.
   4. The switch: shared stores capture into their own spool and sync through that engine; the
      old shared path is deleted. The shared store uses the personal code, its spool limit
      included.
3. `created_by` the same on every node. The per-part apply rule comes with 2.4.
4. Snapshot and reclaim for shared databases.
5. Files in shared databases. Done in #1956.

Moderators other than the owner are not a stage now (6.2).
