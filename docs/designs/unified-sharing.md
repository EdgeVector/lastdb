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

What stays the same: the entry format, `AcceptEntry`, grants, leases, proposals and their
statuses, the relay API, and the app API.

The full history goes away. A revoke still works, because a revoked member's later entries do not
count. What is lost is rebuilding a shared database from every entry it ever had. Nothing needs
that except the shared code it replaces.

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
3. **Removing a member.** A removed member still holds the read key. The relay already refuses
   it new entries, but a relay that routed wrongly would hand it content it can open. Rotating
   the read key when a member leaves closes that. It matters more in an org, where members come
   and go, and it is an open question here.
4. **Groups.** Not needed. People's preset sets a role on each member's grant, and the node and
   relay see only the grants.

## 7. Stages

Each stage is one PR, built and proved in CI (`shared_database_proof.py`,
`personal_database_proof.py`, the lease proofs and the signed-history DEV proofs). A stage that
changes the home copies records into a fresh home and updates `../home-format.md` in the same PR.

1. Proposals sealed to the owner. In #1955.
2. Shared writes through the spool and head rows. Removes `authored.json`, `uploaded.json`,
   `received.json` and the in-memory history.
3. One apply rule for every database: stamps for each part of a record, and `created_by` in the
   store for the `submit` rule.
4. Snapshot and reclaim for shared databases.
5. Files in shared databases.
6. Read key rotation on member removal, if Tom chooses it.

Moderators other than the owner are not a stage now (6.2).

## 8. Open questions

1. Rotate the read key when a member is removed?
