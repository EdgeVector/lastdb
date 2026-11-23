# Design: signed-history sharing

Status: approved by Tom, 2026-11-22. Describes the code in `lastdb_node/src/log_sync`, `mutation_log` and `signed_history_materialize`.
Related: `signed-history-single-log.md` (the log and cursor), `relay-protocol.md` (the relay), `cloud-api.md` (the wire format).

## Principles

1. **One owner per fact.** Each question has one system that answers it. Everyone else asks that system or derives from it.
2. **The history is the truth, the store is a view.** A shared database's records are a function of the accepted entries and the publisher's grants.
3. **The relay is a mailbox, not a judge.** It decides who may deposit an entry. Nodes decide what counts.
4. **A signed entry is never rewritten.** A mistake is answered by a later signed entry.

## Who owns what

| Fact | Owner | Rule |
|---|---|---|
| Does an entry count? | `AcceptEntry` in `mutation_log` | A function of the entry and the publisher's grants. Only the publisher's direct entries, bundles and publisher kinds change records. |
| May a node deposit an entry? | Relay | The publisher, or a member holding `submit` or `write` that appends `data` (proposals, and direct entries that the nodes count only under a lease) to its own stream. |
| Order and continuity | The device stream | Consecutive `writer_seq`, each naming the previous hash. |
| A record's value, author and revision | History | The winner by Lamport position. The revision is the winning entry's hash. |
| Who created a record | History | The writer of the earliest write by position. |
| What is pending upload | The log and its cursor | The entries in `authored.json` past the count in `uploaded.json`. |
| What a database shows | Materializer | The fold of the accepted entries. |
| What happened to a proposal | The publisher | A direct entry that cites it, or a `heads` entry that cites it. |

## Roles

| Role | May do | Publisher accepts |
|---|---|---|
| `read` | Read the log. | Nothing. |
| `submit` | Propose. | A create, and a change to a record the member created. |
| `write` | Propose. | A change to any record. |

A member never signs a direct entry. A `write` grant does not let a member write; it widens what the publisher accepts from it. `any` carries `read` only.

## Publisher write path

1. The store validates the write and commits it.
2. The node signs the `data` entries (and the `schema` entry first, when the schema is new or changed) and appends them to the log through `history_store::commit_authored`. The commit accepts the entry on a copy of the history, appends it to `authored.json`, swaps the copy in, and wakes sync.
3. An entry that cannot be signed leaves a record with no entry. `reconcile_keys` puts each key back to its history winner, or to no record, and the caller gets the error.

One window remains: a crash between step 1 and step 2 leaves a record with no entry until a later write touches that key.

## Proposal path

1. A member calls `POST /api/databases/{id}/submissions` with an idempotency key. The node signs a `data` entry with `stream = proposal` on its own device stream, names the publisher grants it holds in `seen_heads`, and records the entry in `proposals.json` under the idempotency key. The same key with the same body returns the same entry; with a different body it is a conflict. A proposal carries the change (`create`, `replace` or `delete`) and the history entry it expects to find.
2. The sync loop uploads the entry. The relay stores it when the member still holds `submit` or `write`.
3. The publisher pulls member streams and decides each undecided proposal once, in the order the members signed them (Lamport order):
   - Accepted: the store validates and commits the change, and the publisher signs a direct entry with `incorporated_from` naming the proposal. The proposing member is the record's writer.
   - Not accepted: the publisher signs a `heads` entry that cites the proposal. This happens when the grant does not allow it, or when the record is no longer the one the proposal expected.
   - A failure that is not about the proposal (storage, signing) decides nothing; the next turn retries.
4. The member reads the decision from the publisher's stream. `POST /api/databases/{id}/submissions/status` answers:

| Status | Meaning |
|---|---|
| `submitted` | The publisher has not decided, or its decision has not arrived. |
| `published` | A publisher direct entry cites the proposal. Final. |
| `not_accepted` | A publisher `heads` entry cites the proposal. Final. |
| `unknown` | This node holds no such proposal. |

A proposal never changes the member's own copy. The record changes on the member only when the publisher's accepting entry arrives.

## Schemas

Each sync turn, before any upload, the publisher signs a `schema` entry for every schema its store declares that the history lacks or holds with an older definition. A write still signs its own schema first. A receiver's store holds only the schemas the history declares, so a schema with no record yet still reaches it.

## Local files

All under `<home>/log-sync/<database id>/`.

| File | Holds |
|---|---|
| `authored.json` | Every entry this node signed, in author order. |
| `uploaded.json` | One number: how many log entries the relay has acknowledged. |
| `received.json` | The accepted entries of every other stream, in Lamport order, so a restart rebuilds the history without the relay. |
| `proposals.json` | A member's ledger of its proposals, by idempotency key. |
| `blobs/<digest>` | The sealed blobs of the personal database's `bundle` entries, kept so a restart rebuilds the history without the relay. |

A restart replays `authored.json` and `received.json` together in Lamport order.

## What is not built

- Member direct writes and leases in the node (the `lease` entry kind and its counting rule are in `mutation_log`; see `write-leases.md`). A grant entry still carries a `cutoffs` field because the signed bytes include it; nothing reads it.
- Files in shared databases. A proposal that carries a file is refused.
- Payload encryption of `data` entries in a shared database. They are plain record key and field JSON, base64 on the wire, until the read key delivery for members is designed. Only bundle blobs are encrypted, and bundles are the publisher's alone. The personal database seals every payload: see [personal-database-signed-log.md](personal-database-signed-log.md).
