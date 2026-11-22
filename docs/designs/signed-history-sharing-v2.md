# Design: signed-history sharing, second pass (draft)

Date: 2026-11-22. Author: Claude, for Tom. Status: approved by Tom, 2026-11-22. Step 1 (relay admits by grant, past-cutoff entries join the chain) is in the change that adds this file.
Builds on `docs/designs/signed-history-single-log.md` and LastDB #1826 to #1833.

## Why a second pass

The People DEV proof took eight PRs because the sync, the relay and the local database each decided for themselves what counts. Every failure was two of them disagreeing:

| Failure | Disagreement |
|---|---|
| Member's write refused, stream stalls | The node says "void by cutoff". The relay says "refused by current role". |
| Member's replace never applied | Each node stamps its own record revision. |
| Anyone could edit the publisher's comment | Ownership was a FoldDB stamp that the winner overwrites. |
| Phantom record on the member | The local database was written before the history accepted the write. |
| A day lost to a "wipe" | A sticky node-wide error and a label that meant the opposite. |

## Principles

1. **One owner per fact.** Each question has one system that answers it. Everyone else asks that system or derives from it.
2. **The history is the truth, the database is a view.** A shared database's records are a pure function of the accepted entries and the publisher's grants.
3. **The relay is a mailbox, not a judge.** It decides who may deposit. Nodes decide what counts.
4. **Never rewrite a signed entry.** Mistakes are voided by later signed facts. They are never edited away.

## Who owns what

| Fact | Owner | Rule |
|---|---|---|
| Does an entry count? | History (`AcceptEntry` plus cutoffs) | A pure function of the entry and the publisher's grants. Re-evaluated when grants change. |
| May a node deposit an entry? | Relay | Only that the caller holds a current grant (read does not count, so submit or write) or is the publisher. Not which kind of entry. |
| Order and continuity | The device stream | Consecutive `writer_seq`, each naming the previous hash. |
| What is a record's value, author and revision | History | Winner by position. Revision is the winning entry's hash. |
| Who owns a record | History | The writer of the earliest write by position. Only the owner may replace or delete it by proposal. |
| What is pending upload | The log and its cursor | Entries past `uploaded.json`. |
| What a database shows | Materializer | The fold of counted entries, nothing else. |
| What went wrong | Per-database status | Pending, last uploaded, last error, voided count. |

## What changes

### 1. The relay stops judging kinds of entry

Today the relay refuses a direct entry from a member whose role is below write, while nodes use a per-stream cutoff. The two rules disagree, and a refused entry stops the chain behind it.

The relay accepts an entry from any caller that holds a current non-revoked grant, whatever the entry's kind. Nodes already discard entries that do not count. A downgraded member's late direct entry is then stored, the chain stays continuous, and every node ignores it by cutoff.

Abuse is not new: a submit member can already append any number of proposals. A revoked member holds no grant, so the relay refuses it, and the publisher's storage bill is bounded by the same paid-storage gate as today.

### 1a. A past-cutoff entry joins the chain

The same rule applies in `AcceptEntry`. An entry past its writer's reduction cutoff used to be rejected, so it never entered the chain and the entries after it waited for a predecessor that would not come. It is now accepted into the stream chain with no effect on records. `within_cutoff` decides whether an entry counts, both when it is accepted and when a later publisher grant changes the cutoffs.

### 2. A refused entry never stalls a stream

With the relay change, the relay refuses only a caller that holds no grant, meaning a revoked or never-granted member. Nothing that member signs counts, so nothing useful waits behind a refused entry. The upload step reports the refusal in the per-database status instead of retrying silently, so a stuck stream is visible.

### 3. The member's intent survives

When a member's own unacknowledged direct entry is voided, the node appends a proposal with the same change on the same stream, at the next sequence number. The void entry stays in the log. The publisher applies the proposal under the creator and history-entry checks. The member's local copy goes back to the published state, which fits the 17 November rule that a proposal does not change the receiver's copy.

### 4. The local database becomes a derived view (direction)

A direct write today commits to FoldDB first and signs second. If signing fails, or the entry is later voided, the database holds a record that history does not. The materializer needs a special delete step for it.

The direction: validate the write, accept the entry into the history, then materialize. The database is then only ever the fold of counted entries. Void entries, undo and prune all reduce to one operation, a refold. This needs a validate-without-commit call on FoldDB, so it is a separate, later change.

### 5. Visible state

`cloud_sync.databases[id]` reports log length, acknowledged count, last error, and the number of voided entries. People prints per-item read errors. The node-wide `last_error` goes once People and the CLI stop reading it.

### 6. A publisher's history carries every declared schema

A receiver's store holds only the schemas the signed history declares. Schema entries used to be signed only when a record of that schema was first written, so a schema declared by a setup and not written yet never reached a receiver, and a read of it there named an unknown schema. Each sync turn now signs a schema entry for every schema the publisher's store declares that the history lacks, or holds with an older definition. A write still signs its own schema first, so a schema entry always precedes the data that needs it.

## Build order

1. Relay admits by grant, not by entry kind (Exemem Lambda in `exemem-infra`, and the reference relay). This clears the stall.
2. Convert a voided direct entry into a proposal on the same stream.
3. Derived local database for received writes (needs the FoldDB validate call).
4. Drop the node-wide `last_error`.

## What stays

Single log with cursor, history-level compare-and-set, history ownership, winner authorship, void by cutoff, Exemem as the relay, per-database status.

## Risks and open questions

1. **Relay change is in the deploy path.** A merge to `exemem-infra` main deploys to DEV and then to PROD in stages. It needs the People DEV proof first.
2. **A downgraded member can store junk.** It costs the publisher's storage, and it can be stopped by revoking the member. A per-member byte cap is possible later, but it is not proposed now.
3. **Old relays.** The reference relay and the Lambda must move together, since a node cannot tell which rule a relay applies.
4. **Conversion to a proposal.** The proposal carries no `expected` for a direct create, and for a replace it carries the entry the member last saw. That needs one more check against the creator rule.
5. **Not verified by a run.** The stall was derived from the code (the Lambda's per-entry append, the reader's consecutive-sequence loop, and `AcceptEntry`'s predecessor rule), and it fits the People log line `pending 1, refused 403`. A run on a stalled stream would confirm it.
