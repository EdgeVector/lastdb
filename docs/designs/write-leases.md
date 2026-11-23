# Design: write leases, one per database

Date: 2026-11-22. Author: Claude, for Tom. Status: approved in direction by Tom on 2026-11-22. Tom said to build it on 2026-11-23. The entry kind and the counting rule are built (section 13); the node is built after them.

What he approved: timed leases with no emergency revoke, a length the publisher chooses, one lease for the whole database (membership stays as grant entries), and a node that keeps the lease on disk and decides each write locally, without asking the relay, until the lease is close to its end.

The relay protocol as built is in `relay-protocol.md`. The sharing model as built (members propose, the publisher accepts or signs not accepted) is in `signed-history-sharing.md`. This document adds one entry kind on top of both.

## 1. The idea

Who is a member and how long members may write are two separate facts.

- **Membership** is a list the publisher keeps by signing "add" and "change" entries, one per person. These are today's grant entries. They have no end time.
- **The lease** is one entry for the whole database: "everyone who is a writer as of this entry may write until this time." The publisher's node signs a new one on a schedule.

While a writer is covered by an unexpired lease, its writes count at once and nobody approves them. When no lease covers it, its node proposes instead, as in a share.

## 2. Why one lease for the database

| | Lease per member (first version) | Lease per database |
|---|---|---|
| Publisher signs, per renewal | One entry per member | One entry |
| A member's node downloads, per renewal | Everyone's leases | One entry |
| Removing a member | A final lease for that member, then a role change | One "change" entry. The next lease does not cover them |
| Change to the grant entry format | Yes | None. A new entry kind instead |
| Existing write grants | Must be signed again | Stay as they are |

Exemem's check does not change. It keeps one row per member from the membership entries and answers "may this key deposit entries" with one lookup. It does not read leases.

## 3. Roles

| Role | Covered by a live lease | Not covered |
|---|---|---|
| `write` | Writes directly | Proposes. The publisher accepts a change to any record |
| `submit` | (never covered) | Proposes. Creates records and changes the ones it created. A direct entry it signed earlier, as a writer, is stored and ignored |
| `read` | (never covered) | Reads |

Deliver, Share and Org stay presets of these roles in People.

## 4. The lease entry

A new publisher entry kind, `lease`:

| Field | Meaning |
|---|---|
| `expires_at_ms` | A covered member's node signs direct entries only before this time, by its own clock |
| `closes_at_ms` | Later than `expires_at_ms`. From this time the lease may be closed. The gap is the time a member has to get its entries to the relay |
| `closed` | For leases this entry closes: per device stream that wrote under them, the last entry the publisher had accepted |

A lease covers every key whose role is `write` in the membership entries that come before it on the publisher's chain.

### Choosing the length

The publisher chooses per database, to fit how often its devices are online (Tom, 2026-11-22). It renews when half the lease is left, so members keep direct writes while the publisher is never away for more than half a lease. Rule of thumb: twice the longest gap you expect to be offline.

| Publisher's devices | Lease length |
|---|---|
| Always on | 1 to 24 hours |
| On most days | 3 to 7 days |
| Occasionally | 2 to 4 weeks |

## 5. The rules

### A member's node

The node keeps the newest lease on its own disk, with the other entries it received. Every write is decided from that local copy and the node's clock. The node does not ask Exemem, or anyone, before a write (Tom, 2026-11-22).

1. It signs a direct entry only while the newest lease it holds covers it and has `expires_at_ms` in the future by its own clock, less a small margin. The entry names that lease. The record is in its database at once.
2. Otherwise a change goes out as a proposal, and the node's database does not change until the publisher accepts it.
3. It fetches the next lease in the background, when it syncs. The publisher signs one at half life, so a node that syncs now and then always holds a lease with time left.
4. The lease survives a restart and works offline, because it is a signed entry on disk. The app can show the time left ("write access until 14:00 tomorrow").

The margin covers a clock that runs a little slow: the node stops signing direct entries a few minutes before the expiry, and proposes from there.

### The publisher's node

1. **Renew.** Sign a new lease when half of the newest one is left.
2. **After a membership change**, sign a new lease at once, so a new writer is covered without waiting.
3. **Close.** A new lease entry closes every earlier lease whose `closes_at_ms` has passed, and records how far each stream that wrote under it had got.
4. Any device of the publisher may do these.

There is no way to end a lease early (Tom, 2026-11-22). Every lease has an end time.

### Every node: does a member's direct entry count

A direct entry by member M, on stream s, at sequence n, naming lease L, joins the stream when L is an accepted lease entry and M's role was `write` as of L.

It counts unless L is closed and the entry lies past the closing point:

- L is closed by the earliest later lease entry signed at or after L's `closes_at_ms`.
- If that entry lists stream s, entries on s naming L with a sequence above the listed one do not count. If it does not list s, no entry on s naming L counts.
- An entry at or before the closing point is final.

The rule reads only signed entries. No node's clock takes part. When a lease is closed, a node recomputes the records that the newly excluded entries wrote, and the member's node signs each excluded change again as a proposal if it may still propose.

**Expiry binds honest signers. Closing binds everyone.**

### The relay

The relay is a mailbox (Tom, 2026-11-23). A member holding `write` or `submit` may deposit proposals and direct data on its own stream. A member holding `write` may also deposit bundles. The relay does not read lease entries. The nodes alone decide what counts, so a direct entry that no lease covers, such as one from a `submit` member, is stored and ignored. Section 13, item 9 gives the reason.

## 6. A day in the life

Lease 24 hours, renewed when 12 remain, closed 12 hours after expiry.

| Time | Publisher signs | Dana, a writer |
|---|---|---|
| 0h | L0: expires 24h, closes 36h | Writes directly, naming L0 |
| 12h | L1: expires 36h, closes 48h | Pulls L1. New entries name L1 |
| 24h | L2: expires 48h | L0 has expired |
| 36h | L3: expires 60h. It closes L0 and records how far Dana's stream had got | Everything she signed under L0 and uploaded by now is final |

Removing Dana at 20h:

| Time | Publisher signs | Dana |
|---|---|---|
| 20h | "Dana: submit", then a lease that expires at 44h. It does not cover her | Her node pulls both. L1 still covers her until 36h. Her app says "write access ends at 36h" |
| 36h | | Her node stops writing directly and proposes |
| 56h | The first lease signed after L1's closing time (48h). It closes L1 | Everything she uploaded by now is final |

## 7. Edge cases

| Case | Outcome |
|---|---|
| Writer writes and uploads within the window | Counts. Final at the next close |
| Writer is removed | Writes until the lease that covers her expires. Nothing is taken back |
| Writer offline: wrote before expiry, uploads after the lease was closed | **Taken back.** Her node signs the same changes again as proposals, if she may still propose |
| Publisher offline longer than the lease | The lease lapses. Writers propose. Their changes wait as pending and are decided when the publisher returns |
| New writer added | Covered by the lease the publisher signs right after adding her |
| Writer's clock is slow | It may sign after expiry. Those entries count until the lease is closed |
| Writer's clock is fast | It stops early and proposes |
| Stolen writer device | It can write until the lease that covers it is closed. A shorter lease shortens this |
| Two writers change one record | Higher position wins (Lamport, node key, device) |
| A writer adds a second device | Covered by the same lease. A stream the publisher never saw has nothing recorded at closing |
| Two publisher devices sign leases at the same time | Both are valid. A member names the newer. Closing uses position order, which is total |

## 8. Scale

| Size | Works how |
|---|---|
| Up to thousands of writers | As designed. Renewal is one entry. Each member downloads the member list once and one lease per period |
| A very large audience of readers | No member list. One grant of `read` to `any` |
| A very large number of writers | Not this design. Every member would download the whole list. The fix is for the publisher to sign one fingerprint of the list and for each writer to attach a short proof of being on it. A later design |

## 9. What changes in the code

| Where | Change |
|---|---|
| Entry format (`mutation_log`, Exemem entry parser, OpenAPI) | New kind `lease`. The grant entry does not change |
| `mutation_log` | A member's direct entry joins its stream when it names a lease that covers its signer. The counting rule and the recompute at closing |
| Node, member side | Sign directly while covered, otherwise propose. Sign excluded entries again as proposals |
| Node, publisher side | Renew, close, the lease length setting |
| Relay | `write` may deposit direct data and bundles |
| App API and SDK | One "change this record" call that answers applied or pending |
| People | Org preset. Shows the end time. An org recipe in the DEV proof |

## 10. What it costs

1. The recompute at closing comes back. It was the hard part of this week.
2. Removing a writer takes up to one lease length.
3. The publisher must be online once per half lease, or writers drop to proposing until it returns.
4. One rare case takes a write back (section 7).
5. A new entry kind in LastDB and in Exemem's parser.

## 11. Order of work

1. Prove option B in People CI (in progress). It is what a database does when no lease covers a member.
2. The `lease` entry kind, in LastDB and Exemem.
3. Counting rule and recompute in `mutation_log`, with proof scenarios.
4. Member side and publisher side in the node.
5. Relay door for `write`.
6. App call, SDK, People preset and recipe.

## 12. Decisions

Settled by Tom on 2026-11-22:

1. No emergency revoke for now.
2. The publisher chooses the lease length per database.
3. One lease per database, as a new entry kind. Membership stays as grant entries.
4. A node keeps the lease on disk and decides each write locally until the lease is close to its end.

Proposed by Claude and shown to Tom with the design, not separately confirmed:

1. The publisher signs a fresh lease right after a membership change.
2. One "change this record" app call that answers applied or pending.

No longer needed: signing existing write grants again.

## 13. Build choices (2026-11-23)

Where this document left a detail open, the build chose the simplest thing.

1. **Naming the lease.** A member's direct entry names its lease by citing exactly
   one `lease` entry in `seen_heads`. No new field and no change to the data entry
   format. Citing it also makes the order value rule hold and keeps the entry pending
   until the node holds the lease. `grant_at_device` and `grant_at_seq` stay absent.
2. **"Before it on the publisher's chain."** The grants before a lease are the
   publisher's grant entries with a lower position `(lamport, node_key, device_id)`.
   A member's role as of a lease is the last such grant that names the member.
   The check is made when the member's entry is accepted. A grant of a second
   publisher device that arrives later with a lower position does not change an
   entry that was already accepted.
3. **Accepted versus counting.** A direct entry of a member joins its stream when it
   names exactly one accepted lease, whatever the member's role. Whether it counts is a
   separate fact: it counts only when the member was `write` as of that lease and the lease
   is not closed past it. The fact is recomputed whenever a lease entry is accepted. An excluded entry stays
   in its stream (the chain stays gapless) and writes nothing.
4. **Closing.** A lease L is closed by the earliest later lease entry, in position
   order, whose `authored_at_ms` is at or after L's `closes_at_ms`. That entry's `closed`
   list gives one point per stream, and it applies to every lease the entry closes. A
   stream it does not list has point 0. The publisher fills `closed` with
   `VerifiedHistory::closing_streams(authored_at_ms)`: for each lease with no closing entry
   whose `closes_at_ms` has passed, each stream that wrote under it, at the head the
   publisher holds for that stream.
5. **The recompute.** When a lease entry changes which entries count, each record that
   those entries wrote is recomputed as the winner among the counting data entries that
   wrote it, and the bundle writes that a non-bundle write shadowed or displaced. The
   record's creator is the earliest remaining write. A record with no remaining write is
   removed.
6. **Bundles.** A bundle is the publisher's, as before. A relay admits a `write` member's
   bundle (the protocol says so), and a node ignores it.
7. **`closed` size.** At most 65535 streams, strictly ascending by node key and device id.
8. **Expiry is not checked by nodes.** Expiry binds honest signers; a node never reads a
   clock to decide whether an entry counts.
9. **The relay and a reduced writer (resolved, Tom, 2026-11-23).** The relay admits direct
   `data` entries from `submit` members as well as from `write` members. A writer reduced to
   `submit` may hold direct entries it signed under the earlier lease and did not upload. A
   device stream has no gaps, so if the relay refused those entries (403), the writer's later
   entries, its proposals, could never be deposited. The relay therefore does not read roles
   for direct entries beyond "may deposit at all", and the nodes decide what counts. A direct
   entry that the member's role as of the named lease does not cover is accepted into its
   stream by `mutation_log` and never counts (it is in the excluded set), so it does not stop
   the entries after it. Bundles and file uploads stay with `write`. `read`, revoked and
   unknown callers deposit nothing, and `lease` stays the publisher's.
