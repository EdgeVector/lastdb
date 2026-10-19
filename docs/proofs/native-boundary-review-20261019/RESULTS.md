# Native boundary review

The review found three incorrect rules and one obsolete memory control.
The changes use the current core model and native disk format.

- A cleared optional lookup field could leave its old related key readable.
  A delete could also remove a related key after another source replaced that association.
  Moves, Delete, and Purge now require the same current association proof.
  Missing, null, and non-scalar optional lookup values name no related key.
- Later writes in one request could copy values from before an earlier write.
  Each schema prepares from the current private state immediately before its stage.
  A repeated canonical key starts the next stage.
  Independent keys remain batched, and the complete create/update batch publishes once.
- Shared molecules could expose another key layout through reads, counts, pages, or samples.
  One membership rule now checks both declared key fields before the result limit.
  Administrative pages recover usable keys and establish a complete bounded set before the page slice.
  Samples stop after enough valid records.
- The memory sampler called a retired control and reported a purge that did not occur.
  It now reports the measured difference and captures native counts after actual pressure relief.

Per-field conflict rules remain in effect.
A mixed related record does not prove that one source owns its complete association.
Source cleanup leaves that unproved association unchanged.
The code adds no ownership map or second write buffer.

The normal release comes from `fca23b6ff69613adc42d683dd95e071c861c61f7`.
All 1,890 source inputs match the immutable commit and the worktree.
The executable contains 67,836,816 bytes.
Its SHA-256 is `cdbabbee5b79238469d805226c60b9092e4fdebf9b9d98ebc92d86aae50f2a4c`.
The compact identity reconstructs from the previous identity and 30 changed or new inputs.
All 11 existing operation helpers remain unchanged.

The full workspace and separate feature lanes pass Clippy with warnings denied.
The release build, format, function size, file size, new-allow, artifact, registry, write-path, and workspace checks pass.
No tests or new executable check suite run in this review.

Two accepted runs use fresh synthetic data with the normal executable.
One uses blind hashes and OPE ranges; the other uses plain hashes and ranges.
Each run sends 35 mutations and checks 86 query results before and after a normal reopen.
The accepted results preserve cleared keys, current shared keys, moved values, mapped membership, counts, and page order.
The existing core write helper also passes 74 read observations, including 29 observations with no file reads.
Its checks include queued writes, durable writes, updates, deletes, reopens, and concurrent reads.
These observations establish correctness and file-read behavior, with no throughput or latency claim.

Two earlier operation attempts use the same accepted executable.
Their administrative requests go through the keyed batch route, which deliberately removes the administrative override header.
Those attempts contain 14 and seven correctly refused query results.
Both `true` and `1` are valid on the single-request route; the earlier header-value diagnosis was incorrect.
The accepted runs use that supported route in parallel and retain batches for keyed reads.
The request bodies and required record values remain unchanged.
The earlier raw responses and separate failed reviews remain available.

Administrative pages and counts require completeness within the existing 10,000-candidate and byte allowances.
They return an explicit error when the allowance cannot establish that complete set.
The primary body refill remains limited to 1,024 records per batch for ordinary paged range reads.
The declared range field can add a cold field fetch; warm calls reuse the core model.
The batch API continues to accept keyed reads only.

The receipt seal covers five operation roots and 11 normal, unforced node exits.
Eight exits belong to the accepted runs; three belong to the refused administrative attempts.
The separate executable-only baseline copy starts no node and uses the supported exact cleanup.
Cleanup records state the final disposition of each owned root.
The final independent review occurs after cleanup, so it cannot read the deleted private executable copies again.
The preserved inputs, copy checks, and direct release-executable check establish the recorded executable identity.

The source, release, operation, and cleanup reports in this directory record the independent reviews.
Raw HTTP bodies remain in the operation directories.
The larger local receipt files and build logs remain beside them under the repository's existing ignore rules.
