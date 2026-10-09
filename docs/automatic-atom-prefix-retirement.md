# Automatic atom prefix retirement

A snapshot can name a plain segment before GC appends its Delete record. The prefix and the longer local file have different SHA-256 digests. Atom compaction previously retired only the longer digest. A later snapshot could carry the old Put prefix into a restore.

The existing purged-atom receipt path now keeps exact atom refs from one confirmed manifest commit. A speculative cut or failed cloud CAS does not replace that cache. The cache stores chunk metadata, not atom IDs or payloads. Its header records the store UUID, epoch, counter, and manifest digest.

Before atom compaction, the store matches a cached ref to a local chunk address: collection, shard, group, and UUID. It verifies the declared prefix length and digest, plus the full local digest, from one open file. An exact match binds the prior prefix digest to that local chunk's retirement. This proof does not depend on a missing path or a matching UUID alone.

The durable pre-compaction record contains that relation. After compaction succeeds, a retired local chunk can also retire its proven prefixes. The next snapshot uses the existing exact purged-atom receipt. Pending digests clear only with a confirmed commit.

A failed or interrupted compaction cannot promote prefix claims through the older disk-absence recovery rule. Until recovery, a snapshot cut refuses the incomplete record. Recovery discards unfinished prefix claims and retains their remote refs. Historical refs without a surviving, provable local preimage still require the bounded owner rewrite operation.

Remote-only refs, prefix mismatches, duplicate addresses, and foreign store or epoch metadata supply no retirement authority. Native compaction still copies every live record into its successor. Shared values and unrelated namespaces use the same preservation rule.

## Serialization and commit order

One store-local mutex serializes atom compaction, manifest cuts, and confirmed commits. Clones share that mutex. The order is the outer publisher lock, this retirement mutex, then high-water or native shard locks. No network request or async wait occurs under the retirement mutex. Helpers that load or write the sidecar do not acquire it again.

The atomic sidecar update combines the new committed header and receipt cleanup. An identical repeated commit returns without cleanup, so it cannot erase retirements from a later compaction. A lower committed counter or a different digest at the same counter refuses the commit. A later reserved cut does not by itself advance this committed counter.

The cache replaces its prior refs on each confirmed commit. It does not accumulate manifest history. At more than 65,536 atom refs, it retains the small commit header with an empty ref set. This disables prefix inference but preserves duplicate and stale-commit checks. It does not block the cloud snapshot.

## Verification

The native plain regression commits S0 before Delete, then compacts and cuts a successor. It checks exact retirement receipts, physical deleted payload absence, retained and shared positive controls, and native chunk restore. A second cut before commit proves pending state survives a publication retry.

Negative tests cover scope, address, length, both digests, remote-only refs, duplicate addresses, incomplete compaction, same-counter conflicts, stale commits, and the cache limit. A maximum-size fixture reports serialization and durable-write cost without a time threshold.

The manifest wire format and receipt validator do not change. This is a forward fix. An upgrade does not infer authority for an older manifest until a confirmed commit supplies its refs.
