# Backup retirement and chunk versions

A chunk identity contains its collection, shard, group, and UUID. Its digest identifies one byte version of that chunk.

A backup cut can replace a predecessor digest with a newer digest under the same identity. Absence of the newer digest does not prove absence of the predecessor digest.

The retirement paths now retain the exact predecessor reference when only the newer version has absence proof. This applies to unbackable atoms and named holes. The next cloud presence check covers the retained digest. A later receipt can remove that reference only with separate absence proof.

The publisher also validates the final manifest against its exact predecessor before a manifest upload or latest-pointer update. This check occurs after all retirement changes. The restore validator keeps its existing rules.

The tests cover:

- A changed digest under one identity, followed by each retirement path.
- Retention of the predecessor digest, byte count, and CSN.
- A later valid retirement with separate predecessor absence proof.
- Refusal before network publication for an invalid final manifest or an absent predecessor.
- A real publisher call that checks the predecessor digest before publication.

## Existing invalid cloud chains

This change prevents new invalid steps. It does not change previously published objects.

The September 2026 investigation found 11 invalid transitions in a 43-manifest chain. All object hashes matched. The restore validator correctly refused the chain.

Recovery must preserve valid erasure receipts and all still-recoverable references. A new hash chain needs separate validation and a successful restore before any claim of recovery. Do not replace missing evidence with a fabricated receipt or relax the restore check.
