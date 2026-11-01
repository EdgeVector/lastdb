# OPE candidate security review

Status: Carry-profile candidate rejected and PR 365 closed without merge, 2026-10-24.

This finding applies to `lastdb:clww-carry128:hmac-sha256:v1`.
The replacement binary CLWW profile uses a distinct identity and requires its own review.

The [independent automated review](https://github.com/EdgeVector/lastdb/pull/365#discussion_r4368589478) found a concrete suffix-leakage counterexample.
This is a code review result, not an independent professional cryptographic audit.
The candidate remains disconnected from native readers and writers.

## Finding

The [original CLWW paper](https://saweis.net/pdfs/pore-clww.pdf) defines leakage that includes order and the first differing bit position.
Its definition does not permit arbitrary information about later plaintext bits.
The vendor [extension note](https://cipherstash.com/research/backward-carry-ope) claims that its transform adds no leakage.
The following counterexample contradicts that claim for the candidate's exact profile.

Compare two possible one-byte datasets in one field key, hash, and epoch:

- Dataset A contains `0x00` and `0x80`.
- Dataset B contains `0x40` and `0xc0`.

Both datasets contain two equal-length, ordered values.
Their first differing bit occupies the same position.
Their later plaintext bits differ between the datasets.
Thus, the stated CLWW leakage is identical for A and B.

Let `T(x)` interpret the first ten raw ciphertext bytes for `x` as a big-endian integer.
Those bytes include the reserved byte, the fixed marker, and the first plaintext bit's output.
The common-prefix PRF values cancel in the difference.

For A, the remaining bits are zero and generate no suffix carry.
Thus, `T(0x80) - T(0x00)` always equals 128.
For B, the second plaintext bit is one and its PRF depends on the first bit.
Independent suffix carries make `T(0xc0) - T(0x40)` equal 127 or 129 with probability near one half.
A public observer can therefore distinguish datasets with identical CLWW leakage.
The observer needs no private key, original-range payload, or encryption query.

The single authorized crypto proof program demonstrates this distinction with fixed generated inputs and 256 generated keys in CI.
It emits only aggregate results, without real keys or database data.
Its existing order checks still serve a different purpose: correct order does not establish acceptable confidentiality.

## Release decision

Do not activate this candidate through a native selector, default, converter, or primary upgrade.
Do not describe its privacy as inherited from the original CLWW result.
Independent original-range encryption does not remove leakage already present in the searchable term.
Generic warnings about additional structure do not resolve this specific decision.

The next route needs an explicit choice:

1. Investigate another published OPE construction with full-range support and acceptable storage and CPU cost.
2. Select a published ORE construction with a documented leakage model and implement its comparator in the relevant storage readers.
3. Assess this candidate's extra leakage explicitly before any decision to retain it.

The third route requires further analysis; acceptance of one counterexample is not a complete cryptographic review.
The first route must not replace full range values with hashes or a narrow integer domain.
The second route changes the native comparison contract and the future cloud query plan.

Keep any incomplete integration source inactive until that choice and the subsequent review are complete.
The primary, cloud configuration, identity key, and archived primary stay unchanged.
