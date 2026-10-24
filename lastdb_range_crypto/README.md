# LastDB Range Crypto

Status: First implementation package, 2026-10-24. The active disk codec does not select this format.

This Apache-2.0 crate implements the published CLWW-derived exact OPE construction with backward carry and high-bit placement.
The [construction note](https://cipherstash.com/research/backward-carry-ope) describes this later extension.
The [original CLWW paper](https://saweis.net/pdfs/pore-clww.pdf) describes the underlying prefix-based construction.
The implementation uses existing RustCrypto primitives and no vendor OPE dependency or copied vendor source.

The later extension and this exact byte profile need independent cryptographic review before activation.
The original paper's security proof does not automatically cover this extension.
This crate makes no claim of an independent audit or an order-only leakage guarantee.

## Purpose

A user with the private identity seed can create deterministic, sortable terms for complete range values.
The cloud can compare existing terms without the private key.
Key creation requires no directory preload, server call, or registration of each range value.
Only values under one molecule, blinded hash, and epoch need comparable terms.

The public API provides private-key derivation, complete range terms, prefix interval bounds, and bounded canonical text conversion.
It does not provide an atom cipher, an original-range decoder, a cloud protocol, or a database migration.

## Private-key derivation

The input is the user's 32-byte private Ed25519 identity seed, rather than a public key or a serialized key file.
`RangeOpeRoot::from_private_key_seed` derives a separate 32-byte root through HKDF-SHA256.
It uses salt `lastdb:range-ope:private-seed:v1` and info `lastdb:range-ope:root-key:v1`.
This root is independent of the existing content and hash purposes.

The molecule key uses HKDF-Expand from that root.
Its info contains these four length-framed fields:

1. `lastdb:range-ope:molecule-key:v1`.
2. The exact `FORMAT_ID`.
3. The exact UTF-8 molecule identity.
4. The eight-byte big-endian epoch.

The hash key uses HKDF-Expand from the molecule key.
Its info contains the length-framed label `lastdb:range-ope:hash-key:v1` and the stored blinded hash.
Each field length is an unsigned four-byte big-endian integer.
A scope component must not exceed 65,535 bytes; the molecule identity must be nonempty.
An empty hash is framed explicitly and does not alias an absent field.

This hierarchy permits a later grant of a molecule secret without disclosure of the account root.
The protected grant API remains later work; this crate exports no plaintext key accessor.
The same private seed and scope produce the same term across devices.
The new format does not draw its OPE key from the existing random molecule bundle.
Existing bundles require an explicit integration and conversion decision.

Private key holders use zeroizing storage and redacted debug output.
Owned intermediate plaintext and keystream buffers use zeroizing storage where available.
The cryptographic libraries' internal stack copies do not receive a full memory-erasure guarantee from this crate.

## Frozen candidate byte profile

The identity is `lastdb:clww-carry128:hmac-sha256:v1`.
Changing the derivation, PRF, framing, marker, or carry rule creates a new incompatible profile.
This profile is not byte-compatible with the vendor's BLAKE3 implementation.

The logical input is the exact complete byte string, with a fixed byte `0x01` before it.
The marker also precedes an empty input.
Text receives no case conversion, Unicode normalization, numeric interpretation, or truncation.
The complete input length is not part of the PRF input, because that would destroy common-prefix order.

For input bit position `i`, obtain the first byte of:

```text
HMAC-SHA256(scoped_key,
    "lastdb:range-ope:prefix-prf:v1\0" || FORMAT_ID || prior_bits)
```

Process bits most significant first. Each prior bit occupies one byte, `0x00` or `0x01`.
Let `P` be those PRF bytes in big-endian order.
Let `B` be the plaintext bits, each also stored as one base-256 digit.
The ciphertext is `P + 128*B`, with a reserved leading byte for the final carry.
The implementation computes the carry from right to left.

The prefix PRF is a keyed function of all earlier bits, rather than a separate map for each independent input byte.
The carry rule retains the complete integer sum.
Two values first differ under the same PRF state, so the high-bit signal determines the direction before the later digits.
If one value is a prefix of another, the extra digits can add a carry to the common prefix or leave it equal.
The shorter value sorts first in both cases.
This explains order correctness; it does not prove the privacy of the construction.

Raw term size is `8*N + 9` bytes for `N` logical input bytes.
Canonical lowercase hex size is `16*N + 18` bytes.
An empty range therefore has a real nine-byte term.
The leading carry is zero or one, and the raw size is congruent to one modulo eight.
The parser checks shape and canonical lowercase hex, rather than authenticity or scope.
Keep terms from different scopes out of the same ordered stream.

## Prefix reads and resource limits

For prefix `p`, create the interval `[encrypt(p), encrypt(successor(p)))`.
The successor increments the rightmost byte below `0xff` and removes the suffix after it.
No successor means the end of that named hash stream.
The bound may contain invalid UTF-8 bytes; it is not an API text value.
An empty prefix selects the complete named hash stream.
An encrypted prefix is not used as a ciphertext byte prefix.

`max_term_bytes` bounds each raw output before allocation.
`max_text_bytes` bounds the complete hexadecimal output before allocation.
The prefix method may allocate two bounded terms and one bounded temporary plaintext successor.
The crate imposes no small integer domain or fixed logical range length.
Native callers must account for the entire stored key frame, not only this term.

The current native format caps an entire stored key at 65,535 bytes.
Its current `ope_v1` expands a logical range eight times; this profile's hex form expands it sixteen times, plus framing.
Preserve the full existing logical range allowance through an explicit new format before activation.
Do not silently reduce that allowance, truncate a value, or hash it into a narrow domain.

## Remaining release work

The [implementation plan](../docs/designs/ope-range-keys-implementation-plan.md) covers encrypted original ranges, all readers and writers, conversion, and restore.
Those changes precede selection of the new native codec.
The future cloud protocol also needs authorization, coherent revisions, freshness, and response-completeness rules.

CAUTION: Terms expose order, equality, length, and additional structure. They do not replace authenticated content encryption.

Do not assume that the later variant exposes only the leakage proved for original CLWW ORE.
Its full leakage analysis is part of the independent release review.
Product use remains blocked on that review and completion of the integration packages.
