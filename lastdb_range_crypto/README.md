# LastDB Range Crypto

Status: Replacement candidate, 2026-10-24. Native readers and writers do not select this format.

This crate independently implements binary CLWW OPE from the [FSE 2016 paper, Section 3.2](https://saweis.net/pdfs/pore-clww.pdf).
Its PRF excludes the maximum 128-bit value, so addition of a plaintext bit cannot wrap.
Independent output blocks replace the rejected extension's suffix carry.
The [earlier security finding](../docs/designs/ope-candidate-security-review.md) applies to the closed carry-profile candidate.
This replacement still requires independent review of its exact PRF, framing, key grants, and protected original ranges before native activation.
No restricted vendor source or OPE dependency is used.

## Published construction and PRF instantiation

For each plaintext bit, CLWW emits `u = F(key, position, prior_bits) + bit mod M`.
This profile uses `M = 2^128` and a separate 16-byte block for every bit.
The ordinary unsigned byte order compares the first unequal block.
All later output blocks are irrelevant to that comparison.

The LastDB PRF derives a candidate from the first 16 bytes of HMAC-SHA256.
It rejects the all-`0xff` block and tries the next counter, for at most 16 draws.
Thus, every returned block lies between zero and `2^128-2`.
Addition of zero or one stays within 128 bits.
An exhausted sampler returns `PrfRefused`; it produces no ciphertext.
A caller must refuse that write before its ack.

In the random-function model, a returned block is uniform over all non-maximum 128-bit values.
Coupling it to a uniform 128-bit block fails with probability at most `Q/2^128` across `Q` prefix evaluations.
Add the HMAC PRF advantage to that bound.
The 16-draw refusal probability for one prefix is `2^-2048` in that model.
These are review arguments for this instantiation, not an independent audit or a promise of order-only secrecy.

## Exact stored-byte profile

The identity is `lastdb:clww-binary128-nonwrapping:hmac-sha256:v1`.
The rejected identity was `lastdb:clww-carry128:hmac-sha256:v1`; its terms are incompatible.
A change to the PRF, framing, derivation, or block format requires another identity.

Prepend the fixed byte `0x01` to the complete logical input, including an empty value.
Read all bits most significant first.
Encode each prior bit as one byte, zero or one.
Do not normalize text, narrow the domain, or truncate the range.
Do not include the complete input length in the prefix PRF context.
Position is implicit in the length of the prior-bit sequence.

For each counter from zero through 15, obtain the first 16 bytes of:

```text
HMAC-SHA256(scoped_key,
  "lastdb:range-ope:binary-prefix-prf:v1\0" || FORMAT_ID || prior_bits ||
  0xff || counter_u32_big_endian)
```

The terminator cannot appear among the prior-bit bytes.
The fixed-width counter makes rejection draws distinct.
After the first accepted block, add the input bit as a big-endian integer.
Append that block and update the prefix state with the plaintext bit.
There is no carry into another block and no reserved leading ciphertext byte.
The first unequal plaintext bit therefore changes its complete output block by exactly one.
A shorter plaintext produces a proper ciphertext prefix and sorts first.

Raw term size is `128*(N+1)` bytes for `N` logical input bytes.
Canonical lowercase hex size is `256*(N+1)` bytes.
The empty range has a real 128-byte term.
Parsing checks canonical hex and a positive raw size divisible by 128; it does not authenticate a term.

For prefix `p`, select `[encrypt(p), encrypt(successor(p)))`.
The successor increments the rightmost byte below `0xff` and removes the suffix.
An empty or all-`0xff` prefix has no upper bound within that named hash.
Bounds accept raw bytes, including a successor that is not valid UTF-8.

## Private-key derivation

The root input is the user's 32-byte private Ed25519 identity seed.
Use HKDF-SHA256 with salt `lastdb:range-ope:private-seed:v1` and info `lastdb:range-ope:root-key:v1`.
This purpose is separate from the current content and hash purposes.

Derive a molecule key with HKDF-Expand and these length-framed fields:

1. `lastdb:range-ope:molecule-key:v1`.
2. The exact `FORMAT_ID`.
3. The exact molecule identity in UTF-8.
4. The eight-byte big-endian epoch.

Derive the OPE stream key from that molecule key with the framed label `lastdb:range-ope:hash-key:v1` and stored blinded hash.
Each field length uses four unsigned big-endian bytes.
A scope component has a 65,535-byte cap; a molecule identity cannot be empty.
An empty hash is explicit and does not alias an absent context.
The same private seed and scope produce the same terms across devices.
The current random molecule bundles do not define this new range root.

Key holders use zeroizing storage and redacted debug output.
The crate supplies no raw secret accessor.
Library-internal stack copies receive no complete memory-erasure guarantee.

## Protected original ranges and scoped grants

`RangeOpeRoot::for_molecule` derives a molecule secret without disclosure of the account root.
`RangeMoleculeKey::for_hash` derives its exact OPE stream key.

`protect_range` prepares the term and an independently encrypted original range.
Its key uses HKDF-Expand from the molecule key with label `lastdb:range-ope:original-key:v1` and the blinded hash.
The cipher is AES-256-GCM-SIV with a fresh random 12-byte nonce.
Its envelope is `[version=1][nonce:12][ciphertext][tag:16]`.

The authenticated context frames `lastdb:range-ope:original-context:v1`, the format, molecule, epoch, blinded hash, and SHA-256 of the term.
The digest binds the entire term without a scope-field size reduction.
`open_range` authenticates the context, decrypts the original, and checks its regenerated term.
A changed scope, term, or ciphertext produces refusal.
A prepared write retry must reuse the complete prepared value.

`wrap_for_access` protects only the molecule secret under an authorized access-domain key.
Its context frames `lastdb:range-ope:grant-context:v1`, the format, molecule, epoch, and exact nonempty access domain.
It uses the same nonce and envelope rules and a fixed 32-byte secret payload.
`from_access_wrap` authenticates that context before it creates a recipient key holder.
A recipient does not need the owner's private seed.
This grant codec does not grant authorization, change membership, or implement a cloud share service.

## Resource and release boundary

Caller byte limits bound original ranges, raw terms, and canonical text before their use.
Context sizes have explicit bounds, and size arithmetic is checked.
The native whole-key cap remains 65,535 bytes; this source does not raise it or select a new home format.
Native integration must preserve the complete old logical range allowance despite the larger terms.

The [implementation plan](../docs/designs/ope-range-keys-implementation-plan.md) covers all readers, writers, migration, and primary adoption.
The single authorized CI proof covers order, prefixes, scopes, sizes, malformed input, suffix isolation, and protected range/grant context checks.
Builds and execution stay in CI; other tests remain retired.

CAUTION: Searchable terms expose order, equality, length, and first-differing-bit information. Low-entropy ranges still permit inference.

Authenticated original-range encryption does not remove leakage from searchable terms.
This candidate remains inactive until cryptographic review and the complete native integration pass their release gates.
