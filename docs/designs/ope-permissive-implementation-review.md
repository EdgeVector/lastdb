# Permissive OPE Implementations And The Private-Key Contract

Current route: [binary CLWW replacement](ope-range-keys-implementation-plan.md).
The earlier backward-carry candidate below is rejected; [PR 365](https://github.com/EdgeVector/lastdb/pull/365) closed without merge.

Source review: 2026-10-24. This is a candidate review, not an implementation audit or a release approval.
Implementation now starts with LastDB-owned source under the [fixed candidate codec contract](../../lastdb_range_crypto/README.md).

Tom requires the user's private key to control creation of an ordered set of lookup keys.
The [implementation plan](ope-range-keys-implementation-plan.md) retains that requirement.
The review finds permissively licensed implementations, but no reviewed direct replacement for LastDB's general string ranges.
Do not infer release readiness from a permissive license.

## Required behavior

The private key supplies the secret root for a separate symmetric range key.
The public identity key must not suffice to create lookup terms.
The cloud receives searchable terms and encrypted data, rather than the private key or range key.

The byte-sortable OPE contract is:

```text
private-key-controlled secret
    -> separate symmetric OPE key for one molecule, hash, format, and epoch
    -> deterministic term for each exact range value

same secret + same scope + same range -> same lookup term
x < y within one scope              -> term(x) < term(y)
```

The order is exact unsigned UTF-8 byte order.
The codec accepts a new range value without a complete local key set or server interaction.
Order across unrelated hashes or molecules is not required.
The private key controls term creation; it does not conceal the order of existing terms from the cloud.

Keep the existing content and hash purposes and use a separate private-seed range purpose.
The [root derivation](../../fold_db/crates/core/src/crypto/e2e.rs#L35) uses HKDF-SHA256 after conversion of the identity seed.
The current [molecule store](../../fold_db/crates/core/src/db_operations/molecule_key_store.rs) also keeps random molecule keys inside encrypted access-domain bundles.
In that model, the browser needs the private key and a small encrypted molecule bundle before it creates terms.
This does not require a preload of range keys.

The selected new range purpose derives the molecule range secret deterministically from the private seed and its versioned scope.
That changes the current random-bundle creation rule and requires an explicit format and recovery decision.
Sharing can grant a scoped molecule secret without disclosure of the account root.
The first codec contract fixes that choice; it does not reinterpret an existing molecule bundle.

## Candidate results

| Implementation | License evidence | Actual fit for LastDB |
|---|---|---|
| `sentclose/ope` | [MIT](https://github.com/sentclose/ope/blob/master/LICENSE) | Pure Rust OPE. The documented entry point accepts values up to 65,532. It is not a general string-range codec. |
| `tonyo/pyope` | [MIT](https://github.com/tonyo/pyope/blob/master/LICENSE) | Python implementation of Boldyreva OPE over integer domains. Useful reference, rather than a Rust/browser dependency. |
| `husobee/ope` | [MIT](https://github.com/husobee/ope/blob/master/LICENSE.txt) | Incomplete Rust program, with an external local-path dependency and a placeholder decoder. Reject as a product dependency. |
| `aymanmadkour/ope` | [Apache-2.0](https://github.com/aymanmadkour/ope/blob/master/LICENSE.txt) | Java Fast OPE and modular OPE. Its reviewed Fast OPE path encodes bytes independently and uses a 64-bit secret parameter. Reject that path for this contract. |
| `enquo/cretrit` | [MIT manifest](https://github.com/enquo/cretrit/blob/main/Cargo.toml) and [license notice](https://github.com/enquo/cretrit#licence) | Rust Lewi-Wu ORE. A candidate if the engine gains a custom comparison function; not byte-sortable OPE. |
| `dbogatov/ore-benchmark` | [MIT](https://github.com/dbogatov/ore-benchmark/blob/master/LICENSE) | C# research implementations of BCLO OPE and CLWW ORE. Useful references, rather than Rust product dependencies. |

These license statements cover the reviewed project files.
Dependency licenses, provenance, and attribution still require review for a selected release.
Retain applicable copyright and license notices.
No vendor-service requirement appears in the MIT grants above.

### Rust OPE limits

The [sentclose README](https://github.com/sentclose/ope) documents the small integer domain and claims WebAssembly support.
The reviewed [implementation](https://github.com/sentclose/ope/blob/master/src/ope.rs) takes integer inputs and integer outputs.
Do not convert a full string to a hash or truncate it to fit; either change destroys the required original order.
Do not concatenate independent tiny-domain OPE terms as a substitute for a reviewed string construction.
That design exposes the order and equality of each tiny domain independently.
Browser build support and production suitability remain unverified in this review.

The [husobee source](https://github.com/husobee/ope/blob/master/src/main.rs) returns the constant `42` from its decoder.
Its [manifest](https://github.com/husobee/ope/blob/master/Cargo.toml) depends on `../dist_variate`.
These are concrete readiness problems, independent of its permissive license.

### Python and research references

The [pyope README](https://github.com/tonyo/pyope) describes an experimental integer implementation.
It also warns that different package versions can produce different terms for the same key and input.
Any port therefore needs its own frozen stored format and independent review.
It is not the CLWW candidate from the earlier plan.

The [MIT CLWW reference](https://github.com/dbogatov/ore-benchmark/blob/master/src/crypto/clww-ore/CLWW.cs) implements ORE with its own comparison function.
It does not establish a permissive implementation of the vendor's exact byte-sortable variant.

### MIT Rust ORE alternative

The [cretrit README](https://github.com/enquo/cretrit) describes the Lewi-Wu construction and explicitly reports no independent implementation audit.
Its Rust ciphertext type supplies comparison through an `Ord` implementation.
That does not mean that serialized ciphertext bytes preserve order.
LastStore's stored-key comparator would need a deliberate format-aware change across all relevant read and write paths.
Parameter choice, range-byte representation, deterministic exact identity, and ciphertext size also require design.
Do not replace the OPE codec with this library alone and expect existing byte trees to remain correct.

## An implementation owned by LastDB

A LastDB-owned implementation can meet the license goal without a vendor dependency.
Implement a published construction with established keyed-hash and key-derivation libraries.
Do not invent a new cipher or copy code from a restricted implementation.
Do not describe this review as a clean-room certification or a patent clearance.

Exact byte order remains a hard requirement for an ordinary stored tree.
Section 3.2 of the [CLWW paper](https://saweis.net/pdfs/pore-clww.pdf) explicitly permits comparison errors in its direct ORE-to-OPE conversion.
A naive implementation of that conversion is therefore not the proposed exact solution.
The vendor's backward-carry variant also needs its own security review; its properties do not follow automatically from the original paper.

Before adoption, specify the complete algorithm, input representation, scope derivation, output size, and correctness argument.
Obtain independent cryptographic review of both order correctness and information leakage.
Then integrate the codec through the existing plan's reader, writer, conversion, and recovery packages.
The original-range encryption and prefix-bound work remain necessary.
Tests stay retired; product operations and measurements use fresh CI DEV data under the existing verification rules.

## Recommendation

For a strict byte-sortable OPE requirement, retain a LastDB-owned, reviewed implementation as a proposed delivery route.
No reviewed library above removes that engineering and cryptographic review work for general string keys.
Keep MIT `cretrit` as the existing-code alternative if a custom comparator is acceptable.
That choice trades a larger storage-format change for reuse of an existing ORE implementation.
Do not activate either path or change the accepted key format during this research task.
