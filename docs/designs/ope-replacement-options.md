# Replacement options after the OPE review

Status: Decision support, 2026-10-24. No replacement is selected or activated.

[PR 365](https://github.com/EdgeVector/lastdb/pull/365) closed without merge after CI confirmed the suffix-leakage finding.
The [security review](ope-candidate-security-review.md) records that finding.
The original private-key requirement and complete range values remain requirements for any replacement.

## Published alternatives

| Route | Published basis | Main cost or limit |
|---|---|---|
| Original binary CLWW OPE | Section 3.2 of the [CLWW paper](https://saweis.net/pdfs/pore-clww.pdf) | Large ciphertexts; its comparison error is negligible with large parameters, rather than exactly zero |
| Original byte-wise CLWW OPE | Appendix B and Section 3.2 of the same paper | Smaller ciphertexts; it exposes the exact byte difference at the first unequal byte |
| BCLO OPE | [EUROCRYPT 2009 paper](https://www.iacr.org/archive/eurocrypt2009/54790225/54790225.pdf) | Needs a complete finite string-to-integer domain and a reliable large-domain hypergeometric sampler |
| Lewi-Wu ORE | [CCS 2016 paper](https://eprint.iacr.org/2016/612.pdf) | Needs its published comparator instead of ordinary byte order, plus full-range domain support |

The original CLWW OPE conversion has no backward carry between output blocks.
Its binary form exposes the first unequal bit; the byte-wise form also exposes the difference between the unequal bytes.
Increasing its output domain reduces the wrap error; it does not remove those stated leakage properties.
The rejected extension's security claim cannot substitute for either published construction.

## Concrete size and correctness choices

These sizes use the previous fixed one-byte input marker and lowercase hex.
They are calculations for LastDB's possible representation, not benchmark results or an audited new profile.
Let `N` be the complete original range length in bytes.

| Possible parameter choice | Raw term bytes | Hex term bytes | Per-comparison wrap-error bound under the PRF assumption |
|---|---|---|---|
| Binary CLWW, `M = 2^128` | `128*(N+1)` | `256*(N+1)` | `2^-128` |
| Binary CLWW, `M = 2^256` | `256*(N+1)` | `512*(N+1)` | `2^-256` |
| Byte-wise CLWW, `M = 2^256` | `32*(N+1)` | `64*(N+1)` | At most `2^-248` |

These probability bounds concern the published construction's random key or PRF model.
They are not a runtime check, an exact-order guarantee, or a bound on every comparison over the entire possible domain.
The application needs an explicit lifetime error budget before selection.
Variable-length input framing and LastDB's exact PRF contexts still require a separate review.

Every option needs a complete size contract before native activation.
The current whole stored-key cap is 65,535 bytes.
A replacement must preserve supported logical ranges through a versioned storage change, rather than reduce or hash those ranges.
CI must measure disk cost, native key overflow storage, and cold range reads with the active representation.

## Decision boundary

Keep the private identity seed as the root, with separate molecule, epoch, and blinded-hash purposes.
Keep authenticated original-range protection, hash blinding, atom content encryption, and file blob encryption.
Keep cloud and primary changes separate from code review and PR completion.

The construction choice determines the next implementation package.
Do not silently accept the rejected suffix leakage, a new byte-difference leak, or a probabilistic comparison contract.
ORE avoids that OPE conversion's wrap error but changes the storage comparison route.
It also changes the future cloud service design where a service expects ordinary sortable keys.

After that choice, complete the exact byte contract, independent review, all readers and writers, conversion, recovery, and release evidence.
Only then prepare the candidate-specific primary cutover for review.
