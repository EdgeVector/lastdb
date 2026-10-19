# Warm-set read boundaries

Some reads returned different keys or writer details after the data entered memory. A page could also end before later matching records. These changes give memory and disk reads the same selection rules.

## Changes

- Narrowed reads carry an explicit range form. Exact caller keys such as `00000061` retain their original range and selected physical tip.
- Named-layout selection precedes the result window. Bounded candidate batches retain their last examined key before value fetch. The primary tip set controls projection fallback.
- Hash and HashRange fields retain each selected tip's writer. The molecule-wide writer helper remains specific to Single fields.
- Each positive memory tip consumes the shared call's key and metadata allowance, including tips rejected by visibility checks.
- Cross-hash `PageAfter` receives an explicit refusal. Administrative pages use offsets and advertise no unsupported cursor. Fixed-hash query cursors retain their current route.

## Validation

The workspace and remaining package compiler checks pass with warnings refused. Format, changed-file size, function size, and new-allow checks pass. The normal release build passes. The final source review finds no blocker within these five changes.

The release uses source commit `122072ed51e20a1f67ad4915c0efcb9a2ffc1551`. Its executable SHA-256 is `fb68635e41651eb14c3ff91210bfa4a96c83c63239f5d3cd4b89868c4e523127`. Two source inventories agree. All 1,877 inputs match that immutable Git commit and the saved build settings.

The saved old executable uses source commit `3de98eff48d662cd8a8c631b953400aa35c03d99`. Its 1,875 product inputs match accepted main `8ed44f56118ec00173f38dd4b05c0872c9e700f7`. The old executable receives no rebuild.

| Product check | Old program | New program |
|---|---|---|
| Same 72 key and page observations, across two normal reopens and warm reads | 46 pass; 26 fail | All 72 pass |
| Shared batch of 64 counts after the same 256-key prime | All 64 return count disagreement; no read-limit refusal | 39 return count disagreement; 25 return the required read-limit refusal |
| Warm confirmation for the budget case | All 256 keys and values; zero file reads | All 256 keys and values; zero file reads |
| Existing broader read operations | Earlier accepted evidence remains separate | All 74 pass; all 29 zero-file requirements pass |
| Existing 100 typed-key and shared-field query items | Earlier accepted evidence remains separate | All 100 pass: 99 successful replies and one required refusal |

The old failures remain failed observations. They cover cold exact-key counts, mapped-schema pages and counts, and administrative cursor behavior. The new read-limit refusals contain the required error type. All 64 count replies use zero file reads.

The first candidate operation stops when the request tool tries to decode a plain-text refusal as JSON. It remains an incomplete operation. The separate full repeat saves raw responses before decode and uses the same frozen requests and expected results. Its 72 observations pass. The original files and their hashes remain unchanged.

The first broader operation also remains incomplete. Its comparison treats an absent optional range as different from a null range. The saved response contains the correct hash, integer value, payload, and metadata. The separate repeat compares both optional coordinates with the existing coordinate rule. Exact field names, values, types, order, counts, and identities remain required. All 100 items pass, including normal reopen and typed shared-field updates and purge. Both operational corrections preserve the original failures.

The complete product operations use the same frozen version-three plan, request files, and existing helpers. No product source, helper, expected result, or HEAD changes during the release build or these operations.

All 21 owned node sessions stop normally. An independent review checks each of the seven exact roots before supported cleanup. All seven roots are absent after cleanup. All 49 original receipt files, both seals, all 15 helpers, and the frozen request files retain their hashes. The full local seal exceeds the repository blob limit. The tracked compact seal retains each original file hash and root identity.

The product comparison uses generated data in owned temporary homes. It uses the normal API and existing operation helpers. No test suite or new executable exercise helper forms part of this work.

Normal local API writes use the node's writer. A different-writer example therefore receives source review and compiler checks only. Ordinary empty-key reads cannot prove that different-writer case. The captured dirty-memory allowance branch also receives source review and compiler checks only.

Large offsets can require many candidate values. Prefix and between selectors can collect the full narrowed tip list before bounded value fetches. These requests can reach the shared read allowance. This change makes no general memory-size or throughput claim.

The separate byte-limit design remains separate work. The primary, its archive, migration, cloud activation, installation, and upgrade remain outside this change. No throughput improvement follows from these correctness fixes.
