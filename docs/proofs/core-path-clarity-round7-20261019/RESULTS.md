# Memory and disk clarity review

The count route could treat a valid API key as another key's disk encoding. It could reject a correct two-key count. Keyed responses also labeled reusable keys as opaque. The change keeps API keys explicit and confines hash recovery to physical pages.

## Changes

- Counts retain distinct logical keys. The atom reader still resolves competing physical spellings before the count.
- Exact keys and ranges under one hash retain API coordinates. Physical page and sample reads still recover stored hashes when their field values verify them.
- Delete and Purge retain their disk wait. The unused no-wait option is removed through every enqueue caller. A queued deletion always has a completion receiver.
- Each prepared disk commit owns its space reservation directly. A failed journal commit retains that reservation until runtime drop.
- The sealed queue retains failed request owners. The duplicate failed-request list and the unused disk-frontier atomic are removed.
- Configuration rejects obsolete type tags, non-object input, and invalid path types. Current engine and cloud settings retain their validation.
- Key preload chooses disk fallback from typed resource errors. A timeout keeps that error type through generation reads. Error wording no longer selects policy.
- The unused Hash branch of the one-dimensional range helper is removed. The storage overview now names the native engine and current wrappers.

Captured read owners remain necessary for earlier calls and their memory charges. Selected-tip receipts retain the actual atom location and metadata. Complete range keys remain separate from partial field values. The private disk queue retains flush work and does not serve ordinary reads. Explicit physical format tools retain their active adapters.

## Product comparison

The preserved baseline release matches the source on main `8c4fa18aa760f85f4687f36069daa4eb9a840518`. Its saved release SHA-256 is `efe05898cfd8f3a5c6ca233e78adfb5575ab86f2c650db3f280f3dcb1a6b9f1b`.

The narrow operation creates two distinct keys on a fresh synthetic home. The second key equals the first key's stored hash. A bounded owner read supplies that token without a key export. Both records remain independently addressable.

Before and after a normal reopen, the baseline returns both records but rejects `expected_total_count: 2` with HTTP 409. It also reports `key_form: opaque` for the batch and the separate exact reads. All eight baseline observations remain failed against the original expectations.

The candidate uses frozen source `3de98eff48d662cd8a8c631b953400aa35c03d99`. Its release SHA-256 is `563aa1ba49440f76e43e7c478b439bbd8e07b0b6cdf868888e908f7b8b482a1b`. All 1,875 build inputs match the reviewed source.

The candidate passes all eight narrow observations on the same synthetic home, without new logical writes. Reopen and repeat both return reusable API keys. Each exact read counts one record. The two-key read counts two records.

On a separate fresh home, all 74 read calls pass. The 29 memory-only read checks require and receive zero file reads. All 100 static query items pass: 99 successful responses and one intended unknown-field refusal. These calls cover typed hash and hash-range keys, sparse fields, conditions, order, shared-field deletion, and normal reopen. All owned nodes stop normally.

The original receipts remain sealed. Both owned synthetic homes are removed through supported exact-home cleanup after independent ownership and process checks. All 38 sealed original file checks pass after cleanup.

## Validation

Full required workspace Clippy and the additional Clippy packages pass on the frozen source. The normal release build passes in 118.94 seconds. Format, file-size, function-size, and added-allow checks pass. No lint exemption or executable exercise helper is added. The source review records no unresolved blocker.

The later merge from main adds only two design documents. All 1,875 implementation inputs still match the reviewed build. The executable retains its original build attribution. A rebuild can change the embedded Git version. No test suite runs.

Configuration decoding and failure-owner lifetimes receive source review and compiler checks. The ordinary Mini boot constructs its configuration directly. No induced disk failure or preload timeout forms part of the product operation.

The primary and archive remain outside this work. Native file packing remains active. No migration, cloud activation, install, or upgrade forms part of this change. The separate warm-set byte-limit design remains separate work. This round contains no throughput comparison or whole-process memory-cap measurement.
