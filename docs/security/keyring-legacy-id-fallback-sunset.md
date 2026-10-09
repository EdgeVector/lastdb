# Keyring Legacy-ID Fallback Sunset

Date: 2026-07-16

## Scope

This note records the evidence used to remove the at-rest keyring decrypt fallback for:

- envelope v1 values with no `key_id`
- envelope v2 values stamped with reserved `Keyring::LEGACY_KEY_ID` (`0`)

The reserved id remains reserved so no new DEK can mint as `0`; the read path now accepts only current key-id-stamped v2 envelopes held by the loaded keyring.

## Evidence

- Primary Mini host inspection found no `keyring.enc` under `/Users/example/.lastdb` at the time of this change (`find /Users/example/.lastdb -maxdepth 3 -name keyring.enc` returned no files). The primary Mini runtime therefore was not relying on the keyring Store-DEK fallback.
- A Brain lookup for dogfood `at_rest_keyring` / `keyring.enc` / Store-DEK fallback references returned no durable dogfood deployment record for this path.
- Sync replay already treats v1 or `LEGACY_KEY_ID` at-rest envelopes as poison entries in `fold_db/crates/core/src/sync/engine/replay/decode.rs`; the local keyring provider now matches that current-only policy.

If a future data set contains keyring-backed v1 or `LEGACY_KEY_ID` values, it needs an explicit offline re-seal under the active keyring DEK before booting with this code.
