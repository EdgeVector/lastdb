# HashKey-only blinding v1

Canonical brief: brain `design-lastdb-hashkey-blind-v1`.

## Summary

- Blind **HashKey only** (HMAC-SHA256, domain `hk|v1`, bound to molecule uuid).
- **RangeKey stays plaintext** (order-preserving).
- API / SDK still use **plaintext** HashKey; storage keys use `storage_hash`.
- Product default (env unset): **`blind_v1`**. Explicit `plain` for tests/opt-out
  (`LASTDB_HASH_KEY_ENCODING=plain|blind_v1`).

## Crypto

```text
blind_hash_key(index_key, molecule_uuid, plaintext_hash) =
  base64url_nopad(HMAC-SHA256(index_key, "hk|v1\0" || M || "\0" || hash)[..16])
```

Index key = E2E `index_key` (HKDF from identity seed). Fail closed if blind
mode and key missing.

## Migration hard gate

Offline re-blind of existing homes requires **plaintext HashKey available
outside the storage key** (body field / mutation envelope). If HashKey exists
only inside `mk:` key bytes, pure re-blind is impossible.



**Do not enable `blind_v1` on real homes** until dual-read + CoW migrate proven.
Cloud sync remains paused (Situation) — not a gate for local encoding, but do
not re-enable as part of this work.

## Response mapping (Option I)

Clients always query with plaintext HashKey. Responses re-emit that HashKey
for point/partition filters — never the storage token.
