# HashKey-only blinding v1

Canonical brief: brain `design-lastdb-hashkey-blind-v1`.

## Summary

- Blind **HashKey only** (HMAC-SHA256, domain `hk|v1`, bound to field key uuid).
- **RangeKey stays plaintext** (order-preserving).
- API / SDK still use **plaintext** HashKey; storage keys use `storage_hash`.
- Every node home uses **`blind_v1`**. It is the only HashKey encoding: no
  setting, codec, or library constructor selects a plain hash.

## Crypto

```text
blind_hash_key(blind_key, field_key_id, plaintext_hash) =
  base64url_nopad(HMAC-SHA256(blind_key, "hk|v1\0" || M || "\0" || hash)[..16])
```

`blind_key` = the blind key of the field key's key bundle (random per field key,
wrapped under the node key). `FieldKeyCodec::new` requires the key, so no
codec exists without one.

## Migration hard gate

Offline re-blind of existing homes requires **plaintext HashKey available
outside the storage key** (body field / mutation envelope). If HashKey exists
only inside `mk:` key bytes, pure re-blind is impossible.



Every format 4 home is blinded from creation. The program has no re-blind,
dual-read, or migration path. Cloud sync remains paused (Situation) — not a
gate for local encoding, but do not re-enable as part of this work.

## Response mapping (Option I)

Clients always query with plaintext HashKey. Responses re-emit that HashKey
for point/partition filters — never the storage token.
