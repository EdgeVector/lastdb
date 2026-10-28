# RangeKey OPE v1

Canonical brief: brain `design-lastdb-rangekey-ope-v1`.

Status: no node home uses OPE v1. A format 4 home stores the typed range
profile payload as plain bytes inside encrypted run blocks
(`docs/designs/typed-range-field-contract.md`). The codec keeps OPE v1 until a
later codec cleanup.

Sibling: `DESIGN_HASHKEY_BLIND_V1.md` / `design-lastdb-hashkey-blind-v1`.

## Summary

- Make RangeKey **not greppable** in storage ids (`mk:` / `mhr:`).
- **Order leakage is accepted** (not ORE).
- API / SDK still use **plaintext** RangeKey; OPE is internal storage encoding.
- No setting selects OPE v1 for a node home.

## Combined with HashKey blind

```text
mk:{M}:{esc(blind_hk(HashKey))}\0{ope(RangeKey)}
mhr:{M}\0{esc(ope(RangeKey))}\0{esc(blind_hk(HashKey))}
```

Empty range is never OPE-encoded.

## OPE algorithm (v1)

Per-byte fixed-width mono map (hex, no `\0`):

```text
for each byte index i and byte b:
  word = (HMAC(ope_key, "rk|ope|v1\0" || M || "\0" || i_be32)[..4] as u32 & !0xFF) | b
  emit 8 hex digits of word
```

- Same position ⇒ same high bits ⇒ order of low byte = byte order.
- Concatenation preserves UTF-8 string order (including shorter-prefix-less).
- **Byte-prefix-preserving:** `ope(prefix)` is a string prefix of
  `ope(prefix||suffix)`, so `RangePrefix` works via `create_prefix_end(ope(prefix))`.

`ope_key` = HKDF expand of the E2E root with info `fold:range-ope-v1`.

## Migration hard gate

Same as HashKey sibling: re-encode needs plaintext RangeKey outside the storage
key (body / API). 

Do **not** enable on real homes until CoW proven. Cloud sync stays paused.
