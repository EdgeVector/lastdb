# LastDB Delivery Slice Wire Format

Version: `lastdb.slice.v1`

Delivery artifacts are consent-gated database slices. A producer may stage a
slice, but no network send occurs until the owner approves the pending delivery
outbox item.

## Layers

0. Transport: the bulletin-board message is sealed to the recipient's messaging
   key and signed by the sender's node identity using the existing
   `signed_envelope` channel. Its inner payload has
   `message_type: "delivery_slice"`, a base64 `content_key`, the JWE
   `envelope`, and the descriptor metadata returned during staging.
1. Envelope: JWE JSON serialization with `alg=dir` and `enc=A256GCM`.
   The protected header uses `typ=application/vnd.lastdb.slice+json;v=1`.
   Any JOSE implementation that supports direct A256GCM can decrypt the
   envelope with the transported content key.
2. Payload: UTF-8 JSON `SignedLastDbSlicePayload`.
   It contains the portable slice plus `payload_sha256`, `signer_public_key`,
   and an Ed25519 signature over the payload hash.

## Payload Shape

`payload.version` is `lastdb.slice.v1`.

`payload.provenance` identifies:

- `source`: the saved query, ad hoc query, transform-output view, or legacy
  scope that produced the slice.
- `mode`: `snapshot` or `live`.
- `created_at`: Unix seconds.
- `sender_public_key`: the LastDB node identity that signed the payload.

`payload.schemas[]` carries the schema name, its JSON definition, and the exact
fields included in this slice.

`payload.molecules[]` carries one field molecule per delivered field:

- `schema_name`
- `record_key`
- `field_name`
- `atom_ref`
- optional original molecule UUID/version, writer pubkey, and molecule
  signature when available

`payload.atoms[]` carries the JSON value for each referenced atom. Each atom is
content-addressed as `sha256:<hex>` over the canonical JSON value bytes.

`payload.blobs[]` (optional, default empty) carries **first-class binary files**
(photos, PDFs, attachments). Each blob is content-addressed as `sha256:<hex>`
over the **raw file bytes** (not the base64 form). On the JSON wire the bytes
travel as `bytes_b64`; consumers MUST recompute the hash after decode.

| Field | Meaning |
|-------|---------|
| `blob_ref` | `sha256:<hex>` of raw bytes |
| `content_sha256` | hex digest without prefix |
| `bytes_b64` | standard base64 of raw bytes |
| `size` | decoded length |
| `media_type` | optional (e.g. `image/jpeg`) |
| `name` | optional filename |

File-bearing **field values** must not embed raw/base64 content. They point at
a blob:

```json
{
  "$lastdb_file": {
    "blob_ref": "sha256:…",
    "name": "lake.jpg",
    "media_type": "image/jpeg"
  }
}
```

Every `$lastdb_file.blob_ref` (or `$blob_ref`) in `atoms[]` MUST have a matching
entry in `blobs[]`. Legacy atoms that embedded `{"$file":{"content_b64":…}}`
are rejected by the modern packager; materialization rewrites them into
pointer + `blobs[]` before signing.

## Consumer Contract

A non-LastDB consumer decrypts the JWE, verifies the payload hash and Ed25519
signature, parses schemas/molecules/atoms/**blobs**, joins each molecule to its
atom by `atom_ref`, and joins each `$lastdb_file` atom to its blob by
`blob_ref` (after verifying the blob's raw-byte hash).

A LastDB consumer can mount the same payload by importing the schema definitions,
replaying molecule/atom values into a peer namespace, and storing `blobs[]` in a
local CAS so `$lastdb_file` pointers resolve to the original file bytes. The
format intentionally includes raw canonical components first; flattened views
can be derived by consumers without changing the signed payload.
