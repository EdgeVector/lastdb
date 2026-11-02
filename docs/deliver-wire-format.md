# LastDB Delivery Slice Wire Format

Version: `lastdb.slice.v1`

Delivery artifacts are database slices that the owner exports. The owner
socket route `POST /api/sharing/snapshot` materializes one slice, seals it to
the recipient's messaging key, and returns it. LastDB keeps no staged delivery
and sends no message.

## Layers

0. Transport: the snapshot blob is sealed to the recipient's messaging key
   and signed by the sender's node identity with the `signed_envelope`
   format. Its inner payload has `message_type: "delivery_slice"`, a base64
   `content_key`, the JWE `envelope`, and the descriptor metadata.
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

- `source`: the query that produced the slice.
- `mode`: `snapshot` or `live`.
- `created_at`: Unix seconds.
- `sender_public_key`: the LastDB node identity that signed the payload.

`payload.schemas[]` carries the schema name, its JSON definition, and the exact
fields included in this slice.

`payload.field_keys[]` carries one field key per delivered field:

- `schema_name`
- `record_key`
- `field_name`
- `value_ref`

`payload.records[]` carries the JSON value for each referenced record. Each record is
content-addressed as `sha256:<hex>` over the canonical JSON value bytes.

`payload.blobs[]` (optional, default empty) carries **first-class binary files**
(photos, PDFs, attachments). Each blob is content-addressed as `sha256:<hex>`
over the **raw file bytes** (not the base64 form). On the JSON wire the bytes
travel as `bytes_b64`; consumers MUST recompute the hash after decode.

| Field | Meaning |
|-------|---------|
| `blob_ref` | `sha256:<hex>`: for a blob that carries `access`, the hash of the account-v2 sealed bytes; otherwise the hash of the raw bytes |
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

Every `$lastdb_file.blob_ref` in `records[]` MUST have a matching entry in
`blobs[]`. LastDB reads no other file-value form.

## Consumer Contract

A non-LastDB consumer decrypts the JWE, verifies the payload hash and Ed25519
signature, parses schemas, field keys, records/**blobs**, joins each field key to its
record by `value_ref`, and joins each `$lastdb_file` record to its blob by
`blob_ref` (after verifying the blob's hash).

A LastDB consumer can mount the same payload by importing the schema definitions,
replaying field key/record values into a peer namespace, and storing `blobs[]` in a
local CAS so `$lastdb_file` pointers resolve to the original file bytes. The
format intentionally includes raw canonical components first; flattened views
can be derived by consumers without changing the signed payload.
