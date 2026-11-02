# LastDB data-at-rest threat model

Status: current for format 8 homes. The facts below come from the source tree of 2026-11-01.
File paths are repo-relative. Paths without a crate prefix are under `fold_db/crates/core/src`.

CAUTION: `identity.key` is a plaintext 32-byte seed with owner-only permissions.
A person who obtains that seed can derive every data key of the home.
This model does not protect against a complete compromise of the home and its key.

Target posture: password-manager grade. All user data on disk is encrypted, the
program has no plaintext fallback, the key hierarchy is documented, and key
material is cleared from memory after use.

Earlier designs used other stores and key roots: Sled namespaces, the LastStore
segment engine, an OS keychain master key, a keyring file, and a passphrase
root. The program has none of them. Git history keeps those designs.

## 1. Scope

In scope: all data that a node writes to its local disk:

- the node home (`LASTDB_HOME`, default `~/.lastdb`);
- the main run home `data/disk-run` (records and all other entries);
- the CAS run home `data/disk-cas` (`cas_blobs`);
- the `identity.key` seed and the cloud credentials file;
- the Search inbox;
- the key material that protects them.

Out of scope (separate work):

- Network and transport security, and the trust boundary of the local socket.
- App identity and isolation between apps.
- Cloud-side storage. The cloud receives only the encrypted run files.
- The at-rest protection of LastSeek, which keeps its own store outside LastDB.

## 2. Assets

| Asset | Where it lives | Sensitivity |
|---|---|---|
| Field values (user records) | The `records` collection of the main run home, at `rk:{S}:{hash}\0{range}` keys | Highest: the user's data |
| Alternate entries, field history, kept conflict records | `ak:`, `rh:`, `rcf:` and `rcm:` keys of the main run home | High: links to records and copies of old values |
| File blob bytes (photos, PDFs, imports) | Local `cas_blobs` in the CAS run home, and cloud CAS | Highest: raw user files |
| File KDK (per-blob DEK) | The `dek` field of the `$lastdb_file` pointer, inside the record | Critical: it unlocks the blob bytes |
| Field key bundles | The `field_keys` namespace: `fkb:` metadata and one encrypted `fkw:` wrap for each access domain | Critical: each bundle holds the blind key of its field key |
| Schemas, schema states, permissions, public keys, retry cache | Entries of the main run home | Medium: they show what kinds of data the user keeps |
| Identity seed | `identity.key` in the node home (`lastdb_identity`), plaintext, mode `0600` | Critical: the root of every data key and the signature identity |
| Cloud credentials | `cloud_sync.json.paused` in the node home | Critical: access to the cloud account |
| Search inbox batches | `apps/search/inbox/*.json` in the node home, or the directory that `LASTDB_SEARCH_INBOX` names | High: plain searchable field values |

## 3. Adversaries

- **A1: stolen laptop or stolen disk.** The attacker has the powered-off disk
  or a disk image. The attacker sees every file but not process memory.
  This is the primary adversary of this model.
- **A2: backup exfiltration.** A Time Machine, rsync or other backup copy of the
  node home leaks. This is equal to A1 for the copied files.
- **A3: a malicious local app or a process of the same user.** The process runs
  while the node runs. It can read every file of the user and send requests to
  the local socket. At-rest encryption is not the defense against A3. The
  at-rest design must not make A3 worse.
- **A4: a cloud or provider compromise.** Exemem, R2 or B2 is breached. The
  cloud keeps only the encrypted run files and encrypted file blobs.

Not adversaries: root or kernel malware, hardware attacks on a powered-on
machine (memory bus), and the user.

## 4. Current state

### 4.1 Key material

```
identity.key (Ed25519 seed, 32 B, plaintext, mode 0600)
  │  Ed25519 seed → X25519 secret
  ▼
HKDF-SHA256 (salt "fold:e2e:v1", info "fold:content-key")
  ▼
account content key (E2eKeys::encryption_key, 32 B)
  ├─ HKDF "lastdb/run-home/v2"      → main run home key (data/disk-run)
  ├─ HKDF "lastdb/run-home/v2/cas"  → CAS run home key  (data/disk-cas)
  ├─ CAS storage envelope around each local cas_blobs value
  └─ node wrap (fkw:) of each field key bundle
        └─ field key bundle → blind key → HashKey blind (blind_v1)
```

- The seed: `lastdb_identity/src/lib.rs` (`IDENTITY_KEY_FILE`).
- The content key: `crypto/e2e.rs` (`E2eKeys::from_ed25519_seed`).
  `E2eKeys` derives no other key.
- The run home keys: `storage/disk_pack/run_node.rs` (`home_key`, `cas_key`).
- The field key bundles: `storage/field_key_store.rs`.
- The CAS storage envelope: `storage/encrypting_namespaced_store/mod.rs`
  (`EncryptingNamespacedStore`) selects only the `cas_blobs` namespace and seals
  it through `storage/encrypting_store/mod.rs` (`EncryptingKvStore`).
- Values sealed under a raw key (field key wraps, delivery blobs of a share)
  use one codec: `crypto/at_rest/mod.rs`.

### 4.2 What the disk protects

Disk format v2 compresses each run-file block when that makes it smaller, then
encrypts the block with AES-256-GCM. A run identity selects a derived frame key,
and a block counter supplies the nonce.

- Keys, lookup blocks, records, alternate entries, field key metadata, blob
  counters, queue entries and control values are all inside encrypted blocks.
- A record is plain JSON inside its block. It has no second envelope. The file
  KDK in its `$lastdb_file` pointer has the same protection.
- A local `cas_blobs` value has three layers: the per-blob DEK, the CAS storage
  envelope under the account content key, and the CAS run home block encryption.
- Every HashKey in storage is blinded with the blind key of its field key (`blind_v1`).
  A range key is a typed range payload inside the encrypted blocks. No range uses OPE.
- The V2 cloud backup sends the same encrypted run files. It adds no second
  encryption layer. It excludes `identity.key` and the cloud credentials.

The [typed range field contract](../designs/typed-range-field-contract.md)
gives the block format, the range profiles and the backup profile.

### 4.3 What stays visible

- Descriptors, file footers and frame headers show the format, file identities,
  byte positions and lengths.
- The cloud also sees account and object identities, object counts and request times.
- `identity.key` is plaintext. Its file permissions are its only protection.
- `cloud_sync.json.paused` holds the cloud credentials. The backup reader refuses
  the file unless it is a private file of the current user (no group or other
  permission bits, one link).

CAUTION: With Search on, or after an offline `lastdb search-rebuild`, the
searchable field values are plain JSON on disk, outside the run files, with no
encryption. Inbox files that an earlier LastDB wrote, and the text that the
Search app indexed from them, can also hold the other fields and file keys.

Two paths write plain JSON batch files to the Search inbox
(`db_operations/search_index/search_outbox.rs`):

- Search on: the daemon starts with `lastdbd --enable-search` or with
  `LASTDB_SEARCH_ENABLED=1` (`db_operations/search_index/queue_policy.rs`,
  `enabled`). Each committed write then writes one batch file for each changed schema.
- Offline rebuild: `lastdb search-rebuild` pages the records of a stopped home
  into the same inbox with no Search gate
  (`db_operations/search_index/search_rebuild.rs`, `run_search_rebuild_for_home`).

Both paths use `db_operations/search_index/inbox_fields.rs`. A batch carries only
the searchable fields: a field with the `word` classification and no `secret`,
`no_index` or `no-index` classification. A schema with no field classifications
has no allowlist, so its batches carry every field. A batch carries no file key:
a `$lastdb_file` pointer keeps only `name` and `media_type`. The `dek`,
`blob_ref`, `file_hash` and the thumbnail go, because the plaintext hash
confirms the file content. A batch file stays on disk until a reader deletes it
(for example `lastseek drain --consume`).

### 4.4 Weaknesses

1. **The seed is the root.** A1 or A2 with `identity.key` derives every data key.
   No passphrase, keychain or hardware key protects the seed.
2. **No rotation.** One static content key comes from a seed that never changes.
   The version 1 envelope has no key identity (gap G5).
3. **A3 reads everything.** A process of the same user reads `identity.key` and
   the credentials file. At-rest encryption does not stop it.
4. **No lock state.** Keys stay in process memory while the daemon runs (gap G6).
5. **Search inbox.** Searchable field values leave the encrypted run files when
   Search is on (section 4.3).

## Operation Trinity status

The product encryption standard has exactly four seals (brain
`preference-lastdb-encryption-standard-trinity-only`). Agents must not add or
extend another seal.

> Published schema definitions are not a secret.

| Layer | What | Crypto |
|---|---|---|
| Hash key | The HashKey of a field key in storage | Blind (`blind_v1`) with the blind key of the field key bundle |
| Range key | The RangeKey of a field key in storage | Typed range payload inside an AES-256-GCM run-file block |
| Field value | The record JSON | AES-256-GCM run-file block encryption. The record has no field value seal |
| File blob | CAS and file bytes | Per-blob DEK. The KDK is in the record, inside an AES-256-GCM run-file block (not in Search inbox batches) |

1. **Father: field keys.** Every home blinds the HashKey. The API accepts and
   returns plaintext HashKey and RangeKey values (Option I). A storage key never
   returns as an API key.
2. **Son: records.** A record is plain JSON inside an encrypted run-file block.
   The program has no field value seal, no strict open and no binary record format.
3. **Holy Ghost: files.** Cloud CAS holds each blob under its per-blob DEK.
   Local `cas_blobs` holds the same sealed bytes, and the CAS entry stores no DEK.

The file blob routes (`/api/db/file-blob`, fetch, fork) are live. The local
cache must not store plaintext blob bytes when a DEK is available.

Design: `fold_db/docs/DESIGN_OPERATION_TRINITY.md`.

## 5. Envelope and key rules

### 5.1 No plaintext fallback

A format 8 home always has a key root: `identity.key`. Home creation requires the
node key, and a home opens only with the key that wrote its account-key proof.
A wrong key gets a refusal. No setting writes plaintext run files.

### 5.2 Envelope v2

`crypto/envelope/mod.rs` keeps two envelope codecs:

- Version 1: AES-256-GCM, `[version:1][nonce:12][ciphertext+tag]`. It has no key
  identity and no associated data. Every current seal writes version 1.
- Version 2: AES-256-GCM-SIV, `[version:1][key_id:4][nonce:12][ciphertext+tag]`,
  with the storage context as associated data. The `key_id` makes "which key
  sealed this value" a lookup, not a trial decryption. The associated data binds
  a ciphertext to its location only when the caller supplies that context.

Version 1 stays readable. Version 2 is the base for key rotation (gap G5).

### 5.3 Defense in depth

Full-disk encryption (FileVault, LUKS) is recommended. It protects file names,
the seed and the credentials file against A1 when the machine is off. It does
not help when the volume is mounted, so it does not replace the run-file encryption.

## 6. Gaps

| # | Gap | Status |
|---|---|---|
| G1 | Catalog values without encryption | Closed: every entry is inside an encrypted run-file block. The Trinity standard keeps schemas out of the seal list |
| G2 | Plaintext fallback in shipped builds | Closed: `identity.key` is the only key root, and no setting writes plaintext run files |
| G3 | Key material stays in memory after use | Partly closed: `E2eKeys`, `LocalCryptoProvider`, the run home keys and the cloud credentials buffer clear on drop. Short stack copies remain |
| G4 | No passphrase root | Not planned: recovery uses the 24-word phrase and cloud restore |
| G5 | No key rotation | Open: envelope v2 has the key identity; no key map uses it |
| G6 | No lock and unlock state | Open |

## 7. Non-goals

- Encryption of key names for an attacker inside the process. On disk, the keys
  are inside the encrypted run-file blocks.
- Protection against root or kernel compromise, or against live-memory attackers.
- Read access control between apps (the app isolation work).
- Encryption of the whole node home. `identity.key`, the credentials file, logs
  and app files have their own boundaries.

## Portable same key

LastDB Mini personal data at rest uses the account content key from
`identity.key`. The cloud backup uses the same key material, so a restore needs
only the identity. Do not add a store key on Mini personal data that the
identity cannot derive.

Design: brain `design-portable-same-key-at-rest-cloud`.
