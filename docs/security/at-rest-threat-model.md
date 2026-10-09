# fold_db data-at-rest threat model & gap analysis

Status: **adopted design** (2026-06-10). Implementation tracked as one
fkanban card per gap — see [Gaps → follow-up cards](#gaps--follow-up-cards).

**Update 2026-07-13 (Mini / identity.key):** LastDB Mini's shipping key root is
the 32-byte `identity.key` seed (`E2eKeys::from_ed25519_seed` + optional
`at_rest_kek_from_seed` for keyring wrap). The Argon2id **passphrase root**
(`passphrase.params`, `PassphraseRoot`, `FOLDDB_PASSPHRASE*`) was removed from
`fold_db` core — password unlock and in-place legacy-root migration were
cancelled in favor of identity.key + cloud restore. Sections below that still
describe passphrase / multi-root KEK resolution are **historical design notes**
from 2026-06, not current Mini product code.

Target posture (2026-06 design): **password-manager grade**. A fold_db node
holds a user's private data the way a password manager holds their secrets, so
it inherits the same bar: all user data encrypted at rest, no plaintext
fallback in any shipping configuration, a documented key hierarchy, zeroized
key material, and a tested rotation + recovery story. (The 2026-06 "memory-hard
passphrase option" limb was retired for Mini; recovery is the 24-word phrase /
cloud restore path.)

Every "current state" claim below was verified against the code on
`origin/main` at commit `ea0196243` (2026-06-10), not written from memory.
File paths are repo-relative.


## Operation Trinity status (2026-07-29)

Target posture for Mini personal data (no plaintext fallbacks):

1. **Father — molecules:** product default HashKey `blind_v1` + RangeKey `ope_v1`.
   Storage `mk:` keys must not embed API plaintext hash/range segments; API still
   accepts plaintext HashKey/RangeKey (Option I).
2. **Son — atoms:** field `content` sealed under account E2E key (`ENC:…`).
   **Binary default (unset env) is dual-read** so legacy plain still opens during
   migrate. **Product primaries after reseal** must set durable
   `LASTDB_ATOM_CONTENT_STRICT=1` on the LaunchAgent forever (fail-closed open).
   File KDK (per-blob DEK) lives inside sealed content / access metadata.
3. **Holy Ghost — files:** cloud CAS under per-blob DEK; durable local `cas_blobs`
   sealed under the same DEK (DEK not stored in the CAS row). Plain CAS writes
   require `LASTDB_ALLOW_PLAIN_CAS=1`.

### Mini file-blob plane (threat surface)

| Asset | Where it lives | Sensitivity |
|---|---|---|
| File blob bytes (photos, packs, imports) | Local `cas_blobs` (LastStore) + cloud CAS | Highest — raw user files |
| File KDK / per-blob DEK | Inside sealed atom content / access metadata | Critical — unlocks blob bytes |

Mini file-blob plane is **live** (`/api/db/file-blob`, fetch, fork). Durable local
cache must not store plaintext blob bytes when a DEK is available. Packaging of
atom hash groups remains structural plaintext — body secrecy is content seal +
file-KDK, not frame AEAD alone.

**Terminal product bar:** `scripts/operation-trinity-cow-bar.sh` on a CoW of a
real home: Father encoding log + Board Option I + mk: storage scan; Son reseal
coverage + `ENC:` at-rest samples + unit plain-open-fail under STRICT; Ghost
`blob_cas` unit tests.

Design: `fold_db/docs/DESIGN_OPERATION_TRINITY.md` · brain `project-operation-trinity`.

---

## 1. Scope

In scope: everything a fold_db node persists on the local disk —
the Last Store home, sensitive files under `$FOLDDB_HOME`, uploaded file
blobs, the embedding index, and the key material protecting them.

Out of scope (separate workstreams):

- Network/transport security, CORS/bind hardening, and the loopback
  trust boundary (`fold_db_node/CLAUDE.md` § "Trust boundary: loopback
  owner context" tracks that).
- App-identity enforcement and per-app isolation (`app_security_model.md`
  I1–I4).
- Cloud-side (Exemem/B2) storage — already E2E-encrypted before egress;
  the cloud only ever sees ciphertext.
- Embedding-index encryption — **in flight** on card
  `fold-encrypt-index-at-rest`; referenced below but not re-specified here.

## 2. Assets

| Asset | Where it lives today | Sensitivity |
|---|---|---|
| Atom content (user records, "molecules") | Last Store `atoms` collection plus sealed tip rows | Highest — the user's actual data |
| Derived metadata (idempotency, process results, per-atom metadata) | Last Store metadata / idempotency / process-result collections | High — derived from content |
| Embedding vectors | Last Store `native_index` collection (`emb:` / `graveyard:emb:` keys) | High — embeddings of private content leak that content |
| Uploaded file blobs (photos, PDFs, imports) | Mini local `cas_blobs` + cloud CAS under per-blob DEK (file KDK in sealed atom access metadata); desktop `UploadStorage` removed | Highest |
| Schemas, schema states, views, transforms, lineage | Last Store schema/catalog and lineage collections | Medium — reveals what kinds of data the user keeps and how records relate |
| Permissions & trust data | Last Store permission/public-key collections | Medium |
| Consent ledger (app grants) | Last Store app-identity consent collection (`fold_db_node/src/fold_node/consent.rs`) | Medium-high — which apps may read what |
| Node identity (Ed25519 private key) | Last Store `node_identity` collection (`fold_db_node/src/identity.rs`) | Critical — root of E2E key derivation AND signing identity |
| Node config sensitive fields | Last Store `node_config` collection (`NodeConfigStore`) | Critical (contains identity private key) |
| Exemem credentials (session token, API key) | `$FOLDDB_HOME/credentials.json` / `.enc` (`fold_db_node/src/keychain.rs`) | Critical |
| LLM / web-search API keys | Encrypted files via `secure_store` (os-keychain builds) or plaintext fallback | Critical |
| Master key | OS keychain item `com.folddb.node`/`master-key`, or `FOLDDB_MASTER_KEY` env | Critical — the root |

## 3. Adversaries

- **A1 — Stolen laptop / stolen disk.** Attacker has the powered-off disk
  (or a disk image). Sees every file; does not see the OS keychain
  contents (Keychain is itself encrypted under the login password) and
  does not see process memory. *This is the canonical password-manager
  adversary and the primary one this design defends against.*
- **A2 — Backup exfiltration.** A Time Machine / rsync / cloud-backup
  copy of `$FOLDDB_HOME` (and possibly the launchd plist) leaks.
  Equivalent to A1 plus whatever secrets were pinned in the plist
  (`FOLDDB_MASTER_KEY` — the Lane-D deployment shape `secure_store.rs`
  is migrating off via B1d).
- **A3 — Malicious local app / same-user process.** Code running as the
  same OS user while the node is running. Can read any file the user can
  read and can talk to the loopback HTTP API. At-rest encryption is
  **not** the primary defense here (the API answers with plaintext —
  that's the app-isolation workstream), but at-rest design must not make
  A3 *worse* (e.g. keys readable from world-listable env/plists), and
  the OS-keychain code-signature ACL (`secure_store/codesig.rs`) is the
  one at-rest control that does bite A3.
- **A4 — Cloud / sync provider compromise.** Exemem or B2 is breached.
  Already mitigated: sync and blob upload payloads are E2E-encrypted
  client-side before egress (`factory.rs` sync stack,
  `ingestion/helpers.rs::store_file_content_addressed`).

Non-adversaries (explicit): root/kernel-level malware, hardware
attackers with the machine powered on (memory bus), and the user
themselves.

## 4. Current state (verified)

### 4.1 Key material today

Boot KEK-resolution order
(`decision-2026-06-29-drop-login-keychain-keyfile-now-secure-enclave-later`):

```
passphrase → Argon2id → KEK       [OPTIONAL recovery root]
  │  params persisted in $FOLDDB_HOME/passphrase.params (salt + m/t/p);
  │  passphrase from FOLDDB_PASSPHRASE_FILE (secure) / FOLDDB_PASSPHRASE
  │  (insecure) / interactive prompt. Absent root or no passphrase supplied
  │  → falls through (never an error). Wrong passphrase → HARD ERROR on the
  │  keyring unwrap (never a re-mint). Set via `folddb keyring set-passphrase`.
  └─ else FOLDDB_MASTER_KEY env var (64 hex) [gated by FOLDDB_ALLOW_ENV_MASTER_KEY
  │                                           on app-isolation builds — B1c]
  └─ else random per-install key file ($FOLDDB_HOME/at_rest_key, 0600)
  ▼
master key (random 32 B)  ── AES-256-GCM ──► node_identity Sled tree (ENC: blobs)
                                             credentials.enc
                                             LLM / web-search API key files

node identity Ed25519 seed ──X25519──► HKDF-SHA256 (salt "fold:e2e:v1")
                                          ├─ encryption_key ── AES-256-GCM ──► `main`, `metadata`
                                          │                                    namespaces, file blobs,
                                          │                                    node_config sensitive fields
                                          └─ index_key (HMAC-SHA256)  ──► **no production callers** (see 4.4)
```

- Master-key resolution: `fold_db_node/src/secure_store.rs`
  (`try_get_master_key_with_source`). No-silent-mint discipline: only
  `initialize_master_key` (identity bootstrap) may mint; every other
  caller uses `get_master_key` and errors when no key exists.
- E2E keys: `fold_db/crates/core/src/crypto/e2e.rs`, derived from the
  identity seed in `fold_db_node/src/fold_node/node.rs::load_e2e_keys`
  ("no separate e2e.key").
- Envelope: `fold_db/crates/core/src/crypto/envelope.rs` — AES-256-GCM,
  format `[version:1][nonce:12][ciphertext+tag]`. **No key-id field and
  no AAD** — a ciphertext does not say which key sealed it, and nothing
  binds it to its storage location.

### 4.2 What is encrypted at rest today

| Surface | Mechanism | Key | Caveat |
|---|---|---|---|
| `main` (atoms), `metadata` namespaces | `EncryptingNamespacedStore` → `EncryptingKvStore` (`ENC:` + base64 envelope), wired unconditionally in `fold_db/crates/core/src/fold_db_core/factory.rs` | E2E encryption_key | `migration_mode: true` is hardcoded — plaintext values are accepted on read, forever |
| Uploaded file blobs | *n/a in Mini* — former desktop path used `encrypt_envelope` before local/B2 upload storage | E2E encryption_key | removed with desktop upload-storage surface |
| Node identity tree | `ENC:` envelope (`identity.rs`) | master key | **os-keychain builds only**; plaintext otherwise |
| `node_config` sensitive fields | `ENC:` envelope (`NodeConfigStore::with_crypto_key`) | E2E encryption_key | plaintext-read fallback for legacy values |
| Exemem credentials | `credentials.enc` via `secure_store` | master key | **os-keychain builds only**; plaintext `credentials.json` (0600) otherwise |
| Anthropic / web-search API keys | `secure_store::encrypt_and_write` | master key | **os-keychain builds only**; plaintext fallback otherwise (incl. `FOLDDB_DISABLE_KEYCHAIN=1`) |

### 4.3 What is plaintext at rest today

- **Sled namespaces:** `schemas`, `schema_states`, `schema_superseded_by`,
  `node_id_schema_permissions`, `public_keys`, `idempotency`,
  `process_results`, `views`, `view_states`, `transform_field_overrides`,
  `lineage_forward`, `lineage_reverse`, `native_index` (the
  `ENCRYPTED_NAMESPACES` allowlist in
  `fold_db/crates/core/src/storage/encrypting_namespaced_store.rs` is
  exactly `["main", "metadata"]`; everything else passes through).
- **Embedding vectors** (`emb:` keys in `native_index`) — encryption in
  flight on card `fold-encrypt-index-at-rest`.
- **Direct Sled trees that bypass the namespace layer entirely:**
  `app_identity:consent_requests` (consent ledger), the job-tracker tree,
  sync bookkeeping (`sync_cursors`), and non-sensitive `node_config`
  fields.
- **Sled keys everywhere** — only *values* are encrypted; key names
  (schema names, atom UUIDs, org hashes) are plaintext by design to keep
  `scan_prefix` working.

### 4.4 The load-bearing weaknesses

1. **The shipping headless build has zero effective at-rest encryption.**
   `release.yml` builds the distributed CLI binaries (`folddb`,
   `folddb_server`, `folddb_mcp`) with `--no-default-features`, which
   excludes `os-keychain` (load-bearing per
   `docs/release-pipeline-monorepo.md` — a sandboxed launchd restart
   can't answer the keychain prompt). In that configuration, with no
   `FOLDDB_MASTER_KEY` set, the identity Ed25519 seed is stored
   **plaintext** in the `node_identity` tree. The E2E key is a pure
   function of that seed (`E2eKeys::from_ed25519_seed`). So adversary A1
   reads the seed off the disk, derives the E2E key, and decrypts
   `main`, `metadata`, and every file blob. The encryption of atom
   content is *vacuous* against the primary adversary in the default
   shipped configuration. A password manager never has this property.
2. **`migration_mode = true`, permanently.** The factory hardcodes
   dual-read (`factory.rs`), so a plaintext value in an encrypted
   namespace is silently accepted forever. There is no "migration
   finished, refuse plaintext" switch, which means a downgrade (or a
   bug that writes plaintext) is invisible.
3. **Stale claim in code:** the comment on `ENCRYPTED_NAMESPACES` says
   "Index terms use HMAC-SHA256 blind tokens (see `E2eKeys::blind_token`)"
   — but `blind_token` and `index_key()` have **no production callers**.
   The native index is embedding-based and stored plaintext. The blind
   index exists only as a primitive.
4. **No rotation story.** One static master key, one static E2E key
   derived from a never-rotating identity seed, and an envelope format
   with no key-id. Rotation today means trial-decrypting everything.
5. **No memory hygiene.** No `zeroize` anywhere in fold_db/fold_db_node
   (only transitively inside `ed25519-dalek` via `app_identity_crypto`).
   Master keys, seeds, and derived keys are plain `[u8; 32]` copied by
   value through many call sites and left for the allocator.
6. **No lock semantics.** Keys live in process memory for the life of
   the daemon; there is no locked state, no eviction, no re-auth.
7. **Trust-boundary drift.** `fold_db_node/CLAUDE.md` documents the
   "loopback owner context" boundary, which assumed a single-user Tauri
   node. The platform thesis (app-store-over-your-data, multi-app
   nodes) obsoletes that assumption; at-rest posture must hold even
   when the node hosts apps the user doesn't fully trust (adversary A3
   matters more, not less, over time).

### 4.5 LastStore catalog plaintext — measured, and why the recorded reason was wrong (2026-07-25)

§4.1 through §4.4 above preserve the **sled-era** design record and are not the
current Mini storage map. The shipped backend is
LastStore, whose at-rest policy is **atoms-only sealing** (fold #782): atom
content, blobs, tips, change feed and sync capture carry the value-level `ENC:`
seal; the boot-critical catalogs in `LASTSTORE_PLAINTEXT_NAMESPACES` do not.

**Measured on the live primary**, read-only, classifying every record body as
`ENC:`-enveloped vs cleartext:

| collection | encrypted | plaintext |
|---|---|---|
| `atoms` | 12252 | 0 |
| `tips` (write target), `field_update_order_log`, `sync_capture`, `change_feed`, `cas_blobs` | all | 0 |
| `field_tips` (2026-07-25 pin; leftover residue, not a write target) | all | 0 |
| `schemas` | 0 | 37 |
| `schema_index` | 0 | 2734 |
| `schema_states` | 0 | 37 |
| `idempotency` | 0 | 1140 |
| `public_keys` | 0 | 1 |
| `native_index` | 0 | 472 |

The policy is behaving exactly as designed. **The defect was in its stated
justification, not its behaviour.** `LASTSTORE_PLAINTEXT_NAMESPACES` used to
justify the exemption as "LastStore's cabinet/frame layer owns device-local
at-rest protection", and #782's problem statement said the catalogs stay
"readable under the LastStore cabinet". Both assume a frame layer is carrying
at-rest protection for those namespaces.

There is no such layer in the deployed configuration. Under `packaging=plain`
— the default for new hash-group homes and what the primary runs
(`laststore-layout-v1` → `packaging=plain`) — there is **no LastStore
`data_key` and no frame AEAD**; `open_primary_store` says so in its own
comment. Segment records are `segfmt` framed but unsealed, which is how the
table above was produced: with no key. Frame AEAD applies only to legacy
`frame_aead` (LSF1) homes.

So the accurate claim is: those catalogs are **unprotected at the LastDB
layer**, and their device-local protection is the host filesystem's
(FileVault / LUKS / dm-crypt).

**Residual exposure, calibrated — corrected 2026-08-03 (was wrong below).**

- **Cloud is affected too.** There are two independent backup planes: the
  snapshot plane (`Snapshot::seal()`) encrypts under the account E2E key, but
  the LastStore **chunk** plane (`backup_uploader.rs` + `backup_manifest.rs`)
  has no caller of `seal()` — it ships on-disk `.seg` bytes verbatim. At-rest
  state **is** cloud state for every namespace the chunk plane includes.
  Backup-inclusion, at-rest-exemption, and store-level capture-skip are three
  separately-declared constants (`BACKUP_EXCLUDED_EXACT` in
  `backup_manifest.rs`, `LASTSTORE_PLAINTEXT_NAMESPACES` in
  `encrypting_namespaced_store/mod.rs`, `CAPTURE_SKIP_NAMESPACES` in
  `sync/policy.rs`) that must agree on which namespaces are exempt from
  encryption vs excluded from cloud backup; the first two are enforced
  in-code by `backup_role_matches_at_rest_encryption_exemption`
  (`backup_manifest.rs`), which fails the build if someone adds a namespace to
  one list without the other. See
  `lastdb-cloud-backup-ships-cleartext-schema-chunks-20260803`.
- Full-disk encryption covers powered-off theft, and stops helping exactly
  where §5.3 says it does — "protects nothing once the volume is mounted".
  That case is not hypothetical: the agent fleet routinely `cp -cR` clones the
  data dir (`lastdb-smoke-test`, `lastdb-safe-upgrade` ephemeral probes) and
  `lastdb backup` / restore moves homes around. It also applies to any host
  without full-disk encryption.
- What leaks is **schema shape and descriptive names**, plus derived
  index/state metadata — never atom content. Much of the list is
  uninteresting on its own terms: `public_keys` is public by definition,
  `idempotency` is UUIDs, `schema_states` is `"Available"`. The entries worth
  arguing about are `schemas`, `node_id_schema_permissions` (access-control
  metadata) and `lineage_forward` / `lineage_reverse` (derivation graph);
  several of those hold zero rows today.

**Cost of reversing it**, if that is ever revisited: decrypting these
namespaces adds boot-time AES on exactly the catalogs the startup path reads
(schema load, lineage walks). That is the same cost §5.3 measured and accepted
for the strict flip — but it is also why `native_index` was moved *back* to
plaintext in 2026-07, where value-level AES on ~20k rows dominated cold boot.
Any change here is per namespace and must be measured, not a blanket revert,
and it must go through the strict-marker clearing path (#877) rather than a
bare edit to the constant.

**2026-08-03 update — gap G1 first card (SUPERSEDED, see below).** The four
entries called out above
as "worth arguing about" (`schemas`, `node_id_schema_permissions`,
`lineage_forward`, `lineage_reverse`) moved off `LASTSTORE_PLAINTEXT_NAMESPACES`
onto the encrypted set via the strict-marker clearing path (#877); they are
picked up automatically by the existing `DEFAULT_ENCRYPT_FLIPPED_NAMESPACES`
boot migration. `public_keys`, `schema_states`, and `idempotency` stay
plaintext (uninteresting content, per above). `native_index` and `schema_index`
stay plaintext on the boot-cost precedent this section already documents for
`native_index`; `schema_index` was not independently re-measured in this pass.
`schema_superseded_by`, `process_results`, `views`, `view_states`, and
`transform_field_overrides` were left untriaged this pass rather than
blanket-reverted. Cold-boot delta and the raw-read proof are recorded in the
PR for the first card; the live primary migration itself is a separate,
human-gated follow-up (`lastdb-migrate-primary-to-encrypted-schema-namespaces`).

**2026-08-03 — SUPERSEDED by the Trinity-only encryption standard (won't-undo,
Tom).** Gap G1's catalog default-encrypt direction is **retired as a product
goal**, and the update above is kept only as a record of what briefly landed.
The governing rule is now brain
`preference-lastdb-encryption-standard-trinity-only` /
`decision-2026-08-03-encryption-standard-trinity-only`:

> Published schema definitions are not a secret.

The product encryption story is exactly four seals, and nothing else may be
invented or expanded by agents:

| Layer | What | Crypto |
|---|---|---|
| Hash key | molecule HashKey in storage | blind (`blind_v1`) |
| Range key | molecule RangeKey in storage | OPE (`ope_v1`) |
| Atom content | field `content` | seal under account E2E key (`ENC:`) |
| File blob | CAS / file bytes | per-blob DEK; KDK in sealed atom access metadata |

Consequences for this section:

- `schemas`, `node_id_schema_permissions`, `lineage_forward` and
  `lineage_reverse` are **back on `LASTSTORE_PLAINTEXT_NAMESPACES`**. The
  "entries worth arguing about" framing above is superseded — they are catalog
  state, not secrets, and the argument is settled.
- The boot migration needs no new code in either direction. Returning a
  namespace to the plaintext allowlist is already handled by
  `migrate_laststore_plaintext_policy_namespaces`, which clears the stale
  strict marker and unwraps residual `ENC:` rows; the encrypt sweep skips it
  via `should_encrypt`. Homes that swept under #1153 unwrap on next boot and
  stay readable throughout via dual-read.
- The primary migration follow-up
  (`lastdb-migrate-primary-to-encrypted-schema-namespaces`) is **cancelled**,
  not deferred.
- Cleartext catalog chunks in the object store are **not** a confidentiality
  incident. Retracting historical schema chunks from R2 is moot, and cloud
  re-enable must not be blocked on "schemas not encrypted at rest".
- The backup coupling still holds but now demands a *declaration*: an at-rest
  plaintext namespace must be either in `BACKUP_EXCLUDED_EXACT` or in
  `BACKUP_CLEARTEXT_CATALOG_NAMESPACES`, and no Trinity surface may appear in
  the latter. `schemas` stays backed up — it is a source-of-truth collection,
  and excluding it would make a restored device an unusable copy of the primary.

Agents: check that preference **before** filing any encryption card. Sections
of this document that still read "default-encrypt all namespaces" or "encrypt
schemas at rest" are superseded by it and should be corrected when touched.

## 5. Target architecture

### 5.1 Key hierarchy

```
                 ┌────────────────────────────────────────────┐
   unlock roots  │  OS keychain item (code-signature ACL'd)   │   either, or both
                 │  user passphrase ──Argon2id──► KEK bytes   │   (recovery code = printable KEK export)
                 └──────────────┬─────────────────────────────┘
                                ▼
                     KEK ("master key", 32 B)
                                │  AES-256-GCM key-wrap
                                ▼
              keyring file: $FOLDDB_HOME/keyring.enc
              { key_id → wrapped DEK, purpose, created_at, state }
                                │
        ┌───────────────┬───────┴────────┬─────────────────┐
        ▼               ▼                ▼                 ▼
   store DEK       index DEK        blob DEK         identity DEK
   (all Sled       (embeddings,    (uploaded        (node_identity,
    values)         future blind    files)           credentials,
                    tokens)                          API keys)
```

Properties this buys:

- **Cheap KEK rotation** (passphrase change, keychain re-mint): re-wrap
  a handful of DEKs; no data re-encryption.
- **Per-purpose DEKs**: the embedding index, blob store, and main store
  can rotate independently; compromise of one derived surface doesn't
  hand over the others.
- **Passphrase root via Argon2id** (memory-hard; parameters stored next
  to the wrapped keys so they can be raised over time) gives headless
  and keychain-less platforms a real root instead of a plaintext file —
  and gives desktop users an *additional* factor if they want one.
  Default parameters (Argon2id, OWASP-aligned, tuned for an interactive
  unlock): **m = 64 MiB, t = 3, p = 4**, with a fresh 128-bit random salt
  per root. The `(m, t, p, salt)` tuple is the only material persisted
  (`passphrase.params` beside `keyring.enc`); the derived KEK never
  touches disk. Implemented in `crypto::passphrase`
  (`PassphraseRoot::derive_kek` / `generate` / `serialize`) — the KDF +
  params-persistence foundation; the `secure_store` `MasterKeySource`
  variant and the `init --passphrase` / `unlock` / `passphrase change`
  CLI are later slices.
- **Identity decoupled from data encryption.** Today the Ed25519 seed
  is both signing identity and encryption root; under the target
  hierarchy the DEKs are random and merely *wrapped* by the KEK, so
  reading the identity seed no longer derives any data key, and the
  identity key itself becomes just another wrapped secret. (The
  existing `E2eKeys::from_ed25519_seed` derivation remains as the
  legacy key_id for migration.)
- **No-silent-mint preserved and extended**: the keyring file is the
  single mint point; if `keyring.enc` exists but no root can unwrap it,
  every consumer refuses (same shape as `get_master_key` vs
  `initialize_master_key` in `secure_store.rs` today — that split stays).

### 5.2 Envelope v2

Extend `crypto/envelope.rs` with version `0x02`:
`[version:1][key_id:4][nonce:12][ciphertext+tag]`, with the storage
context (namespace + local-store key) supplied as AAD. Version `0x01` remains
readable for migration. key_id makes rotation observable and makes
"which key sealed this?" a lookup instead of trial decryption; AAD stops
ciphertext-swap within the store.

### 5.3 The architectural fork: where does whole-store encryption sit?

Three candidates, evaluated honestly:

| Option | What it is | Pros | Cons |
|---|---|---|---|
| **(a) KvStore-trait seam** — extend the existing `EncryptingNamespacedStore` decorator to encrypt **all** namespaces (default-encrypt, explicit allowlist of plaintext namespaces = empty) | Values-only encryption above the local store, exactly where `main`/`metadata` already encrypt | Mechanism already exists, tested, and proven in prod; zero Last Store format risk; keys stay plaintext so `scan_prefix` and iteration order are untouched; org-crypto routing already lives here; per-namespace DEK selection is natural | Local-store keys (schema names, atom ids) stay visible; every direct store caller bypasses it (consent store, job tracker — those must be moved behind the seam or wrapped individually); ~30 % size overhead from `ENC:`+base64 (fixable by storing raw envelope bytes instead of base64 for binary-safe trees) |
| **(b) Per-tree/per-prefix encryption** inside each domain store | Each store (consent, lineage, …) encrypts its own values | Fine-grained control | N stores × N implementations of the same wrapper; guaranteed drift (the consent tree exists precisely because someone bypassed the seam); no single audit point |
| **(c) Filesystem-level (FileVault / LUKS / per-directory encryption)** | Rely on OS disk encryption | Zero code; covers local-store keys and file metadata too | Not portable policy (user may not have FileVault on; Linux servers vary); protects nothing once the volume is mounted (any A2 backup of the mounted FS is plaintext); invisible to `folddb doctor`; outsources the core product promise — a password manager does not say "just turn on FileVault" |
| **(d) Encrypted storage engine** (swap the local store for an encrypted LSM / SQLCipher-style engine) | Engine-level pages encrypted, keys included | Strongest coverage incl. key names | A storage-engine migration is a different, much larger project; the local storage engine is load-bearing everywhere (pool semantics, idle-reaper, multi-process locking) |

**Recommendation: (a), with (c) documented as recommended
defense-in-depth.** Concretely:

- Invert `ENCRYPTED_NAMESPACES` into `PLAINTEXT_NAMESPACES: &[&str] = &[]`
  — encrypt by default; adding a plaintext namespace requires a code
  change with a justification comment.
- Move the bypassing direct-`open_tree` consumers (consent store, job
  tracker, sync bookkeeping) behind the namespace seam (or wrap them in
  `EncryptingKvStore` directly where the `NamespacedStore` interface
  doesn't fit, e.g. the sync-aware trees).
- Accept the plaintext local-store-key leak explicitly for now (medium
  sensitivity: schema names + UUIDs, not content), and note HMAC'd key
  names (the existing `blind_token` primitive) as a possible later
  hardening for `scan_prefix`-compatible key blinding.
- Perf: AES-256-GCM runs at multiple GB/s with AES-NI; values dominate
  stored row size; keys (and therefore iteration/scan order) are
  untouched. The known hot spot is bulk scans decrypting every value —
  the same cost `main` already pays today, and the embedding card is
  measuring the worst case (decrypt-every-candidate semantic search)
  before/after. Decision: per-value decryption at the seam, with
  decrypt-once-into-memory caches allowed at specific hot consumers if
  the embedding card's numbers demand it.
- **Strict check at startup (G1d).** The default-encrypt rollout uses
  `EncryptingNamespacedStore::enable_strict_if_clean` to enable strict reads.
  A durable strict marker avoids the repeated check after a successful proof.
  Without that marker, the check tests the existing envelope predicate. It
  does not decrypt a value or decode base64.
  Native LastStore reads at most 16 rows from one physical group per page.
  Logical `main` reuses a native page value when its source and key identify
  the authoritative value. The canonical first form in the first authoritative
  collection needs no additional read. For other supported forms or collections,
  bounded native key-presence batches must prove each earlier candidate absent.
  The probes follow the authoritative form and collection order. A present
  earlier candidate requires the logical batch read. A presence error propagates.
  Page hydration already resolves the current value within its collection.
  Unknown source provenance also requires the logical batch read. Thus, an old
  copy cannot override the current value. The full fallback batch must succeed
  before a plaintext veto.
  A plaintext value stops the check and preserves dual-read mode. A read error propagates
  and leaves the strict state and marker unchanged. Only a complete successful
  check permits a new strict marker. The retained page has a row bound, not
  an absolute byte bound; existing value and shard sizes still apply. A separate
  inventory retains one collection's group identifiers. The check creates that
  inventory once, from disk and resident groups. It runs before normal writers
  and replay; raw plaintext writes must not occur at the same time. Backend
  warm and key caches retain their independent budgets. Other
  storage backends retain their prior full-prefix fallback. This internal
  startup check does not add a product query operation.
  Tests in `storage::laststore::startup_predicate` cover page limits, early
  exit, late plaintext, old copies, read errors, and the durable fast path.
  A cold colon-key fixture checks all 65 values with zero logical body rereads.
  It also limits native shard loads under a one-byte warm-cache budget.

### 5.4 No plaintext fallback in shipping builds

Policy (replaces the "SSH private-key model" for shipped binaries):

| Build | Root today | Root after |
|---|---|---|
| Tauri desktop (os-keychain) | OS keychain | **random per-install key file by default**, passphrase optional; existing keychain-rooted profiles migrate first |
| Shipped headless CLI / launchd (`--no-default-features`) | **none → plaintext** | **always has a root**: the DEFAULT is now a **random per-install key file** (`$FOLDDB_HOME/at_rest_key`, `0600`, auto-generated on first run), used when neither a passphrase nor `FOLDDB_MASTER_KEY` resolves. The refuse-to-start gate is now satisfied honestly by the key-file root — passwordless and keychain-free by default — so a fresh install BOOTS (encrypted) instead of refusing. The gate still fires only in the (rare) case where no root resolves at all (e.g. `$FOLDDB_HOME` unresolvable). |
| Dev/test builds (`cargo run`, CI) | plaintext | the random key-file root is disabled under `#[cfg(test)]` (so unit suites keep their rootless plaintext shape); for a deliberately keyless `cargo run`, plaintext is allowed **only** behind an explicit `FOLDDB_INSECURE_PLAINTEXT=1` opt-in; never the silent default |

The refuse-to-start behavior is the same philosophy as the existing
refuse-to-mint and refuse-to-rotate guards: fail loudly rather than
degrade silently. `folddb doctor` and the boot-time
`audit_master_key_security` (B1e) report the active root.

#### Default at-rest root: random per-install key file (accepted)

Decision of record: `design-at-rest-random-keyfile-default-root` (Tom,
2026-06-27). The DEFAULT at-rest KEK root is a **random per-install key file**,
the fallback root in the boot order:

```
Argon2id passphrase → FOLDDB_MASTER_KEY → random key file (default)
```

On first run, when no higher root resolves, a random 32-byte KEK is generated
with the OS CSPRNG, written to `$FOLDDB_HOME/at_rest_key` (`0600`, owner-only,
beside `keyring.enc` / `passphrase.params`), and used as the master key. On
every subsequent boot it is read back. It feeds the SAME keyring/KEK machinery
as every other root (it produces the KEK that wraps the existing keyring DEKs and
seals the identity tree) — it does not bypass the keyring. Implementation:
`fold_db_node/src/secure_store/keyfile.rs`, wired into
`secure_store::try_get_master_key_with_source`; classified
`MasterKeySource::RandomKeyFile`.

Why: it makes onboarding **passwordless and keychain-free by default** while
keeping an Argon2id passphrase as an optional higher-priority root and leaving
the real door-2 answer to the Secure Enclave follow-up. It is a REAL key root,
so the Gap G2 refuse-to-start gate is satisfied HONESTLY — no
`FOLDDB_INSECURE_PLAINTEXT`, no password, no shared baked-in default password
(each install's key is independent and random).

**Threat model (accepted).** The key file protects against an attacker who
copies the data dir WITHOUT the key file (adversary A1/A2 partial — weak but
non-zero). It does NOT protect against a same-user process (adversary A3) that
can read the whole node home — that process reads `at_rest_key` and unwraps
everything. This is the **same same-user profile the keychain has as deployed
today**: the master key currently sits in a plaintext `0600` launchd plist any
same-user process can read (the door-2 problem). So the key-file default does
not make A3 worse than the status quo; it removes the operational pain (no prompt,
no password) for the smooth default. It is therefore classified
**not secure-at-rest** (`MasterKeySource::is_secure_at_rest() == false`), and
`folddb doctor` / the B1e audit surface the upgrade hint (set a passphrase or
run an OS-keychain build).

This does NOT supersede the vault / password-manager / door-2 target
(`projects-vault-password-manager-mode`, `design-security-review-2026-06-15`);
the keychain remains the upgrade path and the long-term target. It only makes
the smooth default a real (if same-user-recoverable) at-rest root instead of
plaintext, and decouples passwordless onboarding from door 2.

Permissions discipline: the file is created `0600` via the atomic
tmpfile+rename helper (never momentarily group/other-readable), and an existing
key file with looser-than-`0600` permissions, or a wrong length, is a HARD ERROR
— no silent fallback and no silent tighten, matching the codebase's fail-loud
posture for sensitive files.

#### Desktop (`os-keychain`) no-password default also roots in the key file (2026-06-29)

Decision of record: `lastdb-desktop-no-password-root-keyfile-not-keychain` (Tom,
2026-06-29). Originally the random key-file root was the default only on the
headless build; the **desktop** (`os-keychain`) no-password tier still minted an
OS-keychain master key. That made macOS raise a keychain-access prompt on every
launch whenever the signed binary's ACL didn't match the item's (ad-hoc /
locally-built updates, or an item left in the legacy `com.folddb.node` slot by
the FoldDB→LastDB rename). The fix removes the dependency rather than fighting
the ACL:

- **New desktop installs** root the no-password tier in the random key file too.
  `bootstrap_identity_no_password` resolves the shared
  `try_get_master_key_with_source` root (the key file on a fresh install) and
  **never mints an OS-keychain item** — so macOS never prompts.
- **Existing keychain-rooted profiles** are re-rooted onto the key file by
  `secure_store::migrate_keychain_root_to_keyfile`, driven from the Tauri app's
  startup migration seam. It copies the master key VERBATIM out of the keychain
  (current **and** legacy slot) into `at_rest_key` — same bytes, so `keyring.enc`
  and the identity stay wrapped under an identical KEK (no rewrap, no rotation) —
  verifies the read-back, then DELETES the keychain item(s). It is
  safe-by-construction and reversible until the final delete: the keychain stays
  the live root until the key file is proven good. A possible one-time "Always
  Allow" prompt is the LAST keychain read; afterwards no boot touches the
  keychain, so the prompt never recurs.

The OS keychain is thereby **removed from normal no-password boot resolution**.
The only remaining keychain read for this path is the startup migration that
copies an existing keychain KEK into `at_rest_key`, verifies it, and deletes the
keychain item(s). This is an at-rest-default change, not the door-2 answer: the
real same-user-process hardening remains the Secure Enclave follow-up. Residual
A3 (same-user) exposure is unchanged from the key-file analysis above.
Follow-up: when the remember-this-device keychain cache (PR5) lands, the re-root
migration must be gated so it does not clobber that opt-in cache.

### 5.5 Lock/unlock semantics

fold_db **will support a locked state** (today it has none):

- **Unlocked**: KEK present → DEKs unwrapped into memory (zeroize-on-drop
  guards). Normal operation.
- **Locked**: DEKs and KEK zeroized and evicted. Reads/writes of
  encrypted surfaces return a structured `423 Locked`-style error;
  health endpoint and unlock endpoint stay available. Triggers: explicit
  `folddb lock` / API call; optional auto-lock timer; OS sleep hook
  later. Unlock: re-resolve the root (keychain read or passphrase) —
  same code path as boot.
- Boot starts in *locked* and immediately auto-unlocks when a
  non-interactive root (keychain, env) resolves — so interactive
  passphrase nodes are the only ones that stay locked awaiting input.
- Non-goal for v1: locking the *process memory* of in-flight request
  buffers; the guarantee is about key material and refusing new
  decryption work.

### 5.6 Tamper & recovery story

- AES-GCM tags already give per-value tamper detection; envelope v2 AAD
  adds location binding. Surfaced as errors, never silent fallback
  (kill the permanent `migration_mode`).
- Backup/export: a backup of `$FOLDDB_HOME` (Last Store home + `keyring.enc` +
  blobs) restores fully given any one unlock root — the wrapped-DEK
  keyring travels with the data, so ciphertext is never orphaned by
  device loss. A printable **recovery code** (the KEK, exported once at
  init, à la password-manager secret keys) covers keychain loss for
  passphrase-less nodes.
- The no-silent-mint discipline (`secure_store.rs` §"Silent-mint
  hazard") is preserved verbatim and extended to the keyring file.

## 6. Migration sequencing

Ordered to keep every intermediate state shippable and the port-9001
long-lived node safe (no data loss, no full re-index):

1. **Zeroize key material** (no format changes, pure hygiene) —
   `fold-zeroize-key-material`.
2. **Embedding-index encryption** — in flight, card
   `fold-encrypt-index-at-rest` (independent of the keyring; uses the
   existing master-key path; its migration pattern — version-byte
   dual-read, lazy re-encrypt — is the template for step 4).
3. **Keyring + envelope v2 + rotation plumbing** (introduce wrapped
   DEKs; existing data keeps decrypting under legacy key_ids) —
   `fold-master-key-rotation`.
4. **Whole-store encryption at the KvStore seam** (default-encrypt all
   namespaces + absorb the direct-tree bypasses; lazy re-encrypt on
   write, one-shot `folddb migrate encrypt-at-rest` to finish; then flip
   migration_mode off per store) — `fold-encrypt-main-kv-store`.
5. **Argon2id passphrase root** (alternative/additional KEK root) —
   `fold-passphrase-argon2id-root`.
6. **Remove plaintext fallback from shipping builds** (refuse-to-start
   policy; needs 5 so headless nodes have a real root to switch to) —
   `fold-remove-plaintext-fallback`.
7. **Lock/unlock semantics** (needs 3's in-memory DEK objects) —
   `fold-lock-unlock-semantics`.

## 7. Gaps → follow-up cards

| # | Gap | Card |
|---|---|---|
| G1 | Most catalog collections and direct-store bypasses plaintext | `fold-encrypt-main-kv-store` |
| G2 | Shipping headless builds fall back to plaintext (incl. identity seed → E2E key derivable from disk) | `fold-remove-plaintext-fallback` — **addressed**: refuse-to-start gate landed, AND the default at-rest root is now a random per-install key file (`design-at-rest-random-keyfile-default-root`, §5.4 "Default at-rest root"), so the shipped headless build is encrypted-at-rest by default (no plaintext fallback, no password). Same-user (A3) recovery is the accepted residual, identical to today's plist-stored keychain key; the keychain/door-2 target remains the upgrade path. |
| G3 | No zeroization of key material | `fold-zeroize-key-material` |
| G4 | No passphrase/Argon2id root | `fold-passphrase-argon2id-root` |
| G5 | No key rotation (no key-id, static keys) | `fold-master-key-rotation` |
| G6 | No lock/unlock semantics | `fold-lock-unlock-semantics` |
| — | Embedding index plaintext | `fold-encrypt-index-at-rest` (in flight; not re-filed) |

## 8. Non-goals

- Encrypting local-store key names (accepted leak for now; blind-token
  hardening noted in 5.3).
- Replacing Last Store with an encrypted storage engine (option d).
- Defending against root/kernel compromise or live-memory attackers.
- Runtime read-access control between apps (app-isolation workstream).
- Any prod deploy or migration of the port-9001 brain as part of these
  cards; all validation on ephemeral dev nodes.

## Portable same-key (Mini) — Tom 2026-07-18

LastDB Mini (`lastdbd`) personal-data at-rest uses the **account E2E content key**
derived from `identity.key` — the same key material as cloud log/snapshot outer
seal. Host always boots with `at_rest_keyring: None`.

Per-install keyring store DEKs remain available in core for non-Mini / tests
only. Do not reintroduce a non-portable store DEK on Mini personal data.

Design: brain `design-portable-same-key-at-rest-cloud`. Feature:
`feature-portable-same-key-at-rest`.
