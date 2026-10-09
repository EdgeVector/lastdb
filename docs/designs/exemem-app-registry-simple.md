# Exemem app registry — the simple design

Status: implemented.
Proof: `lastdb_node/scripts/app-registry-release-v2-live-e2e.sh`.

A developer declares and resolves schemas during development. A release
binds the locked schema identities, the source commit, the signed artifact
digest, and an immutable release id. Nothing registers a schema during
release or install. One host runs development sessions and published
releases under separate execution identities, directories, capability
grants, and status. A public install resolves a generation-checked channel,
verifies the release, activates it through Host Track, proves that the
desired, installed, active, and observed release ids match, and rolls back
on drift.

## 1. Ideal state

Each statement is testable on a live host. The numbers name the proof in
section 7 that tests it.

- A developer declares schemas and resolves them during development. The
  Schema Service returns the final schema identities. The existing app
  lockfile stores those identities. No new lockfile field.
- A release binds four things: the locked schema identities, the source
  commit, the signed R2 artifact digest, and the immutable release id.
  The release step writes no schema. The install step writes no schema. (1)
  - `artifact_digest` is the SHA-256 digest of the artifact bytes.
  - `release_id` is the SHA-256 digest of the canonical release manifest.
- One host runs development and published releases under separate execution
  identities, separate directories, separate capability grants, and separate
  status.
  - Development runs under `dev:<app_id>:<workspace_id>:<dev_session_id>`
    in a mutable workspace.
  - A published release runs under
    `release:<app_uuid>:<release_id>:<activation_epoch>` from
    `~/.host-track/apps/<app>/versions/<release-id>/`, and the `current`
    pointer selects the active release.
  - Development reports `DEV` or `UNMANAGED`. Development can never report
    `CURRENT`. (2)
  - A published app reports `CURRENT` only when the desired, installed,
    active, and observed release ids match and its probe is green. (3, 8)
- A public install resolves a generation-checked channel, verifies the
  release, and activates it through Host Track. Activation stops on a digest
  fault or a signature fault. (4, 5)
- On drift, the host restores the prior verified release. (9) The host runs
  that check on a cadence, not only when an operator asks. (10)
- Public reads stay anonymous. (6) Writes need a DevCert. (7) The CLI keeps
  the sandbox reserve and promote flow. LastDB access stays on exact keys.

## 2. One host, two execution identities

The host separates the two identities at four levels. No component crosses
the line.

| | Development | Published release |
|---|---|---|
| identity | `dev:<app_id>:<workspace_id>:<dev_session_id>` | `release:<app_uuid>:<release_id>:<activation_epoch>` |
| place | a mutable workspace | `~/.host-track/apps/<app>/versions/<release-id>/` |
| pointer | — | `current -> versions/<release-id>` |
| grants | workspace scope only | release scope only |
| status | `DEV` or `UNMANAGED`; `CURRENT` is not reachable | `CURRENT` on a four-way match and a green probe |
| writes | schemas, during development | no schema write, ever |

### The capability grant

The grant is derived from the identity, never configured beside it.
`CapabilityScope::of` is a total function from the execution identity, and
it has no branch that crosses: a development identity yields a workspace
grant, a release identity yields a release grant. Each grant reaches exactly
one directory, and `covers` resolves the path first, so a symlink or a `..`
segment cannot walk out of the grant.

### Status rules

These two rules are the whole status contract.

1. Development reports `DEV` or `UNMANAGED`. It can never report `CURRENT`.
   `lastdb_node::app_release_host::dev_status` is the only function that
   produces a development status, and it has no branch that yields
   `CURRENT`.
2. A published app reports `CURRENT` only when the desired, installed,
   active, and observed release ids match and its probe is green. An old
   release process observes an older release id, so it cannot report
   `CURRENT`.

## 3. Schema lock, release binding, and publish

```
1  DEVELOP   sandbox reserve -> register schemas -> promote   (CLI order, unchanged)
2  LOCK      final schema identities from the Schema Service, in the existing lockfile
3  BUILD     artifact -> R2;  artifact_digest = SHA-256(artifact bytes)
4  MANIFEST  schema ids + source commit + artifact_digest
             -> release_id = SHA-256(JCS(manifest))
5  PUBLISH   POST /v2/apps/{app_id}/releases      the release id is immutable
6  CHANNEL   PUT  /v2/apps/{app_id}/channels/{channel}   generation checked
7  INSTALL   GET channel -> GET release -> verify -> activate through Host Track
```

Steps 3 to 7 write no schema.

### Release manifest fields

| Field | Value |
|---|---|
| `app_id` | The registry namespace the release belongs to. |
| `app_uuid` | Anchors the release execution identity. |
| `schemas` | The final identities the Schema Service returned, copied from the app lockfile without change. |
| `source_commit` | The commit that produced the artifact. |
| `artifact_digest` | SHA-256 of the artifact bytes in R2. |
| `artifact_url` | Where an installer fetches the bytes. |
| `artifact_signature` | The publisher's Ed25519 signature over `artifact_digest`. |

`release_id` is the SHA-256 of the canonical (RFC 8785 JCS) manifest, so it
is not a manifest field. The manifest adds no lockfile field and no derived
schema identity. A release that references an unresolved schema fails at
publish, because the lockfile holds no identity for it and the registry
refuses the publish with `unresolved_schema`.

## 4. API surface

These are the exact routes. There is no HTTP promote route and no `/api`
prefix.

| Route | Purpose | Auth |
|---|---|---|
| `POST /v1/dev-cert` | Issue the DevCert that authorizes developer writes. | Developer |
| `POST /v2/apps` | Create the app record. | DevCert |
| `POST /v2/apps/{app_id}/releases` | Publish an immutable release. | DevCert |
| `PUT /v2/apps/{app_id}/channels/{channel}` | Point a channel at a release, under a generation check. | DevCert |
| `POST /v2/apps/{app_id}/revocations` | Revoke a release. | DevCert |
| `GET /v2/apps/{app_id}` | Read the app record. | Anonymous |
| `GET /v2/releases/{release_id}` | Read a release manifest. | Anonymous |
| `GET /v2/apps/{app_id}/channels/{channel}` | Read the desired release id and the channel generation. | Anonymous |

`POST /v1/dev-cert` is the exemem auth service. The seven `/v2` routes are
the schema service, mounted in both routers:
`schema_service_server_http::v2_scope` and the `/v2` arms of
`schema_service_server_lambda`.

Each read route resolves one exact key. The app key is the app id, the
release key is the release id, and the channel key is
`<app_id>\x1f<channel>`. The registry runs no scan and no prefix query on
the read path.

## 5. Install, activation, and the four-way proof

```
DESIRED            INSTALLED             ACTIVE                OBSERVED
GET channel   =    versions/<id>/   =    current -> <id>  =    the live process
+ generation       digest + sig ok        directory             reports its id

                   AND  PROBE green   ->   STATUS CURRENT
```

Any inequality removes `CURRENT`.

### Activation order

1. Read the channel. Keep the generation. A stale generation on a later
   write fails with a conflict.
2. Read the release manifest by release id.
3. Download the artifact. Compare the byte digest to `artifact_digest`.
   Verify the signature.
4. Unpack into `~/.host-track/apps/<app>/versions/<release-id>/`.
5. Move the `current` pointer.
6. Observe the live process. Compare the four release ids. Run the probe.

A digest fault or a signature fault stops the flow at step 3. The host never
moves the `current` pointer for a faulty artifact.

## 6. Drift and rollback

The host compares the four release ids on each check. A mismatch is drift.
The host restores the prior verified release, moves the `current` pointer
back to that release-id directory, and proves the four-way match again. Only
a release that already proved `CURRENT` is a rollback target.

An absent reading is not drift. The host rolls back only on a `DRIFT`
status — a real disagreement between release ids it could read, or a revoked
active release. Two readings are absent rather than wrong, and both are
normal:

- The channel read failed, so `DESIRED` is unknown. A registry outage is not
  evidence that the host is wrong.
- The release was activated and has not started yet, so no process has
  written an observation.

Both report `UNKNOWN` and change nothing. The next cycle reads again and
decides on real evidence. This matters because the check repeats every 60 s:
a rollback on an absent reading would make the recurring check the thing that
breaks the host.

## 7. The ten proofs

`lastdb_node/scripts/app-registry-release-v2-live-e2e.sh` runs all ten
against the real schema service binary and the real `lastdb` CLI. Exit code
0 means every proof passed.

| # | Claim | Observation |
|---|---|---|
| 1 | No schema registration during release or install | The catalog write count (`schema_writes` on `GET /v1/health`) is equal before the release and after the install. |
| 2 | No CURRENT in development | A session under `dev:<app_id>:<workspace_id>:<dev_session_id>` reads `DEV` or `UNMANAGED`. |
| 3 | No CURRENT from an old release process | An old release process is held alive after a new activation. Its observed release id differs, so the status is not `CURRENT`. |
| 4 | A digest fault or a signature fault stops activation | The artifact bytes are corrupted, then the signature is corrupted. Both runs fail before the `current` pointer moves. |
| 5 | Generation conflicts fail | Two channel writes use the same generation. The second fails with `generation_conflict`. |
| 6 | Anonymous reads work | The three `GET` routes return 200 with no credential. |
| 7 | Writes need a DevCert | Each write route without a DevCert is rejected with 401. The same call with a DevCert succeeds. |
| 8 | Desired = installed = active = observed after success | The four release ids are printed and are equal, and the probe is green. |
| 9 | Forced drift restores the prior verified release | `current` is pointed at a wrong directory. The host restores the prior verified release id and proves the four-way match again. |
| 10 | The recurring check cycle runs on the operator cadence | `current` is pointed at a wrong directory, then ONE `release-check --watch` invocation runs two cycles. Cycle 1 restores the prior verified release; cycle 2 of the same run reads `CURRENT`. |

The proof run stands in for two things. Everything else is the product.

1. **`POST /v1/dev-cert`.** Production signs DevCerts with a KMS-held ES256
   root, and a local run has no KMS. The script generates its own P-256 root
   and configures it into the schema service through
   `APP_IDENTITY_ROOT_PUBKEYS`. The schema service still verifies every cert
   and every envelope for real.
2. **The artifact transport.** Production reads `artifact_url` over HTTPS
   from R2. The script publishes a `file://` URL, and `fetch_artifact` reads
   those bytes from disk, so the run needs no object store.

Neither stand-in weakens a proof. The digest check and the signature check
in proof 4 run on the artifact bytes after the fetch returns. They are the
same comparison on either transport.

## 8. Operator settings

| Setting | Value | Why |
|---|---|---|
| Release directory retention | The active release and the two before it (`RELEASE_RETENTION = 3`) | Three directories cover one rollback and one repeat failure. |
| Probe cadence | On activation, then every 60 s (`PROBE_INTERVAL`) | The channel read, the revocation check, and the four-way check share one cycle. |
| Observation staleness window | 180 s (`OBSERVATION_STALE_AFTER`) | Past this window the observed release id reads as unknown, so `CURRENT` stays a live claim rather than a cached one. |
| Revocation reaction | The same 60 s cycle | `lastdb app release-check` reads the active release's revocation on the same cycle as the channel read. A revoked active release runs the drift path and restores the prior verified release. No push channel. |

### Running the cadence

`install_and_activate` runs the four-way check at the moment it moves the
`current` pointer — activation order step 6. A release that has not started
yet reads `UNKNOWN` there, because `CURRENT` is a live claim and no process
has written an observation.

`lastdb app release-check --watch` runs the recurring half. One cycle reads
the channel, reads the active release's revocation, runs the four-way check,
and restores the prior verified release on drift. `--interval-secs` overrides
the delay for a test or a proof and `--cycles` bounds the run; the default is
`PROBE_INTERVAL`, and `check_interval` floors any override at one second so a
watch run can never become a busy loop. Without `--watch` the command runs
exactly one cycle, which is what it always did.

## 9. Preserved behavior

- **Anonymous public reads.** The three `GET` routes need no credential.
- **DevCert writes.** Every write route needs a DevCert. One DevCert
  signature covers the write, under a purpose pinned per route
  (`app_release_publish`, `app_channel_set`, `app_release_revoke`), so a
  signature for one write cannot be replayed as another.
- **Sandbox reserve and promote.** The CLI keeps the proven order: reserve
  in the sandbox, register the schemas, then promote. This design adds no
  HTTP promote route.
- **LastDB exact-key access.** Every registry record has a deterministic
  exact key, and the read path performs an exact-key get.

## 10. Where the code lives

| Piece | Path |
|---|---|
| Release, channel, and revocation records; the release id | `schema_service/crates/core/src/app_release.rs` |
| `/v2` handlers | `schema_service/crates/server_shared/src/handlers.rs` and `handlers/*.rs` |
| `/v2` actix route table | `schema_service/crates/server_http/src/lib.rs` |
| `/v2` Lambda route arms | `schema_service/crates/server_lambda/src/routes/apps.rs` |
| Envelope purposes | `app_identity_crypto/src/envelope.rs` |
| Execution identity, Host Track, the four-way proof, drift and rollback | `lastdb_node/src/app_release_host.rs` |
| Developer-side release writes | `lastdb_node/src/app_publish.rs` |
| CLI (`lastdb app release-*`, `dev-status`) | `lastdb_node/src/bin/lastdb.rs` |
| The ten proofs | `lastdb_node/scripts/app-registry-release-v2-live-e2e.sh` |
