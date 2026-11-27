# Contract: LastDB updates itself

Date: 2026-11-27. Author: Claude, for Tom. Status: approved by Tom on 2026-11-27
(brain `decision-2026-11-27-lastdb-updates-itself-rev3-approved`). The full design and its
reasons are brain `design-lastdb-upgrade-20261127` (revision 3). This file is milestone M0:
the formats and interfaces that the later milestones build against. Nothing here is built
yet. Each later PR updates the section it builds and marks it "built".

## 1. Parts and owners

| Part | Owner | Milestone |
|---|---|---|
| `node-release` KMS key and signing-only release role | `EdgeVector/lastdb-app-registry` (`scripts/bootstrap_node_release.py`) | M1 |
| Pinned public keys | `lastdb_node/environments.json` | M1 |
| Developer ID signing, notarization, Linux archives | `scripts/release/nightly_package.py`, release workflows | M2 |
| `lastdbd --accepted-home-format`, `POST /api/system/prepare-stop` | `lastdb_node` daemon | M3 |
| List writer, rollout and withdraw workflows | `scripts/release/node_list.py`, `.github/workflows/node-*.yml` | M4 |
| `lastdb upgrade`, `lastdb service` | `lastdb_node/src/bin/lastdb/` | M5 |
| Updater proof in release CI (built: `scripts/release/node_updater_proof.py`, job `proof` of `node-list-publish.yml`; DEV is listed first, the proof reads that DEV list, PROD is listed only after it passes) | release workflows | M6 |

## 2. Where the lists live

```
<node_index>/<channel>/<target>.json
<node_index>/<channel>/<target>.json.sig
```

- `node_index` per environment, beside `registry_index` in `environments.json`:
  - prod: the app registry bucket's base URL for prod + `/v1/node` (the host of `registry_index` in `lastdb_node/environments.json`)
  - dev: the app registry bucket's base URL for dev + `/v1/node` (the host of `registry_index` in `lastdb_node/environments.json`)
- `channel`: `stable` or `nightly`.
- `target`: `aarch64-apple-darwin`, `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`.
- Objects are public-read, `Content-Type: application/json`, `Cache-Control: max-age=60`.
- A reader uses HTTPS only, follows no redirect, and reads at most 256 KiB per object.

## 3. The list

```json
{
  "list_version": 1,
  "kind": "lastdb-node",
  "channel": "stable",
  "target": "aarch64-apple-darwin",
  "generated_at": "2026-12-01T09:00:00Z",
  "seq": 42,
  "releases": [
    {
      "version": "0.23.29",
      "source_git_oid": "<40 lowercase hex>",
      "published_at": "2026-12-01T08:55:12Z",
      "archive_url": "https://github.com/EdgeVector/homebrew-lastdb/releases/download/v0.23.29/lastdb-aarch64-apple-darwin.tar.gz",
      "archive_sha256": "<64 lowercase hex>",
      "archive_bytes": 31457280,
      "binaries": [
        {"name": "lastdb", "sha256": "<64 lowercase hex>", "size": 40000000},
        {"name": "lastdbd", "sha256": "<64 lowercase hex>", "size": 52000000}
      ],
      "signing": "developer-id",
      "rollout": {"percent": 10, "started_at": "2026-12-01T09:00:00Z"},
      "status": "active"
    }
  ]
}
```

Rules. A reader refuses the whole list if any rule fails.
- `list_version` is `1`; `kind` is `lastdb-node`; `channel` and `target` equal the path.
- `seq` is a positive integer. A reader refuses a `seq` lower than the highest it verified
  for that channel and target. An equal `seq` must have identical bytes.
- `releases` has 1 to 20 entries, ordered by `published_at`, newest first, with no
  duplicate `version`.
- `version`: stable `^\d+\.\d+\.\d+$`; nightly `^\d+\.\d+\.\d+-nightly\.\d{8}\.g[0-9a-f]{9}$`,
  whose last nine characters equal the start of `source_git_oid` (the rule
  `nightly_package.py` already applies).
- `archive_url` starts with `https://github.com/EdgeVector/lastdb/releases/download/` or
  `https://github.com/EdgeVector/homebrew-lastdb/releases/download/` and ends with
  `/lastdb-<target>.tar.gz`. A download may follow GitHub's redirect to its asset host;
  the hash is the check.
- `binaries` names exactly `lastdb` and `lastdbd`. The archive holds exactly these two
  regular files at its top level (the layout `nightly_package.py` writes today).
- `signing`: `developer-id` on `*-apple-darwin` (Developer ID signed and notarized);
  `none` on Linux.
- `rollout.percent` is an integer from 0 to 100.
- `status` is `active` or `withdrawn`. A `withdrawn` entry also carries
  `withdrawn_reason` (one line, at most 200 characters) and `withdrawn_at`.
- Unknown fields are refused, so a field is never silently ignored.

## 4. The signature

The `.sig` object is the app registry's envelope, unchanged
(`app_identity_crypto::IndexSignature`):

```json
{"alg": "ed25519", "key_id": "<sha256 hex of the 32 public key bytes>",
 "payload_sha256": "<sha256 hex of the exact .json bytes>", "sig": "<base64, 64 bytes>"}
```

- `sig` is an Ed25519 signature over the ASCII bytes of `payload_sha256`, as for the
  registry lists. KMS produces it with `kms:Sign`, `SigningAlgorithm` `ED25519_SHA_512`,
  `MessageType` `RAW`.
- The reader verifies the exact downloaded bytes before it parses them.
- **Pinned keys:** `environments.json` gains `node_release_keys`, a list of one or two
  base64 public keys per environment. A list verifies if its `key_id` names one of them.
  Rotation: ship the new key as the second entry in a release, switch the signer, then
  drop the old key in a later release. `lastdb upgrade` accepts no key override and no
  environment override; DEV keys are used only when the binary's environment is DEV.
- **Write order:** the writer uploads `.json`, then `.sig`. A reader that sees a mismatch
  retries once after 2 seconds; a second mismatch means "no update this check", not an
  error.

## 5. Signing role

- One non-exportable KMS key per environment: key spec `ECC_NIST_EDWARDS25519`
  (Ed25519), usage `SIGN_VERIFY`, alias `alias/lastdb-node-release-<env>`.
- One GitHub OIDC role per environment, trusted only for `repo:EdgeVector/lastdb` with
  `environment:node-release` (prod) or `environment:node-release-dev` (dev). It may
  `kms:Sign` that key with `ED25519_SHA_512` and `RAW`, `kms:GetPublicKey` it, and
  `s3:GetObject` / `s3:PutObject` on `v1/node/*` in its own bucket. Nothing else.
- The `node-release` GitHub environment accepts only protected `main` and holds the role
  ARN and the Apple notarization credentials. No private signing key is stored in GitHub.

## 6. Writers

All writers run `scripts/release/node_list.py`, which:
1. reads and verifies the current list (a missing list starts at `seq` 0);
2. applies one change;
3. writes canonical JSON (the JCS form `app_identity_crypto::canonicalize` produces), with
   `seq` + 1 and a new `generated_at`;
4. asks KMS to sign; uploads `.json` then `.sig`;
5. downloads both again and verifies them with the pinned key before it exits 0.

Changes:
- **publish** (release workflows, after every gate, the attestation check and the asset
  upload): add the entry at the front with `rollout.percent` 100 for nightly and 10 for
  stable; drop entries past the 20th.
- **advance** (`node-rollout.yml`, hourly): for each active stable entry below 100,
  set 50 when `started_at` is at least 24 hours old and 100 when at least 48 hours old.
  `started_at` stays the publish time. A withdrawn entry is never changed.
- **withdraw** (`node-withdraw.yml`, dispatch only, inputs `channel`, `version`,
  `reason`): set `status`, `withdrawn_reason`, `withdrawn_at`. Withdrawal is never undone;
  a fixed build is a new version.

Two writers never run at once: every workflow that writes uses the GitHub concurrency group
`node-list-<env>` without cancellation.

## 7. Which release an install takes

- **Install id:** `~/.lastdb/upgrade/install-id`, 16 random bytes as 32 lowercase hex,
  created on first need with mode 0600. It is never sent anywhere.
- **Share:** an install may take an entry when
  `u32_be(sha256(install_id_hex + ":" + version)[0..4]) % 100 < rollout.percent`.
- **Installed release record:** every pair installed from a list keeps its list entry in
  `programs/<version>/release.json`. A pair installed with `--pair` has none.
- **Newer:** an entry is newer than the running pair when its `published_at` is later than
  the `published_at` in the running pair's `release.json`. Version strings are never
  compared: nightly and stable numbers do not sort against each other.
- **Automatic runs** take the newest `active` entry that is newer, in the share, and not
  in `upgrade/rejected`. A running pair with no `release.json` is never replaced
  automatically; `lastdb status` says so.
- **`lastdb upgrade`** (by hand) does the same but ignores the share. `--version` names one
  active entry and ignores the share and `newer`. `--channel` changes the configured
  channel and then behaves like `--version` with that channel's newest active entry.
- **Withdrawn running build:** if the running pair's version is `withdrawn` in a verified
  list, an `apply` install rolls back to `previous` at the next hourly check, outside its
  window. `previous` must not itself be withdrawn; if it is, the install reports and waits.

## 8. Host state

```
~/.lastdb/programs/<version>/lastdb               0555
~/.lastdb/programs/<version>/lastdbd              0555
~/.lastdb/programs/<version>/release.json         0444, the list entry, absent for --pair
~/.lastdb/programs/.staging-<version>-<pid>/      removed on any failure
~/.lastdb/current  -> programs/<version>
~/.lastdb/previous -> programs/<version>
~/.lastdb/upgrade/lock                            flock, non-blocking
~/.lastdb/upgrade/install-id                      0600
~/.lastdb/upgrade/config.toml                     0600
~/.lastdb/upgrade/rejected                        one version per line
~/.lastdb/upgrade/history.jsonl                   one JSON object per attempt
~/.lastdb/upgrade/list-cache/<channel>-<target>.json and .sig   last verified pair
```

`config.toml`:

```toml
channel = "stable"            # stable | nightly
auto = "apply"                # off | check | apply
window = "02:00-05:00"        # local time
service_label = "com.edgevector.lastdbd"
soak_secs = 300
stop_timeout_secs = 600
rss_limit_mb = 16384
```

`history.jsonl` fields: `started_at`, `finished_at`, `from`, `to`, `trigger`
(`manual` | `auto` | `withdrawn`), `result` (`upgraded` | `refused` | `rolled-back` |
`needs-human`), `step`, `detail`. No record content, keys of user data, or secrets.

A program folder is removed only when it is neither `current` nor `previous`, only after
a successful upgrade, and never while `previous` is unset.

An automatic run (`auto` or `withdrawn`) that fails after the drain adds its target to
`rejected`, so it is tried once per version. A withdrawn rollback whose `previous` is in
`rejected` only reports. A stop is bounded by the longer of `stop_timeout_secs` and the
job's own stop window (`ExitTimeOut` or `TimeoutStopSec`) plus 120 s; when the old process
outlives it, nothing is killed and the old job is started again (or the start is queued on
systemd); exit 4 only when it cannot be.

## 9. Daemon interfaces

### 9.1 `lastdbd --accepted-home-format`

Prints the exact marker text the build opens (today
`format=15\nhome=disk-run\ncas=disk-cas\n`) to stdout and exits 0. It opens no home, takes no
lock and reads no environment. The updater compares the output byte for byte with
`<home>/data/laststore-pack-node-v1`, the node marker that
`fold_db/crates/core/src/storage/disk_pack/run_node.rs` (`read_stopped_node_profile`)
checks. A build without the flag is refused.

### 9.2 `POST /api/system/prepare-stop`

Owner socket only. Request: `{"upload": true, "deadline_ms": 120000, "hold_ms": 300000}`.

The daemon:
1. answers new writes with HTTP 503, `{"error": "upgrading", "retry_after_ms": 5000}`;
2. if `upload` and cloud sync is connected, runs only the send step of the sync turn until
   `acked == head` or `deadline_ms`;
3. answers `200 {"head": n, "acked": n, "cloud": "current" | "behind" | "off", "detail": "..."}`.

Writes stay refused until the process exits, `POST /api/system/prepare-stop/cancel`
arrives, or `hold_ms` passes with no exit; then the daemon accepts writes again. A second
`prepare-stop` while one holds answers with the current state. Held writes stay in the
spool; nothing is dropped.

## 10. `lastdb upgrade`

Verbs: (none), `--check`, `--version <v>`, `--channel <c>`, `--pair <dir>`, `--rollback`,
`--status`, `--auto`, `--clear-rejected <v>`, `--accept-backup-behind`, `--json`.

Exit codes: `0` upgraded or already current; `2` refused before any change; `3` rolled
back; `4` needs a human; `5` busy (another holder of the lock). Each run prints one
sentence that says which.

The switch steps (resolve, download and verify, preflight, drain, strict stop with the
flush receipt, switch, verify, soak, keep or roll back) are as in the design, section 8.
Their order is part of this contract: no step that changes the running service comes
before the drain, and nothing in a download runs before its hashes and signature pass.

## 11. Service

- **macOS:** `~/Library/LaunchAgents/<service_label>.plist`, `ProgramArguments` =
  `[<home>/current/lastdbd]`, `EnvironmentVariables.LASTDB_HOME` = the home,
  `KeepAlive` true, `RunAtLoad` true, logs in `<home>/logs/`. Default label
  `com.edgevector.lastdbd`.
- **Linux:** `~/.config/systemd/user/lastdbd.service`, `ExecStart=<home>/current/lastdbd`,
  `Environment=LASTDB_HOME=<home>`, `Restart=always`.
- **Update checks:** the existing app-registry job (`com.edgevector.lastdb-app-registry`)
  also runs `lastdb upgrade --auto` every hour. `--auto` only fetches and records outside
  the window, except for a withdrawn running build (section 7).
- **`lastdb service adopt --label <l>`** rewrites an existing job to run
  `<home>/current/lastdbd` once, with drain, strict stop, start and verify. It never moves
  a file inside the home. A job that already runs `<home>/current/lastdbd` but that this
  home's config does not name (a hand-made job whose `current` points outside
  `programs/`) is adopted the same way. Only a home whose config names the label refuses.
- The CLI on PATH (`~/.local/bin/lastdb`) is a link to `<home>/current/lastdb`.

## 12. Release artifacts

- Archives keep today's names (`lastdb-<target>.tar.gz`, `.manifest.json`); the manifest
  `signing` field becomes `developer-id` on macOS.
- macOS binaries: `codesign --force --options runtime --timestamp --sign "Developer ID
  Application: ..."`, then `xcrun notarytool submit --wait` on a zip of the pair. Both
  channels.
- Linux: `x86_64-unknown-linux-gnu` (built today) and `aarch64-unknown-linux-gnu` (new),
  published on both channels.
- Windows is out of scope.
