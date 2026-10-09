# LastDB app registry — the static signed index

The index is the anonymous read surface of the app registry. A fresh
install reads one static file and one signature. It needs no account and no
live service. Decision: brain
`decision-2026-09-19-app-registry-is-the-release-unit`.

## Files

One file per channel, next to a detached signature:

```
registry/stable.json        registry/stable.json.sig
registry/next.json          registry/next.json.sig
registry/index-signing.pub  (the verifying key, for a cross-check)
```

Public location: the Homebrew tap repo, served raw by GitHub
(`https://raw.githubusercontent.com/EdgeVector/homebrew-lastdb/main/registry`).
`lastdb` reads that location unless `--index` or `$LASTDB_REGISTRY_INDEX`
names another URL base or a directory.

## Index grammar (`index_version` 1)

```json
{
  "index_version": 1,
  "channel": "stable",
  "generated_at": "2026-09-20T10:17:00Z",
  "apps": [
    {
      "app_id": "brain",
      "source": "https://github.com/EdgeVector/brain.git",
      "description": "…",
      "compat": [
        {
          "app_version": "0.9.1",
          "sha": "1f2d2f46a7eb65ad6898bd2b75ec9eb2f40e6aeb",
          "lastdb_version": "0.23.3-2068-g51fca83b2",
          "lastdb_api_version": 1,
          "proved_at": "2026-09-20T10:17:00Z",
          "proof_run": "llms-txt-install-smoke/20260920T1017Z"
        }
      ]
    }
  ]
}
```

A **compat row** is a proved pair: this app commit was proved with this
node build by this proof run. `lastdb_version` is the node build string
(`lastdbd --version`, `GET /api/version` `build`). The same string names the
canary binary and the brew bottle built from the same commit.

There is no minimum-version rule. Install picks the newest row whose
`lastdb_version` equals the running node's build string. No row means no
install, with the remedy in the error (`brew upgrade lastdb`, or wait for the
next proved set).

## Signature

`<channel>.json.sig`:

```json
{
  "alg": "ed25519",
  "key_id": "<sha256 hex of the 32-byte public key>",
  "payload_sha256": "<sha256 hex of the exact index bytes>",
  "sig": "<base64 Ed25519 signature over the ASCII payload_sha256>"
}
```

The reader hashes the bytes it downloaded, compares, then verifies the
signature. Any mismatch refuses the whole file. The verifying key is pinned in
the `lastdb` binary (`lastdb app index trust-key` prints it). `--trust-key`
or `$LASTDB_REGISTRY_TRUST_KEY` overrides it for a test index; the override is
printed on stderr.

## Commands

```bash
lastdb app list [--channel stable|next] [--index <url-or-dir>]
lastdb app info <app> [--channel …]
lastdb app resolve <app> [--channel …] [--lastdb-version <build>] [--json]
lastdb app install <app>… [--channel …] [--dir <path>] [--force]
lastdb app upgrade <app>… [--channel …]
lastdb app index sign --index <file> [--key-file <key>]
lastdb app index verify --index <file> [--trust-key <b64|file>]
lastdb app index trust-key
```

`install` clones `source`, checks out `sha`, verifies `HEAD`, and writes
`lastdb-app-install.json` with the row and its `proof_run`. `upgrade`
reinstalls only when the resolved commit differs from the installed one.

The `--env` / `--schema-url` flags keep the legacy live-service path for
`list`, `info`, `install`, and `upgrade` (one app).

## Who writes the index

- The nightly canary lane writes proved rows to `next` after the isolated
  llms-txt smoke passes on the candidate set (last-stack
  `last-stack-registry-index`).
- One human command promotes a proved node build to `stable`: brew gets the
  node, the index gets the rows. Stable stays a human action until five clean
  manual promotes.

Hermetic proof: `lastdb_node/scripts/app-registry-index-e2e.sh`.
