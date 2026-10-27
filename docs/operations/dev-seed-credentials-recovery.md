# Recover DEV credentials from a protected identity seed

`lastdb cloud recover-dev-credentials-home` recovers credentials for an existing DEV account.
The product reads one base64 identity seed from stdin and registers one fresh device.
It creates a protected credentials-only home and keeps Cloud Sync Off.
It does not open a local store, read local data, start a daemon, or publish data.

Use this command only with a known disposable DEV account and its exact recorded account hash.
It does not accept PROD, an endpoint override, an invite, a device ID, or `--data-dir`.
The derived account hash must match `--expected-user-hash` before target creation or a network call.
The registration response must identify the same account before the credential commit.

CAUTION: The command registers a remote device and mints an API key.
Local file rollback cannot undo a remote registration.
A failed request can report `registration_may_have_committed: true`.
Do not retry that request automatically.

Existing-account signed registration needs no invite.
The accepted Auth source rotates only the same device's keys.
The command does not verify the deployed Auth program or its key policy.
The operator must check that policy or authorize the possible effect within the disposable account.
Do not use a primary or shared identity for this operation.

Create an empty owner-only temporary parent before the call.
On macOS, its path must be `/private/tmp/lastdb-dev-recovery.*`.
On Linux, its path must be `/tmp/lastdb-dev-recovery.*`.
The command ignores `HOME` and `TMPDIR` for this boundary.
The parent must have mode `0700`. The destination must not exist.
No path component can be a symlink.

Pass the LastSecrets value directly to the product:

```bash
set -o pipefail
lastsecrets get exact-disposable-dev-identity-seed-slug |
  lastdb cloud recover-dev-credentials-home \
    --into /private/tmp/lastdb-dev-recovery.EXACT/recovery \
    --expected-user-hash EXACT_RECORDED_USER_HASH \
    --identity-seed-stdin --json
```

The example path and selector are placeholders. Use the exact owned parent and account receipt.
Never print the seed, derive a phrase in a helper, or copy the seed through an agent file.
The command uses the compiled DEV URL and a fresh random device ID.
The registration has a 30-second deadline and a 64-KiB response limit.
It refuses redirects and does not automatically repeat registration.

The result has mode `0700` and exactly three files with mode `0600`:

- `identity.key`: the product-held seed.
- `cloud_sync.json.paused`: the minted API key and compiled DEV URL.
- `.cloud_resume_required`: the Cloud Sync Off marker.

There is no `data` directory.
The product verifies final names, descriptors, owner, mode, single links, and bytes before success.
Its receipt contains no seed, API key, phrase, or response body.
The receipt reports the fresh device ID and the account equality check.

## Store the new key through LastSecrets

Add `--api-key-lastsecrets-slug` when a later product command needs the new API key.
The slug must be `lastdb-dev-recovery-api-key-<expected-user-hash>-<UUIDv4>`.
Use one fresh UUID and retain only its `lastsecrets://` locator.
LastSecrets put is an upsert. The command does not claim exclusive record creation.
Check the exact proposed locator metadata before the operation; do not infer absence from a listing.

The product sends the returned key directly to LastSecrets stdin after the registration account matches.
It uses no shell, secret argument, secret environment value, or raw temporary file.
The product then reads that exact LastSecrets record internally and compares the key in memory.
It commits the protected recovery home only after the exact key comparison succeeds.
The agent must not read or extract the key from the paused cloud config.

The optional route requires the OS account's installed `~/.local/bin/lastsecrets`.
It also requires the private `~/.lastsecrets/config.json` file.
The config must retain its local owner socket and both pinned LastSecrets schema names.
The product accepts no tool, config, socket, HOME, PATH, or endpoint override.
It binds the executable and config descriptors and records their SHA-256 metadata.

The child reads the retained config descriptor. It cannot follow a replaced config path.
The product does not rewrite or initialize the LastSecrets configuration.

It clears the child environment and fixes the child directory to `/`.
It disables Sentry output, Bun crash uploads, and the runtime transpiler cache.
It refuses the fixed automatic config paths before it reads the seed or registers a device.
Bun can load config files in a standalone program.
Those files can select a different route or preload code.

See the [Bun executable docs](https://bun.com/docs/bundler/executables#automatic-config-loading).
The [Bun environment docs](https://bun.sh/docs/runtime/environment-variables) name these config paths and controls.

CAUTION: The LastSecrets child writes one secret and its normal metadata in the local LastSecrets store.
Release any node-write hold before this operation.
The recovery command still reads no local database content and publishes no database data.

The child has a 60-second deadline.
On failure, the product kills and reaps only that child.
The reap has a five-second deadline.
Child errors and output never appear in the receipt.
The private readback has a 4097-byte cap and never reaches stdout.

The receipt reports the locator, code and config hashes, stored-key result, and exact-key comparison result.
It reports `exclusive_create: false` and separate possible-write and local-home outcomes.
Failures after the LastSecrets preflight keep its locator and hashes.

CAUTION: A failed LastSecrets result can leave a stored key after the remote registration.
The product rolls back only the uncommitted local home.
It does not repeat registration, delete the secret, or revoke a remote key.
If local commit fails after LastSecrets succeeds, the receipt retains the confirmed secret-store result.
Keep the locator and the failure receipt for a scoped follow-up.

The offline `prepare-recovery-home` command stays separate.
It reads an existing local identity and config and performs no registration.
