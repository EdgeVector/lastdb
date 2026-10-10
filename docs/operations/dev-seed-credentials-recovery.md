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

There is no `data` directory or active cloud configuration.
The product verifies final names, descriptors, owner, mode, single links, and bytes before success.
Its receipt contains no seed, API key, phrase, or response body.
The receipt reports the fresh device ID and the account equality check.

The offline `prepare-recovery-home` command stays separate.
It reads an existing local identity and config and performs no registration.
