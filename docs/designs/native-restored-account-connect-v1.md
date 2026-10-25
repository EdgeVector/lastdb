# Native restored account reconnect

A native restore contains the account identity and database bytes. It contains no cloud credentials or automatic backup consent.
The fresh `cloud backup-v2 connect --dev` command requires a private recovery phrase file and an otherwise empty enrollment home.
It refuses a restored home with a database.

This change adds an explicit restored-home mode:

```text
lastdb --data-dir /absolute/restored-home cloud backup-v2 connect --dev --restored --json
```

The V2 release flag LASTDB_BYTE_BACKUP_V2_ENABLED remains required. The existing fresh command keeps its checks.
The new mode accepts no invitation and no caller endpoint. It uses the compiled DEV registration endpoint.
It registers the existing account identity with one durable device UUID. It creates no new account identity.

## Local admission

The command requires an explicit absolute path without symlink components.
It refuses the primary, the legacy primary, and the primary archive.
It requires a private owner home and a completed native restore receipt for that exact destination.
The receipt must contain the source commit identity and complete file coverage.
The account seed comes from the existing private `identity.key`; no recovery phrase file is required.

The command refuses a live daemon or an open owner socket.
It holds the same stable native node marker lock that the daemon holds.
It also holds the identity file lock and the enrollment lock through the credential commit.
The command checks the held directory and identity inode before each protected change.
It refuses an identity replacement or a change to CURRENT.

The core validator opens no writer and creates no native file or directory.
It verifies the existing format fence and native root through the supported native reader.
It reads one exact logical-format record with fixed byte, block, page, and key budgets.
The supplied account key must open that encrypted record.
The CLI does not copy native key routing or interpret pack files.

## Credential result and retries

The command saves and flushes `.native_backup_device_id` before registration.
An unknown registration result retains that UUID and identity. An explicit retry uses both again.
The registration response must name the account derived from the held identity.
The command checks the identity and device UUID again before it saves credentials.

Credentials use the existing protected pending file and atomic rename to `cloud_sync.json.paused`.
An existing saved result is idempotent. Its endpoint must match the compiled DEV endpoint.
An authenticated V2 context read must confirm that saved credentials authorize the held account.
The command never replaces conflicting credentials.

Legacy Cloud Sync stays off. Automatic backup consent must already be absent or disabled.
The command does not modify automatic consent, adopt a generation, activate Ready, change Hold, or enroll a recipient address.
It changes no database record, pack file, file pointer, file key, or restore receipt.
The V1 automatic backup command is retired. Explicit V2 Start and Resume retain their cloud admission checks.

## Validation boundary

Source and format checks support review. CI supplies type, lint, build, and benchmark validation.
The DEV check must use an owned restored home with its daemon stopped.
It must confirm the same account, unchanged database bytes, paused credentials, and automatic backup off.
It must also confirm one idempotent retry without another registration.
No primary operation or cloud deployment belongs to this change.
