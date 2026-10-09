# Restore a normal cloud backup without the source disk

The recovery home needs the account identity key and the cloud configuration. It does not need the source LastStore files.

Use a fresh target home:

```sh
lastdb --data-dir <recovery-home> restore --remote-latest --into <fresh-target-home>
```

The restore reads an encrypted account recovery descriptor. It checks the descriptor against the exact `backup/latest` manifest and the source database identity.

The restore checks all backup files and then reads the cloud mutation history. The target stays Cloud Sync Off.

The target gets `.normal_latest_restore_ready` only after the restore succeeds. Its JSON states the database hash, manifest hash, counter, and restore mode.

CAUTION: A backup without a recovery descriptor cannot use this source-free command. Use a source home with its LastStore files for that old backup.

CAUTION: Discovery reads at most 10,000 account recovery descriptors. It refuses a larger or incomplete list before it creates the target.

Use `--db-hash <hash>` when the account has more than one database. Use `--manifest-sha256 <hash>` to require one exact current manifest.
