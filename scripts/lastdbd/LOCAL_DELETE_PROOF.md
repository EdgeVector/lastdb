# Fast local Delete proof

Use this proof before the full cloud recovery proof.
The command creates a new database, with no production data or cloud account.
The schema catalog is a bundled fixture on an owned loopback port.
The installed daemon performs every mutation, query, durable write, and cold start.

Run from a Fold DEV worktree:

```sh
python3 scripts/lastdbd/prove_local_deletes.py \
  --lastdb "$HOME/.lastdb/current/lastdb" \
  --lastdbd "$HOME/.lastdb/current/lastdbd" \
  --output-dir /tmp/lastdb-delete-proof-unique
```

Use a new, short output path for each run.
The Unix socket path must contain fewer than 104 bytes.
The command rejects an existing output directory.

The result uses `lastdb.local-delete-proof.v1` in `result.json`.
A successful result contains these checks:

- The CLI and daemon versions match. Their file hashes remain unchanged.
- Four synthetic records exist before Delete: two Hash records and two HashRange records.
- One record from each schema receives a durable Delete receipt.
- Both deleted keys remain absent with `include_tombstones` false and true.
- Both retained controls preserve their exact keys and fields in both modes.
- Every query returns a complete page, with zero unresolved rows and zero tombstone rows.
- Two distinct cold daemon processes repeat all checks after graceful exits.
- All three owned daemon processes exit. The exact owned database directory is removed.

The result reports each phase duration, process identity, receipt, and resource sample.
The default limits are 180 seconds, 2 GiB per child, and a 2 GiB free disk reserve.
The command suppresses raw daemon output and the recovery phrase.
The command never reads production credentials or changes a shared daemon.

This proof checks local logical erasure and restart durability.
It does not certify atom reclamation, disk byte return, cloud publication, or cloud recovery.
Use the small cloud proof next. Use the full production restore for the final recovery check.

## Offline supervisor tests

```sh
python3 -m unittest discover \
  -s scripts/lastdbd/tests -p test_prove_local_deletes.py
```

The process fixture tests supervisor behavior and failure reports.
They do not replace the proof against the real installed daemon.
