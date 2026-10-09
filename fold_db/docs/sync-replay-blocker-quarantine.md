# Clearing a pinned cloud replay cursor

When cloud download stops at one log sequence, uploads for that target also
stop (same cycle). `lastdb status` (and `GET /api/status`) expose the pin as
`replay_blocker` with `code`, `target`, `seq`, `reason`, and `action`.

## Codes and operator action

| Code | Meaning | Clear path |
|------|---------|------------|
| `cloud_replay_corrupt_entry` | Object unseals badly / is unreadable | **Delete** that cloud log object + local tombstone. Safe for every device: nobody can read it. |
| `cloud_replay_apply_failed` | Object decrypts but this build cannot **apply** it | **Skip local only.** Advance this device's download cursor and write a local tombstone. **Do not** delete the cloud object — peers or a later build may still apply it. |
| `cloud_sync_key_mismatch` | Wrong account/sync key for the prefix | Restore the correct key/mnemonic. Quarantine refuses this code. |

## CLI (preferred)

```bash
# Read the pin
lastdb status   # look for replay_blocker.target / .seq / .code

# Clear it (exact target + seq only — no wildcards)
lastdb cloud quarantine-replay --target personal --seq 1786916561002908000
# alias:
# lastdb cloud quarantine-replay-blocker --target personal --seq …
```

Mode is chosen from the live blocker's code:

- corrupt → cloud delete + tombstone; next sync advances past 404+tombstone
- apply_failed → local skip + cursor advance; next cycle can upload again

## API

```http
POST /api/sync/quarantine-replay-blocker
{ "target": "personal", "seq": 1786916561002908000 }
```

Owner Unix socket only. Response includes `mode` (`delete` | `skip_local`) and
`deleted_log_objects` (1 or 0).

## Safety

- Always pass the exact `target` and `seq` from `replay_blocker`.
- Prefer **skip_local** for apply failures: destroying a still-readable cloud
  object is irreversible for every device on the prefix.
- After clear, wait for the next sync cycle (or force sync) so upload resumes.
