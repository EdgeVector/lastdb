# Delete replay cutoff

A home keeps one global replay cutoff in the entry `layout:delete-replay-cutoff`.
LastDB refuses a replayed or imported mutation when its author time is missing or is at or before the cutoff.
A home without the cutoff entry has cutoff 0.

The owner raises the cutoff on a stopped home:

```sh
lastdb --data-dir /absolute/generated-or-owned/home db recount --delete-cutoff-unix-secs SECONDS --json
lastdb --data-dir /absolute/generated-or-owned/home db recount --delete-cutoff-unix-secs SECONDS --execute --json
```

The command first reports the change. The `--execute` option writes it.
The command uses the existing stopped-home lock and primary refusal.
The cutoff applies to every scope of the home, so it does not need `--all-scopes`.
The other recount steps use the scopes of the command as usual.

Choose the cutoff explicitly. No timer advances it.
A cutoff must be positive, cannot decrease, and cannot advance beyond the current time.
The command writes the one cutoff entry and flushes it. It removes no record.

## Replay rule

After the cutoff rises, a stale device must obtain a current snapshot before it resumes replay.
The rule applies before replay effects and before the native write scope.
A replay refusal does not advance the cloud replay cursor.

The serving process reads and validates the format marker and the cutoff once at startup.
Its clones share that fixed value. Normal writes need no extra disk read or lock for this check.
Offline recount uses a separate instance that reads disk and cannot use the serving cache.
A snapshot restore opens a new process and validates its own restored cutoff.
A home without the key format entry (`layout:keys`) does not start. A malformed cutoff entry prevents startup.

## TTL replay

A TTL Delete erases only the values written before its original age cutoff.
The mutation envelope carries that cutoff, and the author signature binds it to the Delete.
A replay with an unsigned TTL cutoff is refused.
A mutation without a TTL cutoff keeps its existing content hash.

## Order of the steps

1. The owner accepts that older offline work requires a resync.
2. Stop the target home.
3. Run the command without `--execute` and read the report.
4. Run the command with `--execute`.
5. Start the home.

A stale peer must obtain a current snapshot of the home before it resumes replay.
Keep the cutoff with that snapshot. An old backup is not a current snapshot.

## Limits

The cutoff changes only the local home. It does not erase historical backups or cloud files.
A deliberate restore from an older backup can restore old data and its old cutoff.
