# Explicit delete-barrier cleanup

Delete keeps a durable order record for each removed molecule key. This record blocks older writes after a restart or cloud replay.

The owner can remove old delete records from a stopped home:

```sh
lastdb --data-dir /absolute/generated-or-owned/home db recount --all-scopes --delete-cutoff-unix-secs SECONDS --json
lastdb --data-dir /absolute/generated-or-owned/home db recount --all-scopes --delete-cutoff-unix-secs SECONDS --execute --json
```

The command first reports the proposed changes. The `--execute` option writes them. The command uses the existing stopped-home lock and primary refusal.

Choose the cutoff explicitly. No timer advances it. A cutoff must be positive, cannot decrease, and cannot advance beyond the current time. Include every scope. The database names the known scopes; use `--scope` for any additional scope.

## Replay rule

Before the first delete, the command writes and flushes two records in one batch: the minimum replay time and its required format marker. A barrier with an author time at or before the cutoff can then go. An interrupted command can leave extra barriers. The durable replay rule remains in force.

After this change, LastDB refuses a replayed mutation with a missing author time or an author time at or before the cutoff. A stale device must obtain a current snapshot before it resumes replay. LastDB also refuses old raw key writes because they carry no author time.

The rule applies before replay effects and again inside the native write scope. A replay refusal does not advance the cloud replay cursor.

The opt-in format marker is `5;delete-replay-cutoff=v1`. Older serving programs refuse this marker. An ordinary home keeps format `5`. A floor without the matching marker, or a marker without its floor, prevents startup.

## TTL replay

A TTL Delete can remove tips only before its original age cutoff. The mutation envelope now carries that cutoff. The author signature binds it to the Delete. A replay with an unsigned TTL cutoff is refused.

A mutation without a TTL cutoff keeps its existing content hash. Old programs that discard a signed TTL cutoff cannot validate the changed content.

## Upgrade order

Pause TTL on each peer with `LASTDB_TTL_SWEEP=0` before the first peer runs this version. Upgrade every peer that can send or receive this database. Keep old offline peers paused until they also upgrade.

After all peers accept the signed TTL cutoff, enable TTL again. The new signed envelope fails verification on an older peer that drops the cutoff. That failure stops replay. It does not permit a broader Delete.

Choose the explicit delete cutoff only after the owner accepts that older offline work requires resync. Stop the target home before the cleanup command. After cleanup, start a program that supports the fenced format. Do not downgrade that home to an older program.

A stale peer must obtain a current snapshot from the fenced state before it resumes replay. Keep the replay floor with that snapshot. An old backup is not a current snapshot.

## Limits

The cleanup removes local delete order records. It does not erase historical backups or cloud files. A deliberate restore from an older backup can restore old data and its old replay rule.

This change needs Factory Manager review. CI operational results must cover restart, replay refusal, new writes, and signed TTL replay before merge. The retired tests remain retired.

