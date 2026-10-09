# Legacy tombstone drain

`lastdb db drain-legacy-tombstones` converts rows written by the retired
tombstone model into reachability-guarded hard erasures. It walks storage-form
`mk:` keys directly, so it works on the normal BlindV1/OpeV1 layout without
trying to reverse opaque keys. Each daemon request is bounded and resumable;
destructive work is split into the same 64-slot per-schema barrier holds used
by purge, and Search receives a tombstone for every drained slot.

The command is dry-run by default:

```bash
lastdb db drain-legacy-tombstones --schema '<descriptive-or-stored-schema>'
lastdb db drain-legacy-tombstones --schema '<descriptive-or-stored-schema>' --execute
```

Before using `--execute` on a real home, make a copy-on-write clone or durable
copy of the entire node home, start a throwaway `lastdbd` against that clone and
its own Unix socket, and point the CLI at that socket. Never start the probe on
`~/.lastdb`, never reuse the primary socket, and never restart the primary for
this operation. On the clone, require all of the following before scheduling a
live supervised drain:

1. Dry-run reports the expected tombstone population and zero unowned slots.
2. `--execute` completes, and a second dry-run reports zero tombstones for the
   selected schema.
3. Keyed reads and Search queries miss the drained fixture/known-dead keys,
   while adjacent live records remain readable.
4. `lastdb db delete-ledger` contains the committed purge batches.

`--max-keys` lowers the number of `mk:` rows decided per daemon call when a
large or slow home approaches the owner-socket deadline. The CLI follows the
returned storage cursor until the selected keyspace is exhausted; rerunning is
safe after interruption because already-erased rows are absent from the next
walk. This is a migration tool, not a retention timer, grace period, or trash
surface.
