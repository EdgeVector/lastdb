# Format 12 live-record copy

Format 12 and format 13 use their own daemon versions. Use a fresh format 13
home and transfer logical records through the owner socket. The format 13
writer allocates new record revisions and file versions as it accepts them.
The format 12 home and its Backup V2 recovery points remain the source for
historical recovery with the matching program.

The record-home workflow rehearses this sequence with generated data. It
builds the accepted format 12 source at `5373c5c514e612104204baca69cbd3b82995c5f1`
and the candidate on the same runner. Both daemons report their exact source
and home format before the operation starts.

1. Establish a stable source. Stop its writers and TTL activity for the copy
   window. Enumerate complete live keys for the selected schemas using the
   bounded owner list. Refuse an incomplete page, an ambiguous schema, or a
   field or key declaration that differs at the destination.
2. Install each exact typed definition in a fresh target. For a generated
   home, the rehearsal uses the isolated system seed route. An app-owned
   migration needs its authorized declaration procedure and runtime identity.
3. Read whole records by their listed keys. Create each record with all its
   alternate slots and metadata. Verify each file against the source marker
   and include its bytes in the same target request as the complete record.
4. After a durable restart, compare complete key membership, fields,
   alternate groups, metadata, and file digests. Preserve a time-ranged
   schema's original range coordinates and its unshared operator TTL policy.
   The rehearsal enables the target sweep after comparison and observes the
   old range expire while the fresh range stays live.

## Historical state and age

The supported live-record copy refuses a Hash or Single schema with active
write-time TTL. Format 12's complete record query does not expose the author
time. The field-history API can report the current time for a present field,
which leaves empty records without an export. The target's public mutation
API allocates the write time. A complete write-time TTL transfer requires a
separate age export and bootstrap admission contract.

The format 12 delete ledger contains key fingerprints and audit times.
Erased record values lack their original API coordinates. The copy preserves
the resulting absence and rejects an old revision condition. It does not
translate legacy tombstones, field history, delete audit, or replay positions.
Shared stream enrollment requires a new checkpoint and its own admission
boundary. Keep the old recovery history for any required legacy replay.

The rehearsal qualifies the live-record operation in generated homes.
Production conversion, app qualification and backup cutover have their own
approval and recovery steps.
