# Journal resume for separate artifacts

The artifact writer supports repeated open, complete recovery, and explicit checkpoints.
The LastDB home and memory ack paths do not use this writer yet.
The primary, archive, factory, and reset stores remain outside this command's scope.

One typed exclusive lock protects journal append, directory publication, and stream changes.
The writer does not acquire a second file lock during a checkpoint.
The root path comes from that lock's canonical artifact identity.
Unknown formats and unsupported write features fail before the lock file opens for writes.

The CURRENT lifecycle feature is mandatory bit one, `0x2`.
It requires the complete-object-bounds feature, bit zero.
An initial CURRENT with `previous_generation = 0` admits no journal batch.
The writer first completes an empty stream checkpoint with a nonzero previous generation.
It then permits append.
Older programs refuse the new mandatory feature before payload access.
The reader retains support for older artifacts.

The initial stream UUID never supplies a frame identity before its checkpoint.
A retry can retain an incomplete initial directory and allocate a fresh stream.
No admitted stream can treat a missing TAIL as an empty journal.
An older artifact without the lifecycle feature retains the conservative existing-directory refusal.

An open with an existing TAIL follows this order:

1. Acquire exclusive artifact write ownership.
2. Capture and validate CURRENT and TAIL.
3. Verify every complete batch after the committed frontier.
4. Publish the complete directory through the durable frontier.
5. Archive the exact old TAIL under its old stream and checkpoint.
6. Publish an empty fresh-stream TAIL with the exact predecessor digest.
7. Publish CURRENT with the fresh checkpoint stream.
8. Permit new append calls.

The archive path is `journal/<old-stream-uuid>/tail-<checkpoint-as-16-hex-digits>`.
The archive contains the exact 4,096 published old TAIL bytes.
The predecessor digest covers all those bytes, including their stored checksum.
The writer writes the bytes to `tail-<checkpoint-as-16-hex-digits>.pending`, makes them durable, and renames that file to the archive name.
It then makes the stream directory durable.
These barriers precede the new TAIL.
A crash leaves no archive, an unused pending file, or a complete archive.
The next open reuses the same pending name, so a leftover pending file does not accumulate.
An existing archive that matches exactly receives fresh file and directory barriers.
A file at the archive name that is short or does not decode as a TAIL is a leftover of an older in-place write or of a failed write.
No TAIL can reference it before the new TAIL exists, so the exact bytes replace it by rename.
The writer never overwrites a complete archive that decodes to a different TAIL; it refuses that checkpoint.

The new TAIL contains no batch before CURRENT selects its stream.
Its predecessor names the old stream and the exact committed frontier.
A valid transition requires CURRENT to reach that boundary.
The reader verifies the archived old TAIL before it accepts a stream mismatch at that boundary.
A mismatch below the boundary refuses recovery.
The explicit publisher can finish that verified empty-stream transition.
No new admission occurs between the two publications.

An append after the checkpoint clears the predecessor fields in its new TAIL.
Its global batch sequence continues from the committed frontier.
Its stream UUID is fresh, so an old incomplete frame identity cannot repeat.
Old streams remain addressable through committed locator extents.
The writer retains all old files; this unit performs no reclamation.

An explicit writer checkpoint publishes all complete pending batches and starts a fresh stream.
It resets the bounded suffix reserve only after both publications succeed.
A checkpoint with no new work keeps its generation.
Any publication failure poisons write admission.
The caller must reopen and recover instead of retrying a partial batch identity.

The commands use explicit artifact paths:

```text
lastdb-journal append --artifact PATH --input generated.jsonl --batch-size 64
lastdb-journal write --artifact PATH --input generated.jsonl --batch-size 64 --publish-every 2
lastdb-journal recover --artifact PATH
lastdb-journal publish --artifact PATH
```

The append command creates its first stream and retains an unpublished complete suffix.
Its initial stream checkpoint changes only recovery metadata.
The write command resumes the artifact and publishes after the stated number of complete batches.
It publishes any final complete pending batches before it exits.
The library supplies explicit calls; it adds no background timer.
The production 500 ms flush still needs the home and durability-token integration.

The existing complete-batch and suffix admission limits remain in force.
The logical-record warm set retains its existing path and 10,000-record limit.
This artifact result does not establish native LastDB restart recovery or a primary upgrade result.
