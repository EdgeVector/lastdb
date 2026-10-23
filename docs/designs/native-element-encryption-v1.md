# Native records use their existing element encryption

The native pack container has no whole-file encryption.
It keeps its existing compression, checksums, and file format.

This change removes a duplicate record envelope from two exact record families:

- Atom body records keep their existing encrypted content.
- CAS records keep their existing encrypted file bytes and file key format.

The change does not remove atom counters, molecule counters, or blob counters.
It does not change the hash codec, range codec, molecule key bundle, or file pointer.
It does not change the local CAS representation.

## Record policy

The writer recognizes an atom body through the existing body-key codec.
It recognizes a CAS record through its existing key and encrypted file fields.
Those records go to the native store without another account envelope.

Other records keep their current protection.
This includes tips, counters, history, conflicts, metadata, and the change feed.
Some of those records contain copies of the original API keys or private metadata.
A plaintext exception for a complete namespace would expose those fields.

The ordinary reader accepts a direct record only when it has the required inner envelope.
It also reads old records through their existing outer envelope.
A failed outer authentication never becomes a plaintext read.
Atom and file readers still check their existing inner authentication.

Reap preserves the two inner-protected record families.
Reseal does not add another envelope to a direct inner-protected record.
There is no bulk rewrite of old records.

## Old-reader boundary

The encrypted logical marker advances from `4` to `5` before the node serves new writes.
The normal native request makes that marker durable.
An older reader refuses marker `5` before it can discard an unfamiliar direct record.
The new reader accepts marker `4` and reads its existing records.
Native restore accepts marker `4` or `5` after account authentication.

The pack format and cloud protocol do not change.
Native backup copies the exact stored file bytes.
The shared atom and CAS decoders support both record forms during backup and restore.
The manifest, directory, and control-record authentication stay in place.

## File size and memory

The separate local file size refusal and native batch byte refusal are removed.
The existing record format still uses a four-byte body length.
The native write queue keeps its shared memory allowance, operation limits, and disk-space checks.
The general socket request limit remains in place.
Only the exact owner-only raw local file route can exceed that request limit.

Source checks establish code consistency.
The owned DEV command receipts establish actual backup and restore behavior.
No primary home or primary daemon forms part of this change.
