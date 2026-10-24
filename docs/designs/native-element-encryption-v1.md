# Native records use their existing element encryption

The native pack container has no whole-file encryption.
It keeps its existing compression, checksums, and file format.

This change removes a duplicate record envelope from two exact record families:

- Atom body records keep their existing encrypted content.
- New account-v2 CAS records contain one encrypted file envelope.
- Old CAS records keep their account envelope and their original file keys.

The change does not remove atom counters, molecule counters, or blob counters.
It does not change the hash codec, range codec, or molecule key bundle.
The CAS record keeps its JSON fields and its existing `ENC:` file representation.
The new suite changes the file address to the hash of the encrypted envelope.

## Record policy

The writer recognizes an atom body through the existing body-key codec.
It recognizes a new CAS record through its key, account-v2 suite, and encrypted file fields.
The key and `content_sha256` must contain the same encrypted-byte hash.
The record must omit file access keys.
Those records go to the native store without another account envelope.

Old CAS suites keep account encryption.
Their public file names can expose enough information to derive their old file keys.
A new reader does not rewrite those records or their atoms.

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

## Per-file key and CAS contract

The account already has a private content key that every restored device derives from the same account identity.
New files use that key as the secret input to HKDF-SHA256.
The derivation uses the dedicated `lastdb-file-blob-account-v2` salt and a file-purpose label with the original file hash.
It produces one 32-byte file key for each original file hash within that account.
The hash alone cannot produce that key.

The file key produces a deterministic 12-byte nonce through a separate HKDF purpose.
The original file receives one AES-256-GCM encryption step in the existing binary envelope.
SHA-256 of that complete envelope becomes the public `sha256:<hash>` CAS address.
The `ENC:` prefix and base64 text are storage representation, not another encryption step.
Names and media types do not enter the address.

The encrypted atom contains the original hash, file key, encrypted-byte address, suite, and encrypted size.
The raw CAS record contains the encrypted-byte hash and encrypted bytes.
It contains neither the original hash nor the file key.
Nonsecret dependency records and local capture files use the encrypted-byte hash for this suite.
No new file-key lookup or account secret is stored.

The same file on devices with the same account identity produces the same key, envelope, and address.
A repeat local write reuses the existing CAS record.
Another account produces a different key and address.
A shared atom supplies its original file key, so a recipient needs no sender account secret.
Repair reproduces the original envelope from that file key and verifies its address before any write.

Readers keep the old plaintext-addressed suites.
New writes use a distinct account-v2 suite and a new encrypted-byte address.
They cannot overwrite the older ciphertext at its original plaintext address.
An authentication failure does not authorize replacement with a different key.

The final encrypted-byte CAS address cannot serve as the key-derivation input.
That address exists only after the encryption step.
The key derivation uses the original file hash.

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
