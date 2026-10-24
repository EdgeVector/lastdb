# Native records use their existing element encryption

The native pack container has no whole-file encryption.
It keeps its existing compression, checksums, and file format.

The current policy wraps only local CAS blob records.
Tom set this policy on 2026-10-23.

- Atom bodies keep their existing content encryption and no outer storage envelope.
- Tip values, counters, history, conflicts, metadata, and change-feed values use direct storage.
- Local CAS records receive an account storage envelope around their existing file envelope.
- Molecule key bundles and sensitive identity fields keep their explicit key encryption.
- Hash blinding and the range codec keep their existing behavior.

## Record policy

`EncryptingNamespacedStore` selects only the exact `cas_blobs` namespace.
Its constructor has no caller-controlled plaintext list or broad encryption option.
All other namespaces return the underlying storage handle directly.
The wrapper never encrypts a complete packed file or a lookup key.

The wrapper encrypts the complete local CAS record, including the blob metadata and encrypted file bytes.
The CAS reader can still decode an account-v2 record with only its existing file envelope.
Every new CAS write receives the outer account envelope.

Atoms retain encryption of the content field.
The atom header remains readable: atom identity, schema, source filename, creation time, and optional metadata.
Tips retain their key encoding, while their atom pointer and per-key metadata remain readable.
Other direct control records can also contain original API keys and private metadata.
Native cloud backups copy these exact stored bytes and preserve that visibility.
This is an explicit policy change, not whole-file encryption.

Reseal and reap accept only `cas_blobs`.
They cannot add wrappers to atom or tip records or delete those direct records as unsealed residue.
Replay writes direct non-CAS bytes and cannot restore the broad encryption policy.

The keyed constructors retain a single explicit encrypted account-key proof.
This small control value verifies the account key before the database can serve requests.
It is not a namespace-wide storage envelope.
A wrong key or changed proof refuses startup and never replaces the proof.
Keyless embedded construction keeps its existing behavior and cannot supply a keyed native cloud restore.

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

The logical marker `layout:v3:keys` uses direct ASCII value `6`.
The node format marker uses `format=2`.
The creation-intent marker names the third current-key initialization protocol.
Normal startup rejects earlier node markers before it opens the physical store.
A direct physical logical constructor also requires marker `6`.
Startup does not rewrite an older home.
The explicit native record transfer upgrades logical formats `4` and `5` in its inactive target.
The old reader exports the source. The new reader authenticates its encrypted format marker before a target write.
The conversion removes former outer envelopes except CAS and preserves the existing inner encryption.
It publishes marker `6` and the new account-key proof last. An interrupted target can resume conversion.
The source remains unchanged and can reopen with its original program.
This change does not open or change a primary home.

Native restore requires marker `6` and the explicit encrypted account-key proof.
It checks both before destination publication.
Inactive restored-home validation uses the same two-key check.
An existing home with a missing proof refuses startup. Fresh creation writes the proof and marker in one native transaction.
The atom and file readers still authenticate their existing encrypted content.
The metadata and file-dependency decoders read the direct control records.

The physical pack format and cloud transport do not change.
Native backup copies the exact stored file bytes.
New backups retain only the CAS storage wrapper, atom content encryption, and explicit key encryption.

## File size and memory

The separate local file size refusal and native batch byte refusal are removed.
The existing record format still uses a four-byte body length.
The native write queue keeps its shared memory allowance, operation limits, and disk-space checks.
The general socket request limit remains in place.
Only the exact owner-only raw local file route can exceed that request limit.

Source checks establish code consistency.
The owned DEV command receipts establish actual backup and restore behavior.
No primary home or primary daemon forms part of this change.
