# Security

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub's
[private vulnerability reporting](https://github.com/EdgeVector/lastdb/security/advisories/new)
on this repository. Do not open public issues for security reports.

## Data-at-rest posture

LastDB targets a **password-manager-grade** at-rest posture: all user data
encrypted at rest, no plaintext fallback, a documented key hierarchy, and key
material that clears from memory after use.

The threat model, the current state, and the gap list are in
[`docs/security/at-rest-threat-model.md`](docs/security/at-rest-threat-model.md).

Current state, in short:

- A format 8 home encrypts every block of its run files with AES-256-GCM.
  Records, alternate entries, field key metadata, and all other entries are
  inside those blocks.
- Each hash in storage is blinded with the blind key of its field key.
- Each file blob has its own key. Local file blobs also have an envelope under
  the account content key.
- All data keys come from the plaintext `identity.key` seed (owner-only file
  permissions). A person who obtains the seed can derive every data key.
- With Search on, searchable field values go to the Search inbox as plain JSON
  outside the run files.

## Key invariants

- **One key root.** A home opens only with the key that wrote its account-key
  proof. A wrong key gets a refusal; the program does not make a new key.
- **No silent key rotation.** No fallback path writes over an existing key.
- **E2E before egress.** The cloud backup sends the encrypted run files, and
  file blob uploads are encrypted on the client. The cloud stores ciphertext only.
