# Canonical vocabulary operations

The canonical registry defines the standard vocabulary. Each entry has one approved name, description, type, and identity version.
App schema publication reuses this vocabulary. It cannot add a new canonical field.
A declaration handle still controls shared data. Vocabulary approval does not grant access to another app's data.

## Approval and publication

Review each new concept before publication. Name the actual subject and property.
Specify units, text formats, and container element types when they affect the meaning.
Use an existing definition when its meaning and type match.

The service operator publishes approved fields through `POST /v1/fields/vocabulary`.
The body contains these properties:

- `operation`: `canonical_fields.publish`.
- `environment`: `dev` or `prod`, equal to the service environment.
- `name_policy_version`: `1`.
- `fields`: an object with 1 through 512 name-to-definition entries.

Each definition contains `description`, `field_type`, `version`, and optional classification metadata.
The server assigns the active name policy version. App requests cannot assign approval.
The request requires `X-Canonical-Signature`: a base64 Ed25519 signature over the canonical JSON body.
The configured `APP_REGISTRY_VERIFIER_PUBKEY` defines operator authority. An app developer certificate does not grant this authority.
The operation and environment form part of the signed body.
A repeated identical batch is safe. A conflicting definition requires an explicit reset before replacement.
The complete batch passes validation before the first write.

Use `schema_service/scripts/canonical-vocabulary` for real operator work.
Install its Python dependencies, `cryptography` and `rfc8785`, before use.
The `get` command returns the approved vocabulary.
The `publish` command accepts a definition file, an environment, and a LastSecrets locator for the operator Ed25519 seed.
The command retrieves the seed at the point of use. It does not print or save the seed.
It accepts HTTPS service URLs only.

## Activation boundary

CAUTION: The strict service refuses an old registry with unapproved names or definitions.
Do not deploy this source against the old registry before the coordinated reset.
App definitions and their field adapters must use the reviewed vocabulary before normal publication resumes.

Prepare an isolated registry generation from a fresh definition snapshot.
Copy only the approved definitions and required app identities, permissions, credentials, and valid contracts.
Remove affected schema definitions, declarations, embeddings, and release references from the active generation.
Keep a definition-only recovery snapshot outside Git.
Use separate storage for the old and new service versions during the deployment canary.
Old writers must not restore rejected names into the new generation.
Preserve the normal deployment gates and canary period.

Refresh all service caches after activation.
Publish the derived resolver pack with format version 3 and schema snapshot format version 2.
The snapshot carries name policy version 1. Old packs cannot enter the new client cache.
Use a real DEV request to confirm approved names work and rejected names fail.
Confirm a fresh service start and an invalid import batch before production activation.

The definition reset does not remap local data.
It does not open, copy, reset, or delete the primary home, atoms, or files.
A local data remap remains a separate operation.
