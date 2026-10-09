# Starter schema templates

The schema service seeds a small app-template namespace at boot:
`owner_app_id = "templates"` and `source = "starter_seed"`.

These templates are not system primitives. They are starter identities that
apps can bind to explicitly instead of minting app-local near-duplicates:

- `KnowledgeRecord`
- `EventRecord`
- `StateSnapshot`
- `WorkItem`
- `DocumentRecord`
- `MediaAsset`
- `GraphNode`
- `GraphEdge`
- `SecureKV`

Template identity follows the system-treatment rule: app-level meaning belongs
in a `kind` field when the database treatment is otherwise the same. For
example, notes, concepts, preferences, and SOPs can all bind to
`KnowledgeRecord` with different `kind` values. `SecureKV` remains separate
because `secret_value` is classified as highly restricted and must be handled
differently by the system.

Registration-time matching has a dedicated template-cover step before the
embedding-beam component cover. It scans active `templates` starter seeds and
counts exact proposal field-name coverage. A template covers a proposal when it
matches at least 60% of proposal fields; full coverage returns a composed
response without minting a new schema identity, while partial coverage lets the
existing residual-registration path mint only uncovered fields.

This step is intentionally independent of broad purpose text. The normal
purpose gate stays strict for general schema reuse; templates work through this
explicit channel instead of trying to attract proposals with vague purposes.

Operational follow-up for publishing a new resolver pack:

1. Start schema service with the committed seeds and export `GET /v1/snapshot`.
2. Build a resolver pack from that snapshot; the pack publisher's shared-only
   projection includes `starter_seed` templates.
3. Publish to dev, then prod.
4. Replay the historical user-schema corpus and measure template-cover hits.
   The target is at least 60% template hits against the real 81-schema corpus.
