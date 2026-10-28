# Canonical field names and registry reset

Status: Approved by Tom on 2026-10-27. Implementation in progress.

Canonical fields use names that identify one clear fact across domains.
Tom permits a reset of affected definitions. Published use does not justify a name exception.
The reset concerns schema-service definitions. It does not delete primary data.

## Current seed

The canonical seed contains 151 fields. Fifty-one names have one component.
Examples include `id`, `name`, `content`, `timestamp`, `amount`, and `quantity`.
The seed contains none of the 20 audited app abbreviations.
The schema publication path can promote new app fields into the global canonical registry.

The built-in schema seed is empty. The Lambda does not run the Schema.org seed.
Both conditions stay in place. Offline templates must not repopulate invalid canonical names.

## Name rule

Each canonical name must meet these rules:

1. Use lowercase words with underscores. Use at least two meaningful components.
2. Identify the subject and property. Add the unit or encoding when it changes the meaning.
3. Use full concept names. An app name or namespace cannot substitute for the meaning.
4. Reject app abbreviations and unqualified names such as `at`, `size`, `id`, `name`, and `status`.
5. Reject vague longer names such as `item_data`, `entity_name`, and `generic_record_payload`.

Short standard terms can form part of a qualified name.
Examples include `user_id`, `photo_url`, `book_isbn`, and `file_size_bytes`.
Connectors such as `of` remain valid in `date_of_birth`.
A character count or word count alone does not establish a descriptive name.

Examples of candidate changes follow. Each replacement requires a matching definition and type.

| Current name | New definition or action |
| --- | --- |
| `at` | `notice_event_occurred_at` for the existing notice concept. |
| `size` | Select `file_size_bytes` or define `text_content_size_bytes` from the actual value contract. |
| `name`, `title`, `description`, `summary`, `content` | Define the subject. Reuse `document_title` where its meaning matches. Drop entries with no precise subject. |
| `timestamp`, `created_at`, `updated_at` | Name the event and subject, such as `graph_edge_created_at`. |
| `amount`, `price`, `quantity` | Define the monetary or item concept and its unit. Do not invent an unspecified numeric replacement. |
| `sender`, `recipient`, `cc`, `bcc` | Use message or email roles, such as `message_sender` and `email_cc_recipients`. |
| `city`, `country`, `bio`, `username` | Consider `address_city`, `address_country`, `person_biography`, and `account_login_name`. |
| `image_width`, `image_height` | Use `image_width_pixels` and `image_height_pixels`. |
| `rle_h`, `bge_src` | Use full concepts, such as `record_list_hash_key` and `graph_edge_source_record_slug`. |

The seed review covers all 151 definitions, including qualified names.
Keep precise names such as `invoice_number` and `date_of_birth` when their definitions and types fit.
Remove an ambiguous seed entry when a replacement would require a guess.
Do not add a generic prefix merely to satisfy the word rule.

## Canonical field approval

Use the existing canonical registry as the set of approved definitions.
An ordinary schema publication can reuse that set. It cannot silently promote an unknown app field into a global canonical field.

A genuinely new concept requires a separate, explicit canonical publication after review of its name, description, type, and version.
An operator can publish approved definitions in a batch. No new approval database or runtime language model is required.
The seed supplies the initial approved set. Later canonical publications can extend it without a service binary change.

This adds an explicit approval step for new concepts.
It closes the semantic gap that a blacklist or length rule cannot close.
The canonical publication command requires permission from the service operator.
Ordinary app schema publication cannot grant that permission.
The implementation must define that permission and the batch command before deployment.

Each approved definition records the active name policy version.
Only the canonical publication path sets this value.
The policy version is administrative metadata. It does not change data field identity.
Imports and resolver artifacts must carry approved definitions for the active policy version.
Do not trust approval flags from ordinary app requests.

An unapproved name receives an `unapproved_canonical_field` result.
An invalid name receives an `invalid_canonical_name` result with the field, reason, and a qualified example.
The service must not silently invent a replacement, infer a new type, or approve a vague name through similarity alone.

External data can retain local names such as `id`.
An app adapter maps those names to approved canonical names before schema publication.
Canonical approval defines meaning and type. Existing declaration handles still control shared data.
Approval of the same canonical concept must not merge unrelated apps' values.

## One strict rule for every source

Use the same validation before seed insertion, schema publication, declared-field publication, and direct canonical publication.
Check imports, restore artifacts, resolver artifacts, and cache load before they expose canonical definitions.
Validate a complete batch before the first write. Reject an invalid batch rather than silently remove its fields.

Validate before an existing-entry return and again before a final definition write.
An existing name, stored hash, app namespace, or seed flag provides no exception.
Automatic matches can select only approved definitions that meet the policy.

Remove the legacy name exceptions from PR #597.
The name rule no longer needs a search through stored schema families or a comparison of old identities.
Keep the general fixes that preserve field versions and identities during a valid schema expansion.

## Reset boundary

Before the reset, fetch a fresh definition snapshot and prepare the exact removal set.
Remove rejected canonical definitions and their embeddings.
Remove or replace dependent schema definitions, declaration fields, and published app contracts that contain rejected canonical names.
Regenerate their derived registry and resolver artifacts. Invalidate warm service caches.

Published references do not block this reset. Do not keep aliases that accept the rejected canonical names.
Preserve unrelated app identities, permissions, credentials, and definitions.

CAUTION: New field names can produce new field and schema identities.
Update the affected app definitions before normal publication resumes.
Any remap of existing local data requires a separate, explicit operation.
The cloud definition reset does not copy, reset, or delete the primary home, atoms, or files.

## Work order

1. Review a complete seed manifest with Keep, Replace, Split, or Remove for each definition.
2. Implement the common rule, explicit canonical publication, and removal of legacy name exceptions.
3. Update the affected app definitions. Prepare the exact reset manifest and definition-only recovery snapshot outside Git.
4. Use CI for builds and normal required checks. Use real DEV service operations for the publication proof.
5. Coordinate a short schema publication pause. Activate the clean registry and strict service together, then refresh service caches.
6. Confirm that precise approved names work and rejected names fail through every publication source.
7. Confirm that restart and import cannot restore rejected names. Resume publication with the updated app definitions.

No tests or fixture harnesses form part of this work.
Product seed validation must fail before partial seed insertion.
The live operational check must cover a fresh service start, an existing-name request, and an invalid import batch.

## Source references

Seed counts use source revision `88cd15cfcbba781b8ed0aea6071757a8080c0d1f`.

- [Canonical seed](../crates/core/src/builtin_canonical_fields.rs)
- [Empty built-in schema set](../crates/core/src/builtin_schemas/mod.rs)
- [Lambda seed order](../crates/server_lambda/src/state_init.rs)
- [Automatic canonical registration](../crates/core/src/state_fields.rs)
- [Current name policy](../crates/core/src/field_name_policy.rs)
- [PR 597](https://github.com/EdgeVector/lastdb/pull/597)
