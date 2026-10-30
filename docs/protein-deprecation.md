# Protein deprecation and app conversion

Decision: Tom, 2026-10-29. Proteins are deprecated. New app designs use named alternate keys for the same records.
Canonical field reuse supplies definitions and types. It does not establish shared record values.
The node advertises `proteins_deprecated: true` through `/health` and `/api/version`.

Existing protein schemas retain their behavior during the transition.
Deprecation does not delete records, detach molecules, or change a schema key.
No automatic conversion runs at startup.

## App design

Choose one schema for each logical record type. Keep its primary key stable.
Declare named alternate keys from fields in that schema.
Use the alternate selector for reads, counts, and key lists.
Keep the returned primary key for mutations.
Collect independent keys and mutations into batches.

The first supported layout has a Hash primary key in the personal database.
Each complete alternate key includes the primary field.
Mapped fields, compound primary keys, received-data scopes, and alternate watches require additional product work before app conversion.

The public app declaration route installs one owned schema under `app_id/name`.
Its `canonical` response names that runtime schema. Its `identity_hash` names the exact definition from Schema Service.
The node also accepts that identity for record requests.
Schema Service supplies the canonical field definitions, types, and classifications.
The developer publishes those definitions through the existing signed publisher route.
An owner node installs published definitions without a developer credential.
The node keeps alternate declarations separate from that definition identity.
It does not load a second data schema or bind service field mappers.
An existing local schema needs an explicit conversion. A repeat declare cannot change its key or alternate declarations.
The read-only `check` intent writes no local schema or service registration.

## Conversion order

1. Inventory app declarations, retained schema names, reads, writes, lists, counts, watches, and shared fields.
2. Classify each relationship as one record with alternate keys, an explicit same-key field alias, or unrelated records.
3. Select the authoritative schema, field maps, required key fields, and each alternate's missing-value rule.
4. Collect the complete known primary-key domain. A sample or partial page does not establish coverage.
5. Inspect old records by those keys. Report missing fields, divergent values, clocks, metadata, aliases, and external bindings.
6. Extend the explicit owner conversion for accepted protein relationships. Do not bypass the current unshared-field refusal.
7. Rewrite the app to address the chosen schema and named alternates. Preserve primary mutation keys and batch requests.
8. Use real generated app records in CI. Verify complete membership, values, metadata, key moves, deletes, and reopen.
9. Compare latency and disk size with equivalent logical records. Keep the required checks and artifact verification.
10. Publish the app and data cutover as one explicit operation. Retain old references until every caller and share moves.
11. Retire old molecules only after complete conversion and an explicit recovery decision.

Drain old signed peer events before a declaration change.
Resume peer writes after each peer uses the same declaration revision.
The owner route does not coordinate nodes or change remote schemas.

## First conversion boundary

PR #758 supports stopped-home activation and resumable declaration changes for owned, unshared fields.
It preserves primary atoms, author clocks, and metadata.
It refuses existing protein members, external field bindings, and existing field generations.
An app with those relationships needs the correspondence converter before its data cutover.
An app can prepare its new declarations and request flow before that cutover.

Keep the primary and the archived home unchanged during this source and CI work.
The app inventory and accepted conversion contracts belong in repository documentation and PRs while the primary stores remain retired.
