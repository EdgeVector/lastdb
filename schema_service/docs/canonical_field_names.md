# Canonical field names

A canonical field name must identify one fact without the schema name.
Use full words for the concept and its property.
State the unit or encoding when it changes the meaning.

App abbreviations are not acceptable canonical names.
A description or app namespace does not make an abbreviation acceptable.
For example, use `brain_record_list_hash_key` instead of `rle_h`.
Use `brain_graph_edge_created_at` instead of `bge_created_at`.
Standard domain acronyms such as `url`, `json`, `gps`, and `isbn` remain valid.
The explicit pressure-unit suffix `pressure_psi` also remains valid.

## Direct checks

The service rejects these audited app abbreviation tokens:

`bcl`, `bes`, `bfc`, `bge`, `bll`, `bln`, `blp`, `btd`, `bts`, `clu`,
`ctd`, `eph`, `ie`, `klh`, `liv`, `nh`, `prk`, `psi`, `rle`, and `tmb`.

The check recognizes tokens across case, separator, digit, and camel-case
boundaries. A namespace prefix cannot hide a token.
The check does not depend on an embedding service.
It covers direct canonical-field insertion, schema publication, and new field
declarations. Automatic field-name matches exclude the old abbreviated names.

This list gives a direct check for the audited abbreviations.
It does not recognize every possible abbreviation.
Review must reject new app abbreviations and extend the list when necessary.
The rule does not approve a generic name merely because the check accepts it.

## Existing definitions

CAUTION: A rename changes field identity. Do not rename stored definitions in place.

Existing field and schema reads retain their stored names and identities.
An unchanged field can remain in the same stored schema family during migration.
The app owner, schema descriptive name, field type, version, and description
must match a stored definition. An existing field declaration can also retain
its identity on a repeat declaration.

A new schema family or declaration cannot create an abbreviated field.
An incompatible type or version change also requires an acceptable name.
Add replacement declarations and migrate the apps before removal of the old fields.
This code change does not delete registry definitions or change primary data.
