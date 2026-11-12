# LastDB spec catalog

The enforced catalog is `database_policy`. This directory is the same list. Registry version `1` is catalog label `1.0`. The stored spec object is `{ "name", "version" }` with an integer version.

LastDB reserves the database names `graph`, `social`, `medical`, `finance`, and `work`. An app id or a schema prefix does not own those definitions. A name prefix does not select a spec.

`social` is the only reserved name with a schema body. The database `social`, and any independently named database such as `social-family`, uses that body only when the stored spec is `{ "name": "social", "version": 1 }`. The schemas are in `spec/social/`.

`graph` is reserved and unprovisioned. It has no schemas here. Create refuses it. `social-graph` is a different thing: it is personal-only, and it never appears in a shareable database.

`medical`, `finance`, and `work` are reserved names that require `spec.name` to equal the database name. They have no schema body. Share admission denies them with `unknown database spec`. A stored registry row is not permission to publish.

`personal` is the non-shareable database. The binding is null. App-private schemas, fields, and files stay there, including `hearth/PhotoMeta` and `social-graph`.

`lastdb-files` version 1 admits the `FileCatalog` and `FileChunk` records used by Files. Create an owned database with `lastdb db create NAME --spec-name lastdb-files --spec-version 1`. The database name is independent of the spec.

Within one major version a schema changes only by adding optional fields. A breaking change is a new schema name and a copy of the records. An app keeps its own extensions in its namespace in the personal database, keyed by the standard record id.
