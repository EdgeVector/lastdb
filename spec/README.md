# LastDB spec catalog

The enforced catalog is `database_policy`. This directory is the same list. Registry version `1` is catalog label `1.0`. The stored spec object is `{ "name", "version" }` with an integer version.

A database name is a label. It does not select a spec, and no database name is reserved. The stored spec object selects the record types. A database uses the social schemas only when its stored spec is `{ "name": "social", "version": 1 }`. Those schemas are in `spec/social/`.

`social-graph` is personal-only. It is not a database name, and it never appears in a shareable database.

`personal` is the non-shareable database. The binding is null. App-private schemas, fields, and files stay there, including `hearth/PhotoMeta` and `social-graph`.

`lastdb-files` is a publication spec, not a database name. It admits the `FileCatalog` and `FileChunk` records used by Files. Create an owned database with `lastdb db create NAME --spec-name lastdb-files --spec-version 1`. The database name is independent of the spec. `lastdb share create --into` with no `--spec` stays on the legacy path and writes no spec. That absence is not classified as `lastdb-files`. An explicit `--spec` is validated, and a denied spec writes nothing. A social `Photo` file is eligible on the social spec. A private file, including `hearth/PhotoMeta`, is not.

Within one major version a schema changes only by adding optional fields. A breaking change is a new schema name and a copy of the records. An app keeps its own extensions in its namespace in the personal database, keyed by the standard record id.
