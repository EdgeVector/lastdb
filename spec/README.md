# LastDB standard schemas 1.0

`social-graph` is personal-only. It never appears in a shareable database.
`social` is shareable.

Within one major version a schema changes only by adding optional fields.
A breaking change is a new schema name and a copy of the records.
An app keeps its own extensions in its namespace in the personal database,
keyed by the standard record id.
