# One-language schema resolution

The authoritative design is
[direct_schema_registration.md](direct_schema_registration.md).

Summary: LastDB uses one global catalog language. Apps and agents may propose
local names at the edge, but resolution returns a Schema Service catalog
identity plus an edge adapter. The database stores only global catalog schema
identities and catalog-shaped records; adapters do not become the on-disk
language.

