# Multi-key schemas

One schema owns each record and its values. Named alternate keys provide other ways to find that record.
Canonical field reuse supplies definitions and types. Equal field identities do not share mutable record values.

## Request flow

1. Declare a primary Hash or HashRange key and its fields.
2. Declare each alternate from fields in that schema.
3. Read by the primary key or select a named alternate key.
4. Use the returned primary key for a mutation.
5. Let the native request update the record and alternate addresses in one transaction.

An alternate stores primary addresses. It adds no payload copy or independent field history.
Relationships, tags, links, counters, and body files keep their own schemas when they contain separate data.
Collect known keys first. Use batch reads and batch mutations for independent work.

## Existing data

This release refuses an installed schema that does not own its values.
It does not guess record correspondence or change data in place.
Create a fresh home with this release. The previous release serves the old home.
A one-time read of the old home fills the new home through normal declarations and batch writes.
Keep the primary unchanged during source and CI work.

## Publication and field definitions

Schema Service supplies the canonical definitions, descriptions, and types.
The registered app owner publishes its definitions through the signed publisher route.
An owner node can install those published definitions without the developer's private key.
The app retains the returned runtime schema address and definition identity.
Local alternate declarations remain separate from the global definition identity.

## CI evidence

Each app checks its own LastDB compatibility in its own CI.
Keep raw results in CI artifacts and link the run in the PR.
The normal build, security, disk-budget, and benchmark-complete gates remain required.
