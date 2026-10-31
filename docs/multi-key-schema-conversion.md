# Multi-key schemas and explicit conversion

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

## Existing data and the upgrade boundary

The removal release refuses an installed schema with an older shared-value layout.
It does not guess record correspondence or silently change the write behavior.
Complete the explicit app conversion with a previous release that supports that layout.

The conversion must preserve every selected current atom, author clock, key metadata, and record count.
It must classify the complete key domain and refuse unique values that lack an app member.
It pauses writes, copies references into owned molecules, verifies complete coverage, and publishes the target layouts atomically.
The app writes its config to disk before requests resume.

Complete active conversion jobs and convert auxiliary schemas before the upgrade.
Keep the primary unchanged during source and CI work.
An app release, an explicit data conversion, and a daemon upgrade are separate actions.

## Publication and field definitions

Schema Service supplies the canonical definitions, descriptions, and types.
The registered app owner publishes its definitions through the signed publisher route.
An owner node can install those published definitions without the developer's private key.
The app retains the returned runtime schema address and definition identity.
Local alternate declarations remain separate from the global definition identity.

## CI evidence

Use exact before and after revisions with equal logical record counts, values, and body bytes.
Verify complete membership, order, current values, primary keys, deletes, cursors, and reopen.
Compare request latency after one-time conversion cleanup finishes.
Lower physical size is valid when logical data remains complete.
Extra copies from the previous layout are not a minimum data-volume requirement.
Keep raw results in CI artifacts and link the run in the PR.
The normal build, security, disk-budget, and benchmark-complete gates remain required.
