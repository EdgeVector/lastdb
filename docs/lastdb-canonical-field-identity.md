# Canonical Field Identity — registry-sourced hashes, auto-proteins, and the backfill

| Field | Value |
|-------|--------|
| **Status** | **Draft — plan for review** (Tom asked for the plan 2026-07-30) |
| **Date** | 2026-07-30 |
| **Owner** | Tom / EdgeVector |
| **Author** | Design (agent) |
| **Type** | Correction + migration plan |
| **North Star** | `north-star-lastdb-ideal-storage-shape` |
| **Milestone** | `milestone-lastdb-fkanban-protein-primary` |
| **Supersedes (in part)** | `design-lastdb-field-hash-auto-protein` § "Defaults for open edges" |
| **Already shipped** | fold #1002 (local mint + same-product bind + fold on write), fkanban `cr-ms7t4bwe-b923` (app stops reaching into proteins) |
| **In-tree (this repo)** | `docs/lastdb-canonical-field-identity.md` — **source of truth for implementers** |

---

## The target

Canonical fields exist once. A schema that uses one *references* it. Nodes
notice, on their own, when two schemas index the same record under different
keys, register the protein without being asked, and keep every related molecule
current — including the rows that already existed.

Three of those four already work. This document is about the two things in the
way: field identity is computed from the wrong inputs, and nothing folds history.

---

## 1. What is wrong with identity today

Field identity is `H(name, description, type, version)`, minted by Schema Service
in `add_schema` and (since #1002) by the local node for anything the catalog
omitted.

**Description is inside the hash. That is the defect.** Every consequence below
follows from it:

- Rewording a description forks the field. You cannot fix a typo without
  creating a new identity and orphaning the protein that used the old one.
- Two schemas that mean the same field must describe it in byte-identical prose
  to be recognised as the same field. Nothing enforces that, nothing reports it,
  and the failure is silent — the fields simply never fold.
- `version` therefore earns nothing. It is a second lever for splitting identity
  in a scheme where editing prose already splits it, and it cannot do the one
  thing a version is for: hold identity **stable** while the wording improves.

This is not theoretical. `fkanban` folds 18 of its 24 shared BoardCards ↔
MilestoneCards fields. The other 6 fail purely on wording:

| field | BoardCards | MilestoneCards |
|---|---|---|
| `board` | `board slug (HashRange partition key)` | `board slug` |
| `milestone` | `Milestone slug` | `milestone slug (HashRange partition key)` |
| `sk` | `sort key column#position(8)#slug for ordered column lists` | `sort key column#position(8)#slug` |
| `assignee` | `who owns the card, empty if unassigned` | `who owns the card` |
| `created_by` | `immutable creator identity copied from the Card record` | `creator identity` |
| `layout` | `identity marker for HashRange layout (do not change without new schema)` | `identity marker for HashRange layout` |

`layout` is the instructive one: it **must not** fold, because its value is
deliberately different per partition (`hashrange_v1_board_partition` vs
`hashrange_v1_milestone_cards`). Today it is excluded by accident — someone
happened to write a longer sentence. That is the whole problem in one row: the
system cannot tell an intentional distinction from a stylistic one, because it is
reading prose.

---

## 2. The registry already exists

`schema_service/crates/core/data/schema_org/validated_canonical_fields.json`
holds **1,642 canonical fields**, each carrying a description, a type, and a
version. `CanonicalField::field_hash(name)` already computes identity from that
entry. `SchemaState::canonicalize_fields` already resolves an incoming field
*name* onto a canonical one by embedding similarity (`FIELD_SIMILARITY_THRESHOLD
= 0.88`).

So the pipeline canonicalizes the **name** against the registry and then hashes
the **app's own description** anyway — `ensure_field_hashes` reads
`self.field_descriptions` for the description it feeds into the hash
(`schema_service/crates/schema_types/src/declarative_schemas/construct.rs:85`),
never the registry entry. Two schemas that both resolve onto canonical `title`
still receive different identities.

**Sourcing the hash from the registry entry is the fix**, and it is what makes
`version` meaningful: the version lives on the canonical field, and bumping it is
one deliberate, central act that splits that field for every schema at once
(`status` meaning HTTP status, then meaning workflow status). A per-schema
version is noise; a per-canonical-field version is governance.

### Why this is safe now and would not have been before

Until #1002, field identity alone decided whether two molecules were bound into a
protein. Under that rule, loosening identity would have been dangerous —
measured on the live catalog, `created_at` / `"RFC 3339 timestamp"` / `String` is
a single identity spanning **46 schemas**, and `Sop`, `Concept`, `Task`, `Spike`
all key on `slug`.

Binding is now gated on the two schemas being **multi-key siblings of the same
product** (`are_multi_key_siblings`: differing key layouts, non-conflicting owner
app, field-identity overlap ≥ 0.6). Identity no longer carries the safety burden
alone, so it can afford to be generous. That is precisely what registry-sourced
identity requires.

---

## 3. What is missing

Four gaps, in the order they bite. Only the first is the obvious one.

**3.1 No catalog backfill.** Schema Service mints inside `add_schema` and nowhere
else, so only schemas registered since 2026-07-28 carry identities — **2 of
1,115** on the live catalog. Nothing walks the rest. `field_hashes` is *not* an
input to `identity_hash`, so stamping it moves no pin; this is additive.

**3.2 Binding is lazy and unswept.** There is no load-all at startup;
`load_schema_internal` fires on declare, on a cache miss, on mutation, and on
purge. The peer scan only sees schemas already in the in-memory cache, so a pair
binds when the *second* sibling loads. Both orders eventually work — but a schema
nobody touches never binds, and no pass reconciles.

**3.3 Nodes cannot compute canonical identity.** `lastdb_node`'s
`schema_resolver_host` consumes the resolver pack, but the pack's
`canonical_field` records are not read. A node can only mint from local prose, so
an offline node merely fails to conflict with Schema Service rather than actually
agreeing with it.

**3.4 Binding does not fold history.** `AtomStore::protein_backfill_member_tips`
is implemented and unit-tested, and **nothing calls it** — the only reference
outside its definition is its own test. A newly bound sibling partition therefore
stays empty until every record is independently rewritten. New writes propagate;
existing rows do not.

3.4 is the gap between "the mechanism works" and "the data is right", and it is
the one a user would actually notice.

---

## 4. Plan

Sequencing matters more than the individual phases: 1–3 change what identity
*is*, and phase 5 is the only one that rewrites rows at scale. Backfilling tips
before identity settles means backfilling into proteins that phase 1 then
re-splits.

### Phase 1 — identity comes from the registry

`ensure_field_hashes` sources description, type, and version from the canonical
registry entry when the field canonicalized onto one; it falls back to the app's
own metadata only for genuinely app-private fields. Same formula, different
inputs.

Ships with a report of every field whose identity moves. Some will, and that set
is the review artifact — not a footnote.

Open question for review: for app-private fields with no canonical entry, either
(a) keep hashing app prose and accept that identical wording is required, or
(b) hash `(name, type, version)` only and let the same-product gate carry the
rest. (b) is more forgiving and is defensible now that binding is gated, but it
is a real widening and should be decided deliberately rather than inherited.

### Phase 2 — catalog backfill

A batch operator over every Available schema: canonicalize, stamp
`field_hashes` / `field_versions`, idempotent and resumable.

**Dry run is the deliverable.** Before any write, it reports which schema pairs
become siblings, how many fields each protein would carry, and the largest
resulting member set — the same measurement that caught the `created_at` problem
in the first place. Enabling the write path is a separate, evidence-backed
decision.

### Phase 3 — distribute canonical identity to nodes

Have the node read the resolver pack's `canonical_field` records so local minting
resolves against the registry. This is what turns "the node does not disagree
with Schema Service" into "the node computes the same answer".

### Phase 4 — bind reconciliation sweep

A bounded, resumable maintenance pass that walks stored schemas, mints missing
identities, and binds sibling pairs without waiting for both to be touched.
Same shape as the other plane drains. Closes 3.2.

### Phase 5 — tip backfill on bind

When a member joins a protein, enqueue a background job that walks the source
member's existing tips and points the new member at the same atoms.
`protein_backfill_member_tips` already does the work; what is missing is a job
class, a queue, rate limiting, and resumability.

**Background, never synchronous** — key regeneration is not a side effect of
expand (`preference-schema-expand-same-product-different-keys`). This phase wants
a calm primary; it should not run against an in-flight CAS verify grind.

### Phase 6 — fkanban drops its second write

Once 1–5 are in, the open card `fkanban-align-membership-field-descriptions`
collapses to almost nothing: descriptions stop deciding identity, and the
milestone partition is populated by fold plus backfill rather than by the app.
`test/no-protein-reach.test.ts` continues to hold the line that the app never
reaches into proteins to make any of this work.

---

## 5. What "done" looks like

- A field's identity survives a reworded description, and changes only when
  someone bumps the canonical field's version on purpose.
- `layout` still does not fold — because it is declared distinct, not because of
  a sentence-length accident.
- Registering a second key layout over an existing product binds automatically
  and backfills the new partition without an app writing to it.
- A cold node reaches the same bindings as a warm one, without being asked.

---

## Related

- `design-lastdb-field-hash-auto-protein` — original design, plus the 2026-07-30
  correction that binding needs same-product, not identity alone
- `design-lastdb-protein-molecule-set` — protein model
- `concepts-lastdb-protein-write-fold` — write/fold contract
- `preference-schema-expand-same-product-different-keys` — both keys stay
  addressable; reindex is background
- `docs/lastdb-ideal-storage-shape.md` — proteins plane on disk
