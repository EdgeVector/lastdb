# LastDB access complexity requirements

**Status:** product requirement (won't-undo)  
**Audience:** engine, apps, agents  
**Model:** Dynamo-style NoSQL — **not SQL**

---

## One-sentence rule

LastDB supports **keyed access only**: point get by hash (and optional exact range), and **ordered range under a single hash**. There is **no full scan**. There is **no SQL-style table scan or ad-hoc field filter as a query plan**.

If the access pattern is not expressible as a hash (and optional range under that hash), you **design a second keyed schema**, not a scan. Prefer protein fold for shared fields. Dual-write only a thin projection.

---

## Supported operations and required complexity

Let:

| Symbol | Meaning |
|--------|---------|
| — | Exact key known end-to-end |
| **M** | Number of live keys under **one** hash partition (ordered by range key) |

| Operation | Filter shape | Required complexity | Dynamo analogue |
|-----------|--------------|---------------------|-----------------|
| **Point get** | `HashKey` (Hash schema) or exact `HashRangeKey { hash, range }` | **O(1)** | `GetItem` |
| **Range under hash** | `HashKey` on HashRange (all ranges), `HashRangePrefix`, `HashRangeRange` | **O(log M)** | `Query` on PK (+ SK condition) |
| **Multi-get exact keys** | `HashRangeKeys` (K pairs) | **O(K)** | `BatchGetItem` |

### What these bounds mean

- **HashKey / exact HashRangeKey = O(1)**  
  One known key → constant-time tip resolve (relative to database size). Table growth does not make a point get slower in the access model.

- **Range under a hash = O(log M)**  
  Range work is over the **ordered range-key set of that partition only**, size **M**. Complexity is logarithmic in **partition** size, not in global table size **N**.  
  You must supply the **hash** (partition). A “range” without a hash is not a supported access pattern.

- **Multi-get exact keys = O(K)**  
  K known `(hash, range)` pairs → K point gets, whatever partitions they span. This is the only bounded shape that is **not** confined to one hash. The batch must never cost more than reading the partitions it names; if it does, the caller is better off not using it, which defeats the operation.

  The table above stated this from the start and the query planner did not implement it: `HashRangeKeys` was absent from `load_filtered_molecule`'s match until 2026-09-06, so it fell to the O(field) full read. The row loss was the worse half — the full path leaves range slots in storage form, and the in-memory apply has no codec, so an OPE-encoded home matched none of them and the batch returned FEWER rows than it was given, with no error. A bound in this table is a claim about a code path; a variant missing from the planner satisfies no bound at all.

- **Field count is not an asymptotic parameter**  
  Schemas have a fixed, small field set. Projecting more fields changes a constant factor, not big‑O.

- **Physical store (LSM/B-tree seeks, decrypt, etc.)**  
  Implementation detail. The **product requirement** for API/query planning is the table above — same way Dynamo is specified to clients.

---

## Forbidden: full scan

| Not supported | Why |
|---------------|-----|
| Full table / full field scan as a list or query plan | Not Dynamo Query; historical node meltdowns |
| “Scan then filter” for SQL-like `WHERE column = …` | Filters are **key-shaped only** |
| Unscoped `Page` / enumerate-all as the hot path for product lists | Same class as scan |
| Cross-partition range (`RangePrefix` / `RangeRange` / `RangeKey` without hash) as a first-class product op | Walks many partitions ≈ scan |

**There is no full scan.** LastDB is not SQL. Apps must not depend on scan, and the engine must not treat scan as a supported query class for product work.

If you need another list shape, **add a second Hash / HashRange schema** whose hash (and range encoding) makes the query **O(1)** or **O(log M)** under one partition. Shared payload fields fold via protein. Dual-write only a thin projection.

---

## Access-pattern design (required)

1. **Name the query** before the schema (point by id, list by board, list by column, …).
2. **Primary schema** serves the truth key (usually Hash by slug/id) → **O(1)** get.
3. **Other shapes** get a **second schema**. Shared fields use protein fold. Dual-write only a thin projection.
4. **Range keys** are designed so useful lists are **prefix or between under one hash** → **O(log M)**.
5. Never “we’ll scan and filter later.”

### Example (fkanban)

| Intent | Schema | Complexity |
|--------|--------|------------|
| Card by slug | `Card` HashKey(slug) | **O(1)** |
| Cards on a board | `BoardCards` HashRange(hash=board, range=`col#pos#slug`) | **O(log M)** for that board’s partition |
| One column | same + range prefix `todo#` | **O(log M)** |

---

## Explicit non-goals (SQL)

LastDB does **not** require or promise:

- `SELECT * FROM table`
- Full-table scan + predicate pushdown
- JOINs as a storage primitive
- Secondary indexes for arbitrary fields as a general SQL feature. Multi-key same-product fields fold via protein. Thin projections may still be app dual-write.
- Field-equality filters that ignore key layout

---

## Agent / implementer checklist

```text
[ ] Query is point (O(1)) or hash-scoped range (O(log M))
[ ] Hash partition is known for every range query
[ ] No full scan, no scan-then-filter, no unscoped enumerate-all
[ ] Secondary list shapes use a second keyed schema (protein fold for shared fields; dual-write only a thin projection)
[ ] Field count not used as a scaling variable in analysis
```

---

## Related

| Doc | Role |
|-----|------|
| `docs/lastdb-agent-access-model.md` | Agent day-to-day access model (aligned to this requirement) |
| `docs/lastdb-storage-schema-to-atoms.html` | Storage depth (molecules, tips, atoms) |
| Brain | `requirement-lastdb-access-complexity` · `concepts-lastdb-agent-access-model` |

---

## Framing

> LastDB is **Dynamo-shaped**. **Get = O(1). Range under a hash = O(log M). No scan.**  
> Design the key. Dual-write other access patterns. This is not SQL.
