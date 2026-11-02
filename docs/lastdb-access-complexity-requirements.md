# LastDB access complexity requirements

Use named alternate keys on one schema for the same records.
Keep unrelated records in separate schemas. Canonical field reuse supplies definitions and types.
See [the multi-key schema guide](multi-key-schemas.md).

## One-sentence rule

LastDB supports **keyed access only**: point get by hash (and optional exact range), and **ordered range under a single hash**. There is **no full scan**. There is **no SQL-style table scan or ad-hoc field filter as a query plan**.

A field definition does not connect mutable values across schemas. Each alternate key addresses the source schema records.

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
  One known key → one constant-time record read (relative to database size). Table growth does not make a point get slower in the access model.

- **Range under a hash = O(log M)**  
  Range work is over the **ordered range-key set of that partition only**, size **M**. Complexity is logarithmic in **partition** size, not in global table size **N**.  
  You must supply the **hash** (partition). A “range” without a hash is not a supported access pattern.

- **Multi-get exact keys = O(K)**  
  K known `(hash, range)` pairs → K point gets, whatever partitions they span. This is the only bounded shape that is **not** confined to one hash. The batch must never cost more than reading the partitions it names; if it does, the caller is better off not using it, which defeats the operation.

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

For another list of the same records, declare a named alternate key with a hash and a suitable range.

---

## Access-pattern design (required)

1. **Name the query** before the schema (point by id, list by board, list by column, …).
2. **Primary schema** serves the truth key (usually Hash by slug/id) → **O(1)** get.
3. **Other reads of the same records** use named alternate keys on the source schema.
4. **Range keys** are designed so useful lists are **prefix or between under one hash** → **O(log M)**.
5. Never “we’ll scan and filter later.”

### Example (fkanban)

| Intent | Schema | Complexity |
|--------|--------|------------|
| Card by slug | `Card` HashKey(slug) | **O(1)** |
| Cards on a board | Card named key `by_board`, hash=board, range=`column#position#slug` | **O(log M)** for that board’s partition |
| One column | same + range prefix `todo#` | **O(log M)** |

---

## Explicit non-goals (SQL)

LastDB does **not** require or promise:

- `SELECT * FROM table`
- Full-table scan + predicate pushdown
- JOINs as a storage primitive
- An arbitrary field predicate without a declared key. Named alternate keys still require explicit hash and range coordinates.
- Field-equality filters that ignore key layout

---

## Agent / implementer checklist

```text
[ ] Query is point (O(1)) or hash-scoped range (O(log M))
[ ] Hash partition is known for every range query
[ ] No full scan, no scan-then-filter, no unscoped enumerate-all
[ ] Other reads of the same records use named alternate keys on the source schema
[ ] Field count not used as a scaling variable in analysis
```

---

## Related

| Doc | Role |
|-----|------|
| `docs/lastdb-agent-access-model.md` | Agent day-to-day access model (aligned to this requirement) |
| `docs/lastdb-canonical-model.md` | The ladder: schema, record, file blob; the write path, delete and purge |

---

## Framing

> LastDB is **Dynamo-shaped**. **Get = O(1). Range under a hash = O(log M). No scan.**  
> Design the key. Dual-write other access patterns. This is not SQL.
