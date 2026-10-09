# Canonicalization seam unification lock spike

Candidate generation must not hold a write lock across any embedder await. The
candidate pass is therefore read-only: it snapshots candidate schemas and cached
embedding data while holding short-lived read guards, then drops those guards
before the strict dual-signal gate runs. The gate may call the embedder and may
append a shadow near-miss record, so it runs after candidate generation has
returned owned data.

The write boundary stays at the existing mutation points: expansion, persistence,
descriptive-name index update, canonical-field registration, and near-miss
append. Candidate ranking never mutates `schema`, never rewrites fields, and
never updates the index. Field renames and descriptive-name adoption are applied
only after the winning candidate passes its gate.

The final descriptive-name revalidation remains after all async gate work. It
rechecks the namespace-scoped name slot just before new registration and
de-collides a vetoed same-name proposal when the same-name reuse rescue also
declines. That preserves the existing no-write-lock-across-await invariant while
keeping the race guard responsible for concurrent duplicate prevention.
