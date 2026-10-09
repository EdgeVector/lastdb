// Deterministic Stage-2 scorer. Judges a replay trace on the three criteria:
//   1. Correct reuse / no dup explosion
//   2. Faithful field capture
//   3. Right semantic identity (no wrong-merge)
// No LLM required — grounded in the expected_concept labels on the corpus.

export function scoreScenario(scenario, corpus) {
  const byId = new Map(corpus.map((c) => [c.id, c]));
  const concepts = [...new Set(corpus.map((c) => c.expected_concept))];

  // canonical -> the concept of whichever proposal first created it.
  const canonicalConcept = new Map();
  for (const s of scenario.steps) {
    if (s.decision === "NEW" || s.decision === "EXPANDED" || s.decision === "COMPOSED") {
      if (!canonicalConcept.has(s.resolvedTo))
        canonicalConcept.set(s.resolvedTo, s.concept);
    }
  }

  // --- 1. dup explosion: distinct canonicals per concept (ideal = 1) ---
  const SENTINELS = new Set(["<none>", "<rejected>"]);
  const canonicalsByConcept = new Map(concepts.map((c) => [c, new Set()]));
  for (const s of scenario.steps) {
    if (s.decision === "ERROR") continue; // a rejected proposal is not a canonical
    if (s.resolvedTo && !SENTINELS.has(s.resolvedTo))
      canonicalsByConcept.get(s.concept)?.add(s.resolvedTo);
  }
  const dupExplosion = {};
  let extraCanonicals = 0;
  for (const c of concepts) {
    const n = canonicalsByConcept.get(c).size;
    dupExplosion[c] = { distinctCanonicals: n, extra: Math.max(0, n - 1) };
    extraCanonicals += Math.max(0, n - 1);
  }

  // --- 2. reuse correctness: 2nd+ proposal of a concept should NOT be NEW ---
  // REUSED / EXPANDED / COMPOSED all count as correctly avoiding a fresh dup:
  // COMPOSED means the proposal decomposed and reused existing canonicals via
  // typed SchemaRefs rather than spawning a near-duplicate.
  const seenConcept = new Set();
  let reuseOk = 0;
  let reuseTotal = 0;
  const reuseMisses = [];
  for (const s of scenario.steps) {
    if (seenConcept.has(s.concept)) {
      reuseTotal++;
      if (s.decision === "REUSED" || s.decision === "EXPANDED" || s.decision === "COMPOSED")
        reuseOk++;
      else reuseMisses.push({ id: s.id, concept: s.concept, decision: s.decision });
    }
    seenConcept.add(s.concept);
  }

  // --- 2c. decomposition eligibility: items the corpus labels as SHOULD split
  // across 2+ pre-existing canonicals (`expected_decompose: [conceptA, ...]`).
  // Such a record ought to register as COMPOSED, reusing each named component
  // canonical via a SchemaRef instead of re-inlining a mega-schema. Today, with
  // the compositional apply path off, these score as NEW (a decomposition miss),
  // which is the whole point — the metric quantifies how far reuse-maximization
  // has to go. `composedExpected` is the denominator, `composedOk` the numerator.
  let composedOk = 0;
  let composedExpected = 0;
  const decomposeMisses = [];
  for (const s of scenario.steps) {
    const item = byId.get(s.id);
    const want = item?.expected_decompose;
    if (Array.isArray(want) && want.length >= 2) {
      composedExpected++;
      if (s.decision === "COMPOSED") composedOk++;
      else
        decomposeMisses.push({
          id: s.id,
          concept: s.concept,
          decision: s.decision,
          expectedComponents: want,
        });
    }
  }

  // composed_count: how many steps actually composed (reused existing canonicals
  // via SchemaRef). Stays 0 until the compositional apply path is enabled.
  const composedCount = scenario.steps.filter((s) => s.decision === "COMPOSED").length;

  // --- 3. semantic identity: a reuse must land on a same-concept canonical ---
  const wrongMerges = [];
  for (const s of scenario.steps) {
    if (s.decision === "REUSED" || s.decision === "EXPANDED") {
      const landedConcept = canonicalConcept.get(s.resolvedTo);
      if (landedConcept && landedConcept !== s.concept)
        wrongMerges.push({
          id: s.id,
          concept: s.concept,
          mergedInto: s.resolvedTo,
          mergedConcept: landedConcept,
        });
    }
  }

  // --- 2b. field fidelity: final canonical for a concept should retain the
  // union of its members' fields (probe unmapped_fields also flags drops). ---
  const fieldFidelity = {};
  for (const c of concepts) {
    const members = corpus.filter((m) => m.expected_concept === c);
    const wantUnion = new Set(members.flatMap((m) => m.proposal.fields));
    // Resolve the canonical(s) this concept landed on and union their fields.
    const landed = canonicalsByConcept.get(c);
    const haveUnion = new Set();
    for (const name of landed) {
      for (const f of scenario.finalCanonicals[name] ?? []) haveUnion.add(f);
    }
    // Only meaningful when the canonical is user-visible (skip seed merges).
    const missing = landed.size
      ? [...wantUnion].filter((f) => !haveUnion.has(f) && !renamedAway(f, members))
      : [];
    fieldFidelity[c] = {
      wanted: [...wantUnion],
      present: [...haveUnion],
      missing,
    };
  }

  const dupExplosionTotal = extraCanonicals;
  const fieldDrops = Object.values(fieldFidelity).reduce(
    (a, f) => a + f.missing.length,
    0,
  );
  const errors = scenario.steps
    .filter((s) => s.decision === "ERROR")
    .map((s) => ({ id: s.id, status: s.status, error: s.error }));

  // --- HEADLINE reuse-maximization metrics ---------------------------------
  // reuse_rate: the single "are we reusing rather than re-creating?" number.
  // numerator = every step that AVOIDED minting a fresh duplicate canonical
  // (REUSED into an existing canonical/seed, EXPANDED one, or COMPOSED from
  // existing ones); denominator = every eligible step (a successful, non-error
  // registration where reuse was even possible — i.e. NOT the first proposal
  // that necessarily creates the first canonical of its concept). Higher = the
  // registry grows by reuse, not by bloat. Range [0,1]; null when nothing is
  // eligible.
  const seenForRate = new Set();
  let reuseNum = 0;
  let reuseDen = 0;
  for (const s of scenario.steps) {
    if (s.decision === "ERROR") continue;
    const firstOfConcept = !seenForRate.has(s.concept);
    seenForRate.add(s.concept);
    if (firstOfConcept) continue; // the seeding NEW is not a reuse opportunity
    reuseDen++;
    if (
      s.decision === "REUSED" ||
      s.decision === "EXPANDED" ||
      s.decision === "COMPOSED"
    )
      reuseNum++;
  }
  const reuseRate = reuseDen ? round2(reuseNum / reuseDen) : null;

  // mega-schema-growth proxy: how fat are the canonicals this corpus produced?
  // A canonicalizer that over-merges (or never decomposes) grows a few giant
  // schemas; reuse-maximization should keep canonicals lean by referencing
  // shared components instead of re-inlining their fields.
  const fieldCounts = Object.values(scenario.finalCanonicals ?? {}).map(
    (fields) => (Array.isArray(fields) ? fields.length : 0),
  );
  const maxCanonicalFields = fieldCounts.length ? Math.max(...fieldCounts) : 0;
  const avgFieldsPerCanonical = fieldCounts.length
    ? round2(fieldCounts.reduce((a, n) => a + n, 0) / fieldCounts.length)
    : 0;

  return {
    label: scenario.label,
    order: scenario.order,
    summary: {
      concepts: concepts.length,
      distinctCanonicalsTotal: Object.values(canonicalsByConcept).length,
      dupExplosion: dupExplosionTotal,
      reuse: `${reuseOk}/${reuseTotal}`,
      // headline metrics (see comments above)
      reuse_rate: reuseRate,
      avg_fields_per_canonical: avgFieldsPerCanonical,
      max_canonical_fields: maxCanonicalFields,
      composed_count: composedCount,
      composed: `${composedOk}/${composedExpected}`,
      wrongMerges: wrongMerges.length,
      fieldDrops,
      errors: errors.length,
      pass:
        errors.length === 0 &&
        dupExplosionTotal === 0 &&
        wrongMerges.length === 0 &&
        reuseOk === reuseTotal,
    },
    detail: {
      dupExplosion,
      reuseMisses,
      decomposeMisses,
      wrongMerges,
      fieldFidelity,
      errors,
    },
  };
}

function round2(n) {
  return Math.round(n * 100) / 100;
}

// Field renames are expected (field_rename_map), so a "missing" field that was
// clearly renamed into the canonical shouldn't count as a drop. v1 heuristic:
// if any member proposal carried it, but the canonical uses a different label,
// the probe's unmapped_fields already captured fidelity — keep v1 simple and
// only flag a true absence. (Hook for a smarter rename-aware check later.)
function renamedAway() {
  return false;
}

// Compare final canonical sets across orderings -> order-sensitivity report.
export function confluence(scenarios) {
  const signature = (sc) =>
    Object.keys(sc.finalCanonicals).sort().join(" | ") || "<empty>";
  const sigs = scenarios.map((s) => ({ label: s.label, sig: signature(s) }));
  const distinct = new Set(sigs.map((s) => s.sig));
  return {
    converges: distinct.size === 1,
    distinctOutcomes: distinct.size,
    perOrder: sigs,
  };
}
