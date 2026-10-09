// Replay engine: push an ORDERED sequence of schema proposals into a fresh
// registry and record, per step, the canonicalization decision the service
// made. Decision is derived from the user-schema set delta (ground truth),
// cross-checked against the POST status and the non-destructive reuse probe.

// Returns the set of descriptive_names of user-authored canonicals currently
// registered. Falls back to `name` when a schema has no descriptive_name.
async function userCanonicals(client) {
  const res = await client.availableUser();
  const out = new Map(); // descriptive_name -> fields[]
  for (const env of res.body?.schemas ?? []) {
    const key = env.descriptive_name ?? env.name;
    out.set(key, env.fields ?? []);
  }
  return out;
}

function diffKeys(before, after) {
  const added = [...after.keys()].filter((k) => !before.has(k));
  const removed = [...before.keys()].filter((k) => !after.has(k));
  return { added, removed };
}

// A composed canonical is newly registered (added 1 / removed 0, same delta
// shape as NEW) BUT the compositional apply path rewrote one or more of the
// proposal's nested `ref_fields` to reuse an EXISTING canonical via SchemaRef
// instead of re-inlining its fields.
//
// We read the AUTHORITATIVE wire marker — `addBody.composed`, set true only on
// the service's `SchemaAddOutcome::Composed`. This is what distinguishes genuine
// composition from a registration that merely *carries* ref_fields: the response
// schema echoes the (possibly un-rewritten) ref_fields either way, so the
// presence of ref_fields is NOT a reliable signal (a proposal authored with
// ref_fields would look "composed" even with the apply path off). The flag is.
// `composedFrom` lists the canonicals the parent now references, for the trace.
function isComposed(addBody) {
  return addBody?.composed === true;
}

function refTargets(addBody) {
  const rf = addBody?.schema?.ref_fields;
  if (!rf || typeof rf !== "object") return [];
  return Object.values(rf).filter(Boolean);
}

function classifyDecision({ added, removed }, probe, item, after, addBody) {
  // added 1 / removed 0   -> NEW canonical (or COMPOSED if it references existing canonicals)
  // added 1 / removed 1   -> EXPANDED (old superseded by new, a form of merge)
  // added 0 / removed 0   -> REUSED (merged into an existing canonical or seed)
  //
  // `resolvedTo` = WHICH canonical this proposal ended up in. The AUTHORITATIVE
  // source is the add-schema response's own `schema.descriptive_name`: the
  // service tells us the exact canonical the proposal resolved to (after any
  // semantic-name merge, field-overlap expansion, purpose-gated
  // reuse-before-NEW, or cross-schema_type de-collision rename). The
  // before/after key-delta classifies the DECISION shape (NEW vs EXPANDED vs
  // REUSED) but is NOT a reliable source for the *name* of a reuse target: a
  // reuse-before-NEW expansion into a de-collided canonical (e.g. "Contact
  // Records" → "Contacts (Hash)") shows up as added-0/removed-0 because the
  // superseding write reuses the same descriptive_name key, and the
  // non-destructive reuse PROBE — which only does exact/semantic descriptive_name
  // matching and has no purpose signal — can't see purpose-gated reuse, so it
  // either misses or mis-points at a same-named seed. Trusting the probe there
  // made one genuine canonical look like several (a phantom dup-explosion).
  // Reading the resolved name straight off the response fixes the attribution
  // without changing the metric (still: distinct canonicals per concept).
  const resolvedName = addBody?.schema?.descriptive_name ?? null;
  if (added.length === 1 && removed.length === 0) {
    // COMPOSED only when the service reports a Composed outcome (the apply path
    // genuinely reused an existing canonical) — not merely because the proposal
    // carried ref_fields (which an apply-OFF run would echo back unchanged).
    if (isComposed(addBody))
      return {
        decision: "COMPOSED",
        resolvedTo: resolvedName ?? added[0],
        composedFrom: refTargets(addBody),
      };
    return { decision: "NEW", resolvedTo: resolvedName ?? added[0] };
  }
  if (added.length === 1 && removed.length === 1)
    return {
      decision: "EXPANDED",
      resolvedTo: resolvedName ?? added[0],
      superseded: removed[0],
    };
  if (added.length === 0 && removed.length === 0) {
    // resolvedTo: prefer the authoritative resolved name from the response;
    // else the reuse probe's match; else an exact-name dup that already
    // existed; else unknown (likely merged into a schema.org seed).
    const probeMatch = probe?.matched_descriptive_name;
    const exactDup = after.has(item.proposal.descriptive_name)
      ? item.proposal.descriptive_name
      : null;
    return {
      decision: "REUSED",
      resolvedTo: resolvedName ?? probeMatch ?? exactDup ?? "<seed-or-unknown>",
    };
  }
  return { decision: "OTHER", resolvedTo: resolvedName ?? added[0] ?? "<none>", added, removed };
}

// Run one scenario = one ordering of proposal ids against a clean registry.
// `prepare` establishes the baseline: reset (empty) or a restart (seeded).
// Defaults to reset.
export async function runScenario(
  client,
  corpus,
  order,
  { label, prepare } = {},
) {
  if (prepare) await prepare();
  else await client.reset();
  const baseline = await client.listSchemas();
  const baselineCount = baseline.body?.schemas?.length ?? 0;
  // Pre-existing user-tagged schemas (some seeds carry source=user). We measure
  // the corpus DELTA against this so seed noise doesn't pollute the results.
  const baselineUser = new Set((await userCanonicals(client)).keys());

  const byId = new Map(corpus.map((c) => [c.id, c]));
  const steps = [];

  for (const id of order) {
    const item = byId.get(id);
    if (!item) throw new Error(`corpus has no item with id "${id}"`);

    const before = await userCanonicals(client);

    // Non-destructive probe BEFORE the write: what would it reuse, and how
    // faithfully (is_superset / unmapped_fields = field-fidelity signal).
    const probeRes = await client.batchCheckReuse([
      { descriptive_name: item.proposal.descriptive_name, fields: item.proposal.fields },
    ]);
    const probe = probeRes.body?.matches?.[item.proposal.descriptive_name] ?? null;

    const res = await client.addSchema(item.proposal);

    // A rejected proposal (validation/conflict) is NOT a reuse — surface it.
    if (res.status >= 400) {
      steps.push({
        id,
        concept: item.expected_concept,
        decision: "ERROR",
        resolvedTo: "<rejected>",
        status: res.status,
        error: res.body?.error ?? res.body,
        proposalFields: item.proposal.fields,
        probe: null,
      });
      continue;
    }

    const after = await userCanonicals(client);

    const delta = diffKeys(before, after);
    const cls = classifyDecision(delta, probe, item, after, res.body);

    // Per-input field fidelity is judged against a PROJECTION of the resolved
    // canonical, NOT its end-of-run superset (see judge.mjs). The two inputs to
    // that projection are captured here, at decision time, straight off the
    // add-schema response — the only point where the canonical reflects THIS
    // input's contribution and not later inputs' expansions:
    //   - resolvedFields: the resolved canonical's fields AS RETURNED for this
    //     decision (post-merge superset at this moment, before any later input
    //     expands it further). `finalCanonicals[resolvedTo]` instead snapshots
    //     the END state, accumulating fields this input never had — which made
    //     an early, faithful input look like it "invented" fields contributed
    //     by a LATER same-concept input.
    //   - fieldRenames: the input-name → canonical-name renames the service
    //     applied for this input (`mutation_mappers` on the response). Lets the
    //     judge see a synonym collapse (e.g. title → recipe_name) as an explicit
    //     justified rename rather than a dropped + invented pair.
    const resolvedFields = Array.isArray(res.body?.schema?.fields)
      ? res.body.schema.fields
      : null;
    const fieldRenames =
      res.body?.mutation_mappers && typeof res.body.mutation_mappers === "object"
        ? res.body.mutation_mappers
        : {};

    steps.push({
      id,
      concept: item.expected_concept,
      decision: cls.decision,
      resolvedTo: cls.resolvedTo,
      superseded: cls.superseded ?? null,
      composedFrom: cls.composedFrom ?? null,
      status: res.status,
      replaced_schema: res.body?.replaced_schema ?? null,
      proposalFields: item.proposal.fields,
      resolvedFields,
      fieldRenames,
      probe: probe
        ? {
            matched: probe.matched_descriptive_name,
            is_superset: probe.is_superset,
            unmapped_fields: probe.unmapped_fields ?? [],
          }
        : null,
    });
  }

  // Only the canonicals THIS corpus created (delta vs the baseline user set).
  const finalAll = await userCanonicals(client);
  const corpusCanonicals = {};
  for (const [k, v] of finalAll) if (!baselineUser.has(k)) corpusCanonicals[k] = v;
  return {
    label,
    order,
    baselineCount,
    baselineUserCount: baselineUser.size,
    steps,
    finalCanonicals: corpusCanonicals,
  };
}
