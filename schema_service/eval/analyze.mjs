// Turn an eval result (results/latest.json) into a small set of stable
// improvement FINDINGS. Each finding has a deterministic slug so the routine
// UPSERTS one living fkanban card per theme (refreshed with the latest
// evidence) instead of spawning duplicates every hour.
//
// Themes are the recurring schema-service weaknesses this harness exists to
// catch: duplicate canonicals, wrong-concept merges, persona-seed type
// collisions, missed reuse, and the LLM judge's clustered recommendations
// (semantic-overlap-before-NEW, field-overlap-before-REUSE, hierarchical /
// per-field canonicalization a.k.a. "schema splitting").
//
// Each finding's body is an AGENT-READY brief, not a bare report: it opens with
// the fkanban-agent header + Repo/Base/Branch, then GOAL / EVIDENCE / STEPS /
// VERIFY / DONE WHEN / OUT OF SCOPE. The hourly routine UPSERTS one living card
// per slug, so the `fkanban-pickup`/`fkanban-agent` loop can drive each one to a
// merged PR with NO human rewrite.

// The shared VERIFY oracle for every schema finding: re-run the harness against
// the ephemeral, isolated schema_service (NEVER the :9001 brain, never prod) and
// confirm the finding's metric moved.
const VERIFY_RUN = "cd fold/schema_service/eval && node run.mjs --stage1 --judge --baseline=seeded";

// Wrap a finding's evidence in a self-contained, agent-ready brief.
//   slug         — the stable card slug (drives the suggested branch name)
//   goal         — one-paragraph GOAL
//   evidence     — array of markdown evidence lines (latest eval run)
//   steps        — array of STEPS lines (the implementation plan)
//   verifyMetric — plain-English assertion the VERIFY run must satisfy
//   doneWhen     — DONE WHEN line(s)
//   outOfScope   — OUT OF SCOPE line(s)
function agentBrief({ slug, goal, evidence, steps, verifyMetric, doneWhen, outOfScope }) {
  return [
    "**Follow the fkanban-agent skill — drive this through to a MERGED PR. A card is only `done` when its code is actually in the repo.**",
    "",
    "Repo: EdgeVector/fold",
    "Base: main",
    `Branch: fkanban/${slug}`,
    "",
    "## GOAL",
    goal,
    "",
    "## EVIDENCE (latest eval run)",
    ...evidence,
    "",
    "## STEPS",
    ...steps,
    "",
    "## VERIFY",
    "The schema-eval harness is the oracle. It runs against an EPHEMERAL, isolated schema_service — NEVER the :9001 brain, never prod. Run it dev-only:",
    "",
    "```",
    VERIFY_RUN,
    "```",
    "",
    verifyMetric,
    "",
    "## DONE WHEN",
    doneWhen,
    "",
    "## OUT OF SCOPE",
    outOfScope,
  ].join("\n");
}

export function analyze(results) {
  const findings = [];
  const score = results.scores?.[0]; // original order
  const scenario = results.scenarios?.[0];
  if (!score || !scenario) return findings;

  // --- dup explosion ---
  const dup = score.detail.dupExplosion ?? {};
  const dupConcepts = Object.entries(dup).filter(([, v]) => v.extra > 0);
  if (dupConcepts.length) {
    const evidence = dupConcepts.map(([c, v]) => {
      const names = [
        ...new Set(
          scenario.steps
            .filter((s) => s.concept === c && s.decision !== "ERROR")
            .map((s) => s.resolvedTo),
        ),
      ];
      return `- **${c}** → ${v.distinctCanonicals} canonicals: ${names.join(", ")}`;
    });
    findings.push({
      slug: "schema-eval-dup-explosion",
      title: `Schema canonicalization: ${dupConcepts.length} concept(s) fragment into duplicate canonicals`,
      severity: "high",
      tags: ["schema-eval", "auto", "canonicalization"],
      body: agentBrief({
        slug: "schema-eval-dup-explosion",
        goal: "Semantically-equivalent inputs are producing **separate canonical schemas** instead of collapsing to one. This is registry bloat — apps built against one canonical miss data stored under its near-duplicate. Drive the dup-explosion metric to zero by making Stage-2 reuse an existing canonical on strong semantic overlap.",
        evidence,
        steps: [
          "- The fix lives in `schema_service` Stage-2 canonicalization: add a semantic-overlap reuse check BEFORE creating a NEW canonical (compare proposed descriptive_name + purpose + field embeddings against existing canonicals; reuse / EXPAND on a strong match).",
          "- Implementation home is the standing card **`schema-canon-purpose-aware-matching`** (purpose-gated merge + reuse-before-new) — land the reuse-before-NEW half there; do not restate the whole fix here.",
          "- Per-field canonicalization first (canonicalize field meanings, then group schemas by canonical-field signature) would also reduce surface-name sensitivity.",
        ],
        verifyMetric:
          "Assert the metric improved: `dupExplosion` shows **extra == 0** for the previously-fragmented concept(s) — they now collapse to a single canonical.",
        doneWhen:
          "PR merged into main; a fresh harness run reports `dupExplosion extra == 0` (no concept fragments into duplicate canonicals).",
        outOfScope:
          "Changing what the harness measures. Touching the :9001 brain or any prod surface — the eval is ephemeral + dev-only.",
      }),
    });
  }

  // --- wrong-concept merges ---
  const wrong = score.detail.wrongMerges ?? [];
  if (wrong.length) {
    findings.push({
      slug: "schema-eval-wrong-merge",
      title: `Schema canonicalization: ${wrong.length} wrong-concept merge(s)`,
      severity: "high",
      tags: ["schema-eval", "auto", "canonicalization", "correctness"],
      body: agentBrief({
        slug: "schema-eval-wrong-merge",
        goal: "Inputs are merging into canonicals of a **different concept** (a structural/embedding twin), corrupting semantic identity. Drive wrong-concept merges to zero by requiring purpose/concept agreement before Stage-2 merges.",
        evidence: wrong.map(
          (w) =>
            `- \`${w.id}\` (concept **${w.concept}**) merged into **${w.mergedInto}** (a **${w.mergedConcept}** canonical)`,
        ),
        steps: [
          "- The fix lives in `schema_service` Stage-2 canonicalization: require name/purpose (concept) agreement — not just field-structure / embedding similarity — before merging into an existing canonical.",
          "- Implementation home is the standing card **`schema-canon-purpose-aware-matching`** (purpose-gated merge + reuse-before-new) — land the purpose-gated-merge half there; do not restate the whole fix here.",
          "- Concretely: raise the purpose-signal threshold for cross-concept merges, so a structural twin of a different concept resolves to its own canonical (NEW/EXPAND) rather than merging.",
        ],
        verifyMetric:
          "Assert the metric improved: the eval reports **wrongMerges == 0** — no input merges into a canonical of a different concept.",
        doneWhen:
          "PR merged into main; a fresh harness run reports `wrongMerges == 0`.",
        outOfScope:
          "Changing what the harness measures. Touching the :9001 brain or any prod surface — the eval is ephemeral + dev-only.",
      }),
    });
  }

  // --- reuse-maximization: reuse_rate below target OR mega-schema growth ---
  // The headline reuse number and the mega-schema-growth proxy. Fires when the
  // service is re-minting canonicals instead of reusing/composing existing ones
  // (reuse_rate under REUSE_RATE_TARGET), or when a single canonical's field
  // count balloons past MEGA_SCHEMA_FIELD_CEILING (a record that should have
  // decomposed across components is being re-inlined whole). Also surfaces any
  // corpus item LABELED to decompose (`expected_decompose`) that did not.
  const REUSE_RATE_TARGET = 0.8;
  const MEGA_SCHEMA_FIELD_CEILING = 12;
  const sum = score.summary ?? {};
  const reuseRate = sum.reuse_rate;
  const maxFields = sum.max_canonical_fields ?? 0;
  const decomposeMisses = score.detail.decomposeMisses ?? [];
  const reuseBelow = reuseRate != null && reuseRate < REUSE_RATE_TARGET;
  const megaGrowth = maxFields > MEGA_SCHEMA_FIELD_CEILING;
  if (reuseBelow || megaGrowth || decomposeMisses.length) {
    const fattest = Object.entries(scenario.finalCanonicals ?? {})
      .map(([name, fields]) => [name, Array.isArray(fields) ? fields.length : 0])
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);
    const evidence = [];
    if (reuseRate != null)
      evidence.push(
        `- **reuse_rate = ${reuseRate}** (target ≥ ${REUSE_RATE_TARGET}) — reused-or-composed / eligible. ${reuseBelow ? "BELOW target." : "at/above target."}`,
      );
    evidence.push(
      `- **max_canonical_fields = ${maxFields}** (ceiling ${MEGA_SCHEMA_FIELD_CEILING}), avg ${sum.avg_fields_per_canonical} — ${megaGrowth ? "a canonical has ballooned past the ceiling." : "within ceiling."}`,
    );
    evidence.push(`- **composed = ${sum.composed}** (count ${sum.composed_count}).`);
    if (fattest.length)
      evidence.push(
        "- Fattest canonicals: " +
          fattest.map(([n, c]) => `\`${n}\` (${c} fields)`).join(", "),
      );
    if (decomposeMisses.length)
      evidence.push(
        ...decomposeMisses.map(
          (m) =>
            `- \`${m.id}\` (concept **${m.concept}**) should decompose across [${m.expectedComponents.join(", ")}] but registered as **${m.decision}** — re-inlined instead of reusing the existing component canonicals.`,
        ),
      );
    findings.push({
      slug: "schema-eval-reuse-below-target",
      title: `Schema canonicalization: reuse-maximization below target (reuse_rate ${reuseRate ?? "n/a"}, max canonical ${maxFields} fields)`,
      severity: reuseBelow || megaGrowth ? "high" : "medium",
      tags: ["schema-eval", "auto", "canonicalization", "reuse"],
      body: agentBrief({
        slug: "schema-eval-reuse-below-target",
        goal: "The registry is growing by **re-creating / re-inlining** canonicals rather than **reusing or composing** existing ones — the reuse_rate is under target and/or a canonical's field count has ballooned (a record that should split across existing components is stored as one mega-schema). Drive reuse_rate up and mega-schema growth down by reusing existing canonicals and decomposing composite records across the components that already exist (the compositional reuse path).",
        evidence,
        steps: [
          "- The fix lives in `schema_service` Stage-2 canonicalization + the **compositional decompose/apply** path (`state_compositional`): when a proposal's components match existing canonicals strongly, reuse them via typed `SchemaRef`s (`ref_fields`) and register a COMPOSED canonical instead of re-inlining every field.",
          "- Implementation home is the standing card **`schema-decompose-apply-path`** — land the apply (decompose-and-reuse) half there; this eval card only MEASURES it.",
          "- Keep reuse-before-NEW (dup-explosion) and purpose-gated merges intact; composition is additive and off by default until the apply step is enabled.",
        ],
        verifyMetric:
          `Assert the metric improved: \`reuse_rate\` rises toward ≥ ${REUSE_RATE_TARGET}, \`max_canonical_fields\` drops at/under ${MEGA_SCHEMA_FIELD_CEILING}, and the \`expected_decompose\` corpus items register as **COMPOSED** (composed count > 0) rather than re-inlined NEW canonicals.`,
        doneWhen:
          "PR merged into main; a fresh harness run reports a higher reuse_rate (and/or the previously-ballooned canonical back under the field ceiling), with the composite corpus items composing across existing components.",
        outOfScope:
          "Changing what the harness measures. Touching the :9001 brain or any prod surface — the eval is ephemeral + dev-only. (The apply path itself is `schema-decompose-apply-path`, not this card.)",
      }),
    });
  }

  // --- persona-seed cross-type collisions (409) ---
  const errors = (scenario.steps ?? []).filter((s) => s.decision === "ERROR");
  const typeCollisions = errors.filter((s) =>
    JSON.stringify(s.error ?? "").includes("cross-schema_type"),
  );
  if (typeCollisions.length) {
    const byName = {};
    for (const s of typeCollisions) {
      const dn = s.error?.descriptive_name ?? "?";
      (byName[dn] ??= []).push(s.id);
    }
    findings.push({
      slug: "schema-eval-persona-seed-type-collision",
      title: `Ingestion blocked: ${typeCollisions.length} input(s) 409 against incompatible-type persona seeds`,
      severity: "high",
      tags: ["schema-eval", "auto", "ingestion", "seeds"],
      body: agentBrief({
        slug: "schema-eval-persona-seed-type-collision",
        goal: "Real LLM-proposed schemas are being **rejected (409)** because their descriptive_name collides with a persona/starter seed of an incompatible `schema_type` (e.g. a `Hash`/`Single` contacts proposal vs the `Range` **Contacts** persona seed). In production this means the node **fails to ingest** these documents at all. Drive these 409s to zero by handling the cross-type collision in ingestion.",
        evidence: Object.entries(byName).map(
          ([dn, ids]) => `- name **${dn}**: rejected inputs ${ids.map((i) => `\`${i}\``).join(", ")}`,
        ),
        steps: [
          "- Fix the ingestion **409 handling** in `schema_service`: a descriptive_name collision against an incompatible-`schema_type` seed must not hard-fail the document.",
          "- Pick one (in preference order): allow cross-type expansion when molecule-read-safe; OR have canonicalization fall back to a new descriptive_name instead of a hard 409; OR choose persona-seed schema_types that match what the ingestion LLM naturally proposes.",
        ],
        verifyMetric:
          "Assert the metric improved: the previously-rejected inputs above now ingest — the eval reports **0 `cross-schema_type` ERROR steps** for these descriptive_names.",
        doneWhen:
          "PR merged into main; a fresh harness run shows the listed inputs ingest (no `cross-schema_type` 409 ERROR steps).",
        outOfScope:
          "Changing what the harness measures. Touching the :9001 brain or any prod surface — the eval is ephemeral + dev-only.",
      }),
    });
  }

  // --- judge recommendations (clustered) ---
  const bad = results.judgement?.bad ?? [];
  if (bad.length) {
    const fixes = bad
      .map((b) => b.suggested_fix)
      .filter(Boolean)
      .slice(0, 12);
    findings.push({
      slug: "schema-eval-judge-recommendations",
      title: `LLM judge: ${bad.length} decision(s) rated BAD — clustered fixes`,
      severity: bad.length >= 5 ? "high" : "medium",
      tags: ["schema-eval", "auto", "judge"],
      body: agentBrief({
        slug: "schema-eval-judge-recommendations",
        goal: `The LLM-as-judge rated **${bad.length}/${results.judgement.judgedCount}** canonicalization decisions BAD on reuse/field-fidelity/identity. Drive the judge bad-count down by enforcing field fidelity: Stage-2 must not invent fields on REUSE.`,
        evidence: [
          "Recurring suggested fixes from the judge:",
          "",
          ...fixes.map((f, i) => `${i + 1}. ${f}`),
        ],
        steps: [
          "- Field-fidelity: Stage-2 must **not invent fields**. On a REUSE decision, validate that the input's fields are a SUBSET of the canonical's fields; if they are not, choose EXPAND (add the new fields) or NEW (distinct concept) instead of silently reusing.",
          "- The fix lives in `schema_service` Stage-2 canonicalization (the REUSE/EXPAND/NEW decision path).",
        ],
        verifyMetric:
          "Assert the metric improved: the judge **bad-count is lower** than the latest run above (fewer decisions rated BAD on field-fidelity / reuse).",
        doneWhen:
          "PR merged into main; a fresh harness run reports a reduced judge bad-count (no field-invention on REUSE).",
        outOfScope:
          "Changing what the harness measures or the judge rubric. Touching the :9001 brain or any prod surface — the eval is ephemeral + dev-only.",
      }),
    });
  }

  return findings;
}

// A compact one-line summary per finding for the routine log.
export function summarize(findings) {
  if (!findings.length) return "no findings — registry is clean this run";
  return findings.map((f) => `[${f.severity}] ${f.slug}: ${f.title}`).join("\n");
}
