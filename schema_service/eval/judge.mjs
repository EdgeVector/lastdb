// LLM-as-judge: a second opinion on each canonicalization decision, on the
// three criteria. Complements the deterministic label-based scorer — it catches
// subtler issues (a bad/over-generic name, a wrong-but-same-concept merge, lost
// fields) that labels alone miss, and it explains WHY, which feeds the
// improvement-card routine.

import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { callAnthropic } from "./propose.mjs";

function buildJudgePrompt({ input, proposal, decision, resolvedTo, canonicalFields, fieldRenames, siblings }) {
  const renames = fieldRenames && Object.keys(fieldRenames).length ? fieldRenames : null;
  return `You are auditing one decision made by a schema-canonicalization service.

When a user ingests data, the service proposes a schema then decides whether to REUSE an existing canonical schema, EXPAND one, create a NEW canonical, or it REJECTS the proposal (ERROR).

Judge this single decision on three criteria:
1. reuse_ok — did it correctly reuse an existing canonical when an equivalent one existed, and avoid spawning a near-duplicate? (A NEW canonical when an equivalent one already exists = bad. A correct REUSE or a justified NEW = good.)
2. fields_faithful — does the resolved canonical capture the meaningful fields of the input without dropping or inventing them?
3. identity_correct — does it resolve to a canonical whose MEANING matches the input (not a coincidental structural twin, not an unrelated concept)? A REJECT of a sensible input is an identity/usability failure.

INPUT (raw document the user ingested):
${JSON.stringify(input, null, 2)}

PROPOSED SCHEMA (what stage 1 emitted):
${JSON.stringify(proposal, null, 2)}

DECISION: ${decision}
RESOLVED TO CANONICAL: ${resolvedTo}
CANONICAL'S FIELDS (as this input resolved into them — the canonical's field set AT THIS DECISION, not its later end state): ${JSON.stringify(canonicalFields ?? "(unknown / rejected)")}${
    renames
      ? `
FIELD RENAMES the service applied to THIS input (input_field → canonical_field): ${JSON.stringify(renames)}
A rename here means the service judged the two names synonyms and collapsed them onto the canonical label — that is intended de-duplication, NOT a dropped field plus an invented one, so do not penalize fields_faithful for a rename unless the two names mean genuinely different things.`
      : ""
  }
When judging fields_faithful, grade the input against the canonical fields shown ABOVE (the set this input actually resolved into). Do NOT penalize the input for fields that other, later same-concept inputs may add to the shared canonical afterwards — those are not fields THIS input's resolution invented.
OTHER CANONICALS ALREADY CREATED FOR SIMILAR INPUTS: ${JSON.stringify(siblings ?? [])}

Return ONLY JSON:
{"verdict":"good"|"bad","reuse_ok":bool,"fields_faithful":bool,"identity_correct":bool,"rationale":"one sentence","suggested_fix":"one concrete improvement to the service, or empty"}`;
}

function parseVerdict(text) {
  let t = text.trim();
  const fence = t.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) t = fence[1].trim();
  const start = t.indexOf("{");
  const end = t.lastIndexOf("}");
  if (start >= 0 && end > start) t = t.slice(start, end + 1);
  return JSON.parse(t);
}

export async function judgeDecision(ctx, opts = {}) {
  const text = await callAnthropic(buildJudgePrompt(ctx), opts);
  return parseVerdict(text);
}

// Cached judge: verdict is keyed by the decision content (input + proposal +
// decision + resolved canonical). Unchanged decisions across runs are free, so
// the hourly routine only spends tokens when the service's behaviour changes.
async function getOrJudge(ctx, { cacheDir, model } = {}) {
  if (!cacheDir) return judgeDecision(ctx, { model });
  mkdirSync(cacheDir, { recursive: true });
  const key = createHash("sha256")
    .update(
      JSON.stringify({
        i: ctx.input,
        p: ctx.proposal,
        d: ctx.decision,
        r: ctx.resolvedTo,
        f: ctx.canonicalFields,
        // Renames are part of the judge prompt now, so they must key the cache;
        // otherwise a trace with the same resolved fields but different rename
        // context would reuse a stale verdict.
        m: ctx.fieldRenames,
      }),
    )
    .digest("hex")
    .slice(0, 16);
  const f = join(cacheDir, `${key}.json`);
  if (existsSync(f)) return JSON.parse(readFileSync(f, "utf8"));
  const v = await judgeDecision(ctx, { model });
  writeFileSync(f, JSON.stringify(v));
  return v;
}

// Judge a scenario's steps. By default judges only the "interesting" steps
// (errors, NEW-that-might-be-a-dup, reuse misses) to save tokens; pass
// { all: true } to judge every step.
export async function judgeScenario(
  scenario,
  corpus,
  { all = false, model, cacheDir } = {},
) {
  const byId = new Map(corpus.map((c) => [c.id, c]));
  // group corpus ids by concept for sibling context
  const conceptCanonicals = {};
  for (const s of scenario.steps) {
    if (s.decision === "NEW" || s.decision === "EXPANDED")
      (conceptCanonicals[s.concept] ??= []).push(s.resolvedTo);
  }

  const verdicts = [];
  for (const s of scenario.steps) {
    const interesting =
      s.decision === "ERROR" ||
      s.decision === "NEW" ||
      s.decision === "OTHER" ||
      (s.probe && s.probe.unmapped_fields?.length);
    if (!all && !interesting) {
      verdicts.push({ id: s.id, skipped: true });
      continue;
    }
    const item = byId.get(s.id);
    // Field fidelity is judged against a PER-INPUT PROJECTION of the canonical,
    // not its end-of-run superset. `s.resolvedFields` is the canonical's field
    // set as returned for THIS decision (captured in harness.mjs straight off
    // the add-schema response), so it reflects what this input resolved into and
    // excludes fields contributed by LATER same-concept inputs that expanded the
    // shared canonical afterwards — the artifact that made faithful early inputs
    // read as "invented fields." Fall back to the end-state set only when the
    // decision-time set is unavailable (older traces, ERROR steps).
    const canonicalFields =
      s.resolvedFields ?? scenario.finalCanonicals?.[s.resolvedTo];
    const ctx = {
      input: item?.input ?? "(no raw input)",
      proposal: item?.proposal,
      decision: s.decision,
      resolvedTo: s.resolvedTo,
      canonicalFields,
      fieldRenames: s.fieldRenames ?? {},
      siblings: (conceptCanonicals[s.concept] ?? []).filter((n) => n !== s.resolvedTo),
    };
    try {
      const v = await getOrJudge(ctx, { model, cacheDir });
      verdicts.push({ id: s.id, concept: s.concept, decision: s.decision, ...v });
    } catch (e) {
      verdicts.push({ id: s.id, error: e.message });
    }
  }

  const judged = verdicts.filter((v) => v.verdict);
  const bad = judged.filter((v) => v.verdict === "bad");
  return {
    judgedCount: judged.length,
    badCount: bad.length,
    verdicts,
    bad,
  };
}
