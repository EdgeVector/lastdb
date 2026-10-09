// Deterministic local-resolver validation gate.
//
// This is a CI-safe stand-in for deciding whether the local resolver may skip
// schema_service. It compares local reuse decisions against the corpus oracle
// (`expected_concept`) and adversarial near-neighbor fixtures, then reports the
// service-call avoidance / fallback tradeoff the policy would produce.

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

const EVAL_DIR = dirname(fileURLToPath(import.meta.url));

export const POLICY = Object.freeze({
  schemaIntentNameSimilarity: 0.70,
  fieldSimilarity: 0.58,
  minCoverage: 0.75,
  fullRecordCoverage: 1.0,
  ambiguityMargin: 0.08,
  schemaCandidates: 8,
  fieldCandidatesPerSchema: 6,
  componentCandidates: 4,
});

const ADVERSARIAL_FIXTURES = [
  {
    id: "adversarial_trip_seed",
    expected_concept: "trip",
    proposal: {
      descriptive_name: "Trips",
      purpose_statement:
        "Tracks planned travel itineraries, bookings, transportation, and lodging details.",
      fields: ["destination", "departure_time", "arrival_time", "provider", "trip_notes"],
      field_descriptions: {
        destination: "Place the traveler is going",
        departure_time: "Scheduled time leaving the origin",
        arrival_time: "Scheduled time reaching the destination",
        provider: "Airline, train, lodging, or travel vendor",
        trip_notes: "Travel-specific notes about the itinerary",
      },
    },
  },
  {
    id: "adversarial_meeting_note_not_trip",
    expected_concept: "meeting_note",
    proposal: {
      descriptive_name: "Meeting Notes",
      purpose_statement:
        "Captures discussion, decisions, and action items from a meeting.",
      fields: ["title", "notes", "created_at", "attendees"],
      field_descriptions: {
        title: "Meeting title or subject",
        notes: "Detailed notes about discussion and decisions",
        created_at: "When the note was written",
        attendees: "People present for the meeting",
      },
    },
    expectLocalDecision: "fallback",
  },
  {
    id: "adversarial_therapy_note_not_trip",
    expected_concept: "therapy_note",
    proposal: {
      descriptive_name: "Therapy Notes",
      purpose_statement:
        "Records private clinical observations and follow-up guidance from therapy sessions.",
      fields: ["client", "session_date", "note", "follow_up"],
      field_descriptions: {
        client: "Person the session is about",
        session_date: "Date of the therapy appointment",
        note: "Detailed clinical note from the session",
        follow_up: "Recommended next steps",
      },
    },
    expectLocalDecision: "fallback",
  },
  {
    id: "adversarial_travel_note_not_trip",
    expected_concept: "travel_note",
    proposal: {
      descriptive_name: "Travel Notes",
      purpose_statement:
        "Stores free-form planning notes and reflections about travel.",
      fields: ["note_title", "body", "tags", "created_at"],
      field_descriptions: {
        note_title: "Title for the note",
        body: "Free-form note text",
        tags: "Topic labels for the note",
        created_at: "When the note was created",
      },
    },
    expectLocalDecision: "fallback",
  },
];

function loadCorpus(corpusFile) {
  const raw = JSON.parse(readFileSync(join(EVAL_DIR, corpusFile), "utf8"));
  if (!Array.isArray(raw.items)) throw new Error(`${corpusFile} has no items array`);
  return raw.items;
}

function fieldEntries(proposal) {
  const fields = proposal.fields ?? [];
  const descriptions = proposal.field_descriptions ?? {};
  return fields.map((name) => ({
    name,
    text: `${name} ${descriptions[name] ?? ""}`,
  }));
}

function profile(item) {
  const proposal = item.proposal;
  return {
    id: item.id,
    concept: item.expected_concept,
    intentText: [
      proposal.name,
      proposal.descriptive_name,
      proposal.purpose_statement,
    ]
      .filter(Boolean)
      .join(" "),
    fields: fieldEntries(proposal),
  };
}

function validate(items, { precomputedProfiles = null } = {}) {
  const profiles =
    precomputedProfiles ??
    new Map(items.map((item) => [item.id, profile(item)]));
  const registry = [];
  const decisions = [];

  for (const item of items) {
    const local = profiles.get(item.id) ?? profile(item);
    const candidates = registry
      .map((candidate) => scoreCandidate(local, candidate))
      .sort((a, b) => b.score - a.score)
      .slice(0, POLICY.schemaCandidates);
    const best = candidates[0] ?? null;
    const second = candidates[1] ?? null;
    const ambiguous =
      best && second ? best.score - second.score < POLICY.ambiguityMargin : false;
    const reusable =
      best &&
      best.intentSimilarity >= POLICY.schemaIntentNameSimilarity &&
      best.coverage >= POLICY.minCoverage &&
      !ambiguous;
    const falsePositive = Boolean(reusable && best.candidate.concept !== item.expected_concept);
    const decision = {
      id: item.id,
      concept: item.expected_concept,
      localDecision: reusable ? "use_existing" : "fallback",
      serviceDecision: "schema_service_authoritative",
      resolvedTo: reusable ? best.candidate.id : "<schema_service>",
      serviceCallSkipped: Boolean(reusable),
      falsePositive,
      intentSimilarity: round3(best?.intentSimilarity ?? 0),
      fieldCoverage: round3(best?.coverage ?? 0),
      fullRecordCovered: Boolean(best && best.coverage >= POLICY.fullRecordCoverage),
      ambiguityMargin: round3(best && second ? best.score - second.score : 1),
      fallbackReason: reusable
        ? null
        : fallbackReason(best, ambiguous),
    };
    decisions.push(decision);

    if (!reusable && !registry.some((existing) => existing.concept === item.expected_concept)) {
      registry.push(local);
    }
  }

  return summarize(decisions, items.length);
}

function scoreCandidate(local, candidate) {
  const intentSimilarity = textSimilarity(local.intentText, candidate.intentText);
  const fieldScores = local.fields.map((field) =>
    candidate.fields
      .map((other) => textSimilarity(field.text, other.text))
      .sort((a, b) => b - a)
      .slice(0, POLICY.fieldCandidatesPerSchema)[0] ?? 0,
  );
  const covered = fieldScores.filter((score) => score >= POLICY.fieldSimilarity).length;
  const coverage = local.fields.length === 0 ? 1 : covered / local.fields.length;
  return {
    candidate,
    intentSimilarity,
    coverage,
    score: intentSimilarity * 0.55 + coverage * 0.45,
  };
}

function summarize(decisions, total) {
  const skipped = decisions.filter((d) => d.serviceCallSkipped);
  const fallback = decisions.filter((d) => !d.serviceCallSkipped);
  const falsePositives = skipped.filter((d) => d.falsePositive);
  const avgCoverage = skipped.length
    ? skipped.reduce((sum, d) => sum + d.fieldCoverage, 0) / skipped.length
    : 0;
  const fullRecordCoverage = skipped.length
    ? skipped.filter((d) => d.fullRecordCovered).length / skipped.length
    : 0;

  return {
    policy: POLICY,
    summary: {
      records: total,
      before: {
        schemaServiceCalls: total,
        serviceCallAvoidanceRate: 0,
        falsePositives: 0,
      },
      after: {
        schemaServiceCalls: fallback.length,
        serviceCallAvoidanceRate: round3(skipped.length / total),
        fallbackRate: round3(fallback.length / total),
        localDecisions: skipped.length,
        falsePositives: falsePositives.length,
        avgFieldCoverage: round3(avgCoverage),
        fullRecordCoverageRate: round3(fullRecordCoverage),
      },
      pass: falsePositives.length === 0,
    },
    decisions,
    falsePositives,
  };
}

function fallbackReason(best, ambiguous) {
  if (!best) return "no_candidate_schema";
  if (ambiguous) return "ambiguous_candidates";
  if (best.intentSimilarity < POLICY.schemaIntentNameSimilarity)
    return "below_schema_intent_name_similarity";
  if (best.coverage < POLICY.minCoverage) return "insufficient_field_coverage";
  return "needs_live_schema_service";
}

function measureLatency(items) {
  const iterations = 25;
  const precomputed = new Map(items.map((item) => [item.id, profile(item)]));
  const persisted = JSON.stringify([...precomputed.entries()]);

  return {
    importedServiceComputedEmbeddingsMsPerRecord: round3(
      measure(() => validate(items, { precomputedProfiles: precomputed }), iterations) /
        items.length,
    ),
    localPersistedEmbeddingsAfterRestartMsPerRecord: round3(
      measure(() => {
        const restored = new Map(JSON.parse(persisted));
        validate(items, { precomputedProfiles: restored });
      }, iterations) / items.length,
    ),
    localRecomputeFallbackMsPerRecord: round3(
      measure(() => validate(items), iterations) / items.length,
    ),
  };
}

function measure(fn, iterations) {
  const started = performance.now();
  for (let i = 0; i < iterations; i += 1) fn();
  return (performance.now() - started) / iterations;
}

function textSimilarity(a, b) {
  return Math.max(tokenDice(a, b), bigramDice(a, b));
}

function tokenDice(a, b) {
  const left = tokens(a);
  const right = tokens(b);
  if (!left.length && !right.length) return 1;
  const rightCounts = new Map();
  for (const token of right) rightCounts.set(token, (rightCounts.get(token) ?? 0) + 1);
  let overlap = 0;
  for (const token of left) {
    const count = rightCounts.get(token) ?? 0;
    if (count > 0) {
      overlap += 1;
      rightCounts.set(token, count - 1);
    }
  }
  return (2 * overlap) / (left.length + right.length);
}

function bigramDice(a, b) {
  const left = bigrams(normalize(a));
  const right = bigrams(normalize(b));
  if (!left.length && !right.length) return 1;
  const rightCounts = new Map();
  for (const token of right) rightCounts.set(token, (rightCounts.get(token) ?? 0) + 1);
  let overlap = 0;
  for (const token of left) {
    const count = rightCounts.get(token) ?? 0;
    if (count > 0) {
      overlap += 1;
      rightCounts.set(token, count - 1);
    }
  }
  return (2 * overlap) / (left.length + right.length);
}

function tokens(text) {
  return normalize(text)
    .split(" ")
    .filter((token) => token.length > 1);
}

function bigrams(text) {
  const compact = text.replaceAll(" ", "");
  if (compact.length < 2) return compact ? [compact] : [];
  const out = [];
  for (let i = 0; i < compact.length - 1; i += 1) out.push(compact.slice(i, i + 2));
  return out;
}

function normalize(text) {
  return String(text ?? "")
    .toLowerCase()
    .replace(/[_/.-]+/g, " ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

function assertSelfTest(result) {
  if (result.summary.after.falsePositives !== 0) {
    throw new Error(
      `expected zero false positives, got ${result.summary.after.falsePositives}`,
    );
  }
  for (const fixture of ADVERSARIAL_FIXTURES.filter((f) => f.expectLocalDecision)) {
    const decision = result.decisions.find((d) => d.id === fixture.id);
    if (!decision) throw new Error(`missing adversarial decision for ${fixture.id}`);
    if (decision.localDecision !== fixture.expectLocalDecision) {
      throw new Error(
        `${fixture.id} expected ${fixture.expectLocalDecision}, got ${decision.localDecision}`,
      );
    }
  }
}

function printReport(result) {
  const after = result.summary.after;
  const before = result.summary.before;
  console.log("Local schema resolver validation");
  console.log(`records: ${result.summary.records}`);
  console.log(
    `before: schema_service_calls=${before.schemaServiceCalls} avoidance=${before.serviceCallAvoidanceRate}`,
  );
  console.log(
    `after: schema_service_calls=${after.schemaServiceCalls} avoidance=${after.serviceCallAvoidanceRate} fallback=${after.fallbackRate}`,
  );
  console.log(
    `coverage: avg_field=${after.avgFieldCoverage} full_record=${after.fullRecordCoverageRate}`,
  );
  console.log(`false_positives: ${after.falsePositives}`);
  console.log("threshold_recommendation:", JSON.stringify(POLICY));
  console.log(
    "latency_expectation: imported/persisted paths should be progress-free; local recompute fallback should show user-visible progress if it exceeds the app's normal write spinner window.",
  );
}

async function main() {
  const corpusFile =
    process.argv.find((arg) => arg.startsWith("--corpus="))?.split("=")[1] ??
    "corpus_generated_64.json";
  const selfTest = process.argv.includes("--self-test");
  const corpus = loadCorpus(corpusFile);
  const items = [...corpus, ...ADVERSARIAL_FIXTURES];
  const result = validate(items);
  result.latency = measureLatency(items);
  result.adversarialFixtureIds = ADVERSARIAL_FIXTURES.map((item) => item.id);

  if (selfTest) assertSelfTest(result);
  printReport(result);
  console.log(`latency_ms_per_record: ${JSON.stringify(result.latency)}`);

  const outDir = join(EVAL_DIR, "results");
  mkdirSync(outDir, { recursive: true });
  writeFileSync(
    join(outDir, "local_resolver_validation.json"),
    JSON.stringify(result, null, 2),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
