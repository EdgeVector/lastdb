// Eval-only component-cover search.
//
// This does not call schema_service or mutate any registry. It asks:
// "If local ingestion can write to one or more existing schemas plus a generic
// residue bucket, how much of each proposed record can be covered?"
//
// The scorer is intentionally cheap/deterministic so we can run many variants
// quickly before promoting any policy into product code.

import { readdirSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { EphemeralSchemaService } from "./server.mjs";

const EVAL_DIR = dirname(fileURLToPath(import.meta.url));
const FOLD_DIR = join(EVAL_DIR, "..", "..");
const SEED_DIR = join(FOLD_DIR, "schema_service/crates/core/data/schema_org/schemas");

const corpusFile =
  process.argv.find((a) => a.startsWith("--corpus="))?.split("=")[1] ??
  "corpus_generated_64.json";
const includeOverlay = process.argv.includes("--overlay-schemaorg-inheritance");
const showExamples = process.argv.includes("--examples");
const useFieldEmbeddings = process.argv.includes("--field-embeddings");

const variants = [
  {
    name: "field-exact",
    minCandidateScore: 0.2,
    minFieldScore: 1.0,
    weights: { intent: 0, name: 0, fieldCoverage: 1, fieldQuality: 0 },
  },
  {
    name: "field-synonym",
    minCandidateScore: 0.34,
    minFieldScore: 0.72,
    weights: { intent: 0, name: 0, fieldCoverage: 0.75, fieldQuality: 0.25 },
  },
  {
    name: "intent-name-fields",
    minCandidateScore: 0.36,
    minFieldScore: 0.68,
    weights: { intent: 0.28, name: 0.18, fieldCoverage: 0.36, fieldQuality: 0.18 },
  },
  {
    name: "greedy-cover",
    minCandidateScore: 0.34,
    minFieldScore: 0.64,
    weights: { intent: 0.2, name: 0.12, fieldCoverage: 0.48, fieldQuality: 0.2 },
  },
  {
    name: "beam-cover",
    minCandidateScore: 0.32,
    minFieldScore: 0.6,
    beam: true,
    weights: { intent: 0.18, name: 0.1, fieldCoverage: 0.52, fieldQuality: 0.2 },
  },
  {
    name: "aggressive-beam",
    minCandidateScore: 0.25,
    minFieldScore: 0.5,
    beam: true,
    weights: { intent: 0.12, name: 0.06, fieldCoverage: 0.58, fieldQuality: 0.24 },
  },
  {
    name: "embedding-beam",
    minCandidateScore: 0.26,
    minFieldScore: 0.46,
    beam: true,
    embedding: true,
    weights: { intent: 0.1, name: 0.06, fieldCoverage: 0.58, fieldQuality: 0.26 },
  },
];

async function main() {
  const corpus = JSON.parse(readFileSync(join(EVAL_DIR, corpusFile), "utf8")).items;
  const schemas = loadSchemas();
  if (includeOverlay) schemas.push(...schemaOrgInheritanceOverlay());

  console.log(`• corpus=${corpusFile} records=${corpus.length}`);
  console.log(`• candidate schemas=${schemas.length}${includeOverlay ? " (with overlay)" : ""}`);

  let embeddingScores = null;
  let svc = null;
  if (useFieldEmbeddings) {
    svc = new EphemeralSchemaService();
    console.log("• field embeddings: building/starting ephemeral schema_service…");
    await svc.build();
    await svc.start();
    try {
      embeddingScores = await computeFieldEmbeddingScores(corpus, schemas, svc.client);
      console.log(`• field embeddings: loaded ${embeddingScores.size} left/right scores`);
    } finally {
      await svc.stop();
    }
  }

  const results = [];
  for (const variant of variants.filter((v) => useFieldEmbeddings || !v.embedding)) {
    const recordResults = corpus.map((item) =>
      coverRecord(item, schemas, variant, embeddingScores),
    );
    const summary = summarize(recordResults);
    results.push({ variant: variant.name, summary, records: recordResults });
    printSummary(variant.name, summary);
  }

  const best = [...results].sort(
    (a, b) =>
      b.summary.field_coverage_rate - a.summary.field_coverage_rate ||
      b.summary.record_full_cover_rate - a.summary.record_full_cover_rate ||
      a.summary.avg_component_count - b.summary.avg_component_count,
  )[0];
  console.log(`\n══ best: ${best.variant}`);
  printTopResidue(best.records);
  if (showExamples) printExamples(best.records);

  const outDir = join(EVAL_DIR, "results");
  mkdirSync(outDir, { recursive: true });
  const outPath = join(outDir, "component-cover-latest.json");
  writeFileSync(
    outPath,
    JSON.stringify({ corpusFile, includeOverlay, useFieldEmbeddings, results }, null, 2),
  );
  console.log(`\n• wrote ${outPath}`);
}

function loadSchemas() {
  const schemas = [];
  for (const file of readdirSync(SEED_DIR)) {
    if (!file.endsWith(".json")) continue;
    const schema = JSON.parse(readFileSync(join(SEED_DIR, file), "utf8"));
    const fields = Array.isArray(schema.fields) ? schema.fields : [];
    if (!fields.length) continue;
    schemas.push(normalizeSchema(schema));
  }
  return schemas;
}

function normalizeSchema(schema) {
  const desc = schema.descriptive_name ?? schema.name;
  const fields = Array.isArray(schema.fields) ? schema.fields : [];
  const fieldDescriptions = schema.field_descriptions ?? {};
  return {
    name: schema.name,
    descriptive_name: desc,
    purpose_statement: schema.purpose_statement ?? desc,
    fields,
    field_descriptions: fieldDescriptions,
    generic_penalty: genericPenalty(desc),
  };
}

function coverRecord(item, schemas, variant, embeddingScores) {
  const proposal = item.proposal;
  const fields = proposal.fields ?? [];
  const candidates = schemas
    .map((schema) => scoreCandidate(item, proposal, schema, variant, embeddingScores))
    .filter((c) => c.score >= variant.minCandidateScore && c.covered.length > 0)
    .sort((a, b) => b.score - a.score);

  const cover = variant.beam
    ? beamCover(fields, candidates, variant)
    : greedyCover(fields, candidates, variant);
  const covered = new Set(cover.flatMap((c) => c.covered.map((m) => m.inputField)));
  const residue = fields.filter((f) => !covered.has(f));
  return {
    id: item.id,
    concept: item.expected_concept,
    descriptive_name: proposal.descriptive_name,
    totalFields: fields.length,
    coveredFields: covered.size,
    fieldCoverage: fields.length ? round2(covered.size / fields.length) : 1,
    fullCover: fields.length > 0 && covered.size === fields.length,
    partialCover: covered.size > 0 && covered.size < fields.length,
    residue,
    components: cover.map((c) => ({
      schema: c.schema.descriptive_name,
      score: round2(c.score),
      covered: c.covered,
    })),
  };
}

function scoreCandidate(item, proposal, schema, variant, embeddingScores) {
  const intent = textSimilarity(
    `${proposal.descriptive_name ?? ""} ${proposal.purpose_statement ?? ""}`,
    `${schema.descriptive_name ?? ""} ${schema.purpose_statement ?? ""}`,
  );
  const name = textSimilarity(proposal.descriptive_name ?? "", schema.descriptive_name ?? "");
  const matches = [];
  for (const inputField of proposal.fields ?? []) {
    const best = bestFieldMatch(
      inputField,
      proposal.field_descriptions?.[inputField],
      schema,
      variant,
      item.id,
      embeddingScores,
    );
    if (best && best.score >= variant.minFieldScore) matches.push(best);
  }

  const fieldCoverage = proposal.fields?.length ? matches.length / proposal.fields.length : 0;
  const fieldQuality = matches.length
    ? matches.reduce((sum, m) => sum + m.score, 0) / matches.length
    : 0;
  const w = variant.weights;
  const raw =
    w.intent * intent +
    w.name * name +
    w.fieldCoverage * fieldCoverage +
    w.fieldQuality * fieldQuality;
  const score = Math.max(0, raw - schema.generic_penalty);
  return { schema, score, intent, name, fieldCoverage, fieldQuality, covered: matches };
}

function bestFieldMatch(inputField, inputDescription, schema, variant, itemId, embeddingScores) {
  let best = null;
  for (const schemaField of schema.fields) {
    const score = fieldSimilarity(
      inputField,
      inputDescription,
      schemaField,
      schema.field_descriptions?.[schemaField],
      itemId,
      schema.descriptive_name,
      embeddingScores,
      variant.embedding,
    );
    if (!best || score > best.score) {
      best = {
        inputField,
        schemaField,
        score,
        match: score === 1 ? "exact" : score >= 0.9 ? "synonym" : "semantic",
      };
    }
  }
  return best;
}

function greedyCover(fields, candidates, variant) {
  const uncovered = new Set(fields);
  const chosen = [];
  for (let round = 0; round < 4 && uncovered.size; round++) {
    let best = null;
    for (const c of candidates) {
      const newly = c.covered.filter((m) => uncovered.has(m.inputField));
      if (!newly.length) continue;
      const marginal =
        newly.length / fields.length +
        0.12 * c.score -
        0.04 * chosen.length -
        0.02 * c.schema.generic_penalty;
      if (!best || marginal > best.marginal) best = { ...c, covered: newly, marginal };
    }
    if (!best) break;
    chosen.push(best);
    for (const m of best.covered) uncovered.delete(m.inputField);
  }
  return chosen;
}

function beamCover(fields, candidates, variant) {
  let beam = [{ chosen: [], covered: new Set(), score: 0 }];
  for (let depth = 0; depth < 4; depth++) {
    const next = [...beam];
    for (const state of beam) {
      for (const c of candidates.slice(0, 80)) {
        const newly = c.covered.filter((m) => !state.covered.has(m.inputField));
        if (!newly.length) continue;
        const covered = new Set(state.covered);
        for (const m of newly) covered.add(m.inputField);
        const score =
          covered.size / fields.length +
          0.18 * average([...state.chosen, c].map((x) => x.score)) -
          0.05 * state.chosen.length;
        next.push({
          chosen: [...state.chosen, { ...c, covered: newly }],
          covered,
          score,
        });
      }
    }
    beam = next.sort((a, b) => b.score - a.score).slice(0, 12);
  }
  return beam[0].chosen;
}

function summarize(records) {
  const totalRecords = records.length;
  const totalFields = records.reduce((sum, r) => sum + r.totalFields, 0);
  const coveredFields = records.reduce((sum, r) => sum + r.coveredFields, 0);
  const residueFields = totalFields - coveredFields;
  return {
    records: totalRecords,
    record_full_cover_rate: rate(records.filter((r) => r.fullCover).length, totalRecords),
    record_partial_cover_rate: rate(records.filter((r) => r.partialCover).length, totalRecords),
    record_no_cover_rate: rate(records.filter((r) => r.coveredFields === 0).length, totalRecords),
    field_coverage_rate: rate(coveredFields, totalFields),
    residue_field_rate: rate(residueFields, totalFields),
    avg_component_count: round2(average(records.map((r) => r.components.length))),
    avg_residue_fields: round2(average(records.map((r) => r.residue.length))),
  };
}

function printSummary(name, s) {
  console.log(
    `${name.padEnd(20)} full=${pct(s.record_full_cover_rate)} partial=${pct(s.record_partial_cover_rate)} no=${pct(s.record_no_cover_rate)} field=${pct(s.field_coverage_rate)} residue=${pct(s.residue_field_rate)} avg_components=${s.avg_component_count}`,
  );
}

function printTopResidue(records) {
  const byField = new Map();
  for (const r of records) for (const f of r.residue) byField.set(f, (byField.get(f) ?? 0) + 1);
  const top = [...byField.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);
  console.log("top residue fields:", top.map(([f, n]) => `${f}:${n}`).join(", ") || "<none>");
}

function printExamples(records) {
  for (const r of records.slice(0, 10)) {
    console.log(`\n${r.id} ${r.descriptive_name} coverage=${pct(r.fieldCoverage)}`);
    for (const c of r.components) {
      console.log(`  ${c.schema} ${c.covered.map((m) => `${m.inputField}->${m.schemaField}`).join(", ")}`);
    }
    if (r.residue.length) console.log(`  residue: ${r.residue.join(", ")}`);
  }
}

function textSimilarity(a, b) {
  const aa = new Set(tokens(a).map(canonicalToken));
  const bb = new Set(tokens(b).map(canonicalToken));
  return jaccard(aa, bb);
}

function fieldSimilarity(aName, aDesc, bName, bDesc, itemId, schemaName, embeddingScores, preferEmbedding) {
  const a = canonicalField(aName);
  const b = canonicalField(bName);
  if (a === b) return 1;
  const embedScore =
    embeddingScores?.get(fieldScoreKey(leftFieldId(itemId, aName), rightFieldId(schemaName, bName))) ??
    null;
  if (preferEmbedding && embedScore != null) {
    if (fieldSynonymGroups.some((g) => g.has(a) && g.has(b))) return Math.max(embedScore, 0.82);
    return embedScore;
  }
  if (fieldSynonymGroups.some((g) => g.has(a) && g.has(b))) return Math.max(0.94, embedScore ?? 0);
  const nameScore = jaccard(new Set(splitField(a)), new Set(splitField(b)));
  const descScore = textSimilarity(aDesc ?? aName, bDesc ?? bName);
  return Math.max(nameScore, descScore * 0.9, embedScore ?? 0);
}

async function computeFieldEmbeddingScores(corpus, schemas, client) {
  const leftById = new Map();
  for (const item of corpus) {
    for (const field of item.proposal.fields ?? []) {
      leftById.set(leftFieldId(item.id, field), {
        id: leftFieldId(item.id, field),
        text: fieldContext(
          field,
          item.proposal.descriptive_name,
          item.proposal.field_descriptions?.[field],
        ),
      });
    }
  }
  const rightById = new Map();
  for (const schema of schemas) {
    for (const field of schema.fields ?? []) {
      rightById.set(rightFieldId(schema.descriptive_name, field), {
        id: rightFieldId(schema.descriptive_name, field),
        text: fieldContext(field, schema.descriptive_name, schema.field_descriptions?.[field]),
      });
    }
  }

  const left = [...leftById.values()];
  const right = [...rightById.values()];
  const scores = new Map();
  const chunkSize = 80;
  for (let i = 0; i < left.length; i += chunkSize) {
    const chunk = left.slice(i, i + chunkSize);
    const res = await client.fieldMatchProbe(chunk, right, 24);
    if (res.status >= 400) {
      throw new Error(`field-match-probe failed: ${res.status} ${JSON.stringify(res.body)}`);
    }
    for (const [leftId, matches] of Object.entries(res.body.matches ?? {})) {
      for (const match of matches) scores.set(fieldScoreKey(leftId, match.id), match.score);
    }
    console.log(`  embedded field chunk ${Math.min(i + chunk.length, left.length)}/${left.length}`);
  }
  return scores;
}

function fieldContext(field, schemaName, description) {
  return description
    ? `the ${field} field of ${schemaName}: ${description}`
    : `the ${field} field of ${schemaName}`;
}

function leftFieldId(itemId, field) {
  return `${itemId}::${field}`;
}

function rightFieldId(schemaName, field) {
  return `${schemaName}::${field}`;
}

function fieldScoreKey(leftId, rightId) {
  return `${leftId}\t${rightId}`;
}

function canonicalField(field) {
  return splitField(field).map(canonicalToken).join("_");
}

function splitField(s) {
  return String(s)
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean);
}

function tokens(s) {
  return splitField(s).filter((t) => !stop.has(t));
}

function canonicalToken(t) {
  return tokenSynonyms[t] ?? t.replace(/s$/, "");
}

function genericPenalty(desc) {
  const d = String(desc).toLowerCase();
  if (["thing", "creativework", "action"].includes(d)) return 0.18;
  if (["webpage", "person", "event", "recipe", "imageobject", "invoice", "order"].includes(d)) return 0.03;
  return 0;
}

function jaccard(a, b) {
  if (!a.size && !b.size) return 0;
  let inter = 0;
  for (const x of a) if (b.has(x)) inter++;
  return inter / new Set([...a, ...b]).size;
}

function rate(n, d) {
  return d ? round2(n / d) : null;
}

function pct(n) {
  return n == null ? "n/a" : `${Math.round(n * 100)}%`;
}

function round2(n) {
  return Math.round(n * 100) / 100;
}

function average(xs) {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0;
}

function overlaySchema(desc, purpose, fields) {
  return normalizeSchema({
    name: desc.toLowerCase().replaceAll(/[^a-z0-9]+/g, "_"),
    descriptive_name: desc,
    purpose_statement: purpose,
    fields,
    field_descriptions: Object.fromEntries(fields.map((f) => [f, `${f.replaceAll("_", " ")} for ${desc}`])),
  });
}

function schemaOrgInheritanceOverlay() {
  return [
    overlaySchema("Person", "People and contacts with names, communication channels, and organization context.", [
      "identifier", "name", "email", "phone", "telephone", "company", "organization", "url", "website", "same_as", "profile_url", "linkedin", "twitter", "workplace", "office_location", "years_experience", "description",
    ]),
    overlaySchema("Recipe", "Cooking recipes with ingredients, instructions, timing, and servings.", [
      "identifier", "name", "recipe_name", "ingredients", "ingredient_list", "components", "instructions", "recipe_instructions", "steps", "method", "prep_time", "prep_time_minutes", "prep", "cook_time", "cook_time_minutes", "active_time", "inactive_time", "total_time", "servings",
    ]),
    overlaySchema("Event", "Calendar and real-world events with title, time, location, participants, and description.", [
      "identifier", "name", "title", "event_title", "start_date", "start_time", "start", "begins", "end_date", "end_time", "end", "ends", "location", "venue", "place", "location_name", "location_address", "participants", "attendees", "recurring", "description",
    ]),
    overlaySchema("ImageObject", "Images and photographs with URL, capture metadata, camera metadata, caption, and description.", [
      "identifier", "id", "photo_id", "pic_id", "url", "image_url", "image_link", "source_url", "captured_at", "shot_time", "time_taken", "camera_model", "camera_info", "device", "lens", "focal_length_mm", "photographer", "caption", "description",
    ]),
    overlaySchema("CreativeWork", "Notes and written works with title, body content, timestamps, tags, URL, and description.", [
      "identifier", "name", "title", "entry_title", "note_title", "subject", "content", "body", "text", "entry_text", "note", "timestamp", "created_at", "created", "recorded_at", "date_created", "labels", "tags", "topics", "url", "description",
    ]),
    overlaySchema("WebPage", "Saved web pages and links with URL, title, saved time, tags, and description.", [
      "identifier", "url", "uri", "href", "link", "link_url", "webpage_url", "web_address", "title", "page_title", "heading", "name", "saved_at", "saved_timestamp", "bookmarked_on", "created", "categories", "tags", "description",
    ]),
    overlaySchema("Invoice", "Financial transactions, invoices, expenses, amounts, merchants, dates, categories, and payment methods.", [
      "identifier", "transaction_id", "txn_id", "txn_code", "trans_id", "transaction_ref", "reference_number", "amount", "transaction_amount", "cost", "date", "transaction_date", "transaction_time", "merchant_name", "business_name", "merchant", "transaction_category", "category", "type", "description", "payment_method", "card_last_four",
    ]),
    overlaySchema("Action", "Tasks and actions with title, status, due date, assignee, priority, and description.", [
      "identifier", "task_id", "task_number", "uid", "title", "current_status", "status", "progress_status", "due_date", "deadline", "target_date", "assigned_to", "assignee", "responsible_party", "priority", "description",
    ]),
  ];
}

const stop = new Set([
  "the", "a", "an", "and", "or", "of", "for", "to", "with", "in", "on", "at", "by", "is", "are", "be", "this", "that", "used", "stores", "store",
]);

const tokenSynonyms = {
  tel: "phone",
  telephone: "phone",
  mobile: "phone",
  organization: "company",
  employer: "company",
  company_name: "company",
  organization_name: "company",
  employer_name: "company",
  title_text: "title",
  note_title: "title",
  event_title: "title",
  page_title: "title",
  heading: "title",
  recipe_name: "name",
  dish: "name",
  url: "url",
  uri: "url",
  href: "url",
  link: "url",
  webpage: "url",
  web_address: "url",
  start_time: "start",
  start_date: "start",
  end_time: "end",
  end_date: "end",
  venue: "location",
  merchant_name: "merchant",
  business_name: "merchant",
  trans: "transaction",
  txn: "transaction",
  ref: "reference",
  reference_number: "identifier",
  uid: "identifier",
  id: "identifier",
  categories: "tags",
  topics: "tags",
  labels: "tags",
  begins: "start",
  ends: "end",
  deadline: "due",
  target: "due",
  responsible: "assigned",
  party: "assignee",
};

const fieldSynonymGroups = [
  ["name", "full_name", "contact_name", "person", "person_name"],
  ["email", "email_address", "contact_email", "email_addr"],
  ["phone", "telephone", "mobile", "tel", "phone_number"],
  ["company", "organization", "employer", "company_name"],
  ["website", "website_url", "url", "profile_url", "same_as", "linkedin", "twitter"],
  ["workplace", "office_location", "address", "location"],
  ["url", "link", "link_url", "webpage_url", "web_address", "uri", "href"],
  ["title", "name", "event_title", "note_title", "title_text", "recipe_name"],
  ["content", "body", "note", "article_body"],
  ["content", "body", "note", "entry_text", "text", "article_body"],
  ["tags", "labels", "keywords", "categories", "topics"],
  ["start_time", "start_date", "start", "begins"],
  ["end_time", "end_date", "end", "ends"],
  ["venue", "location", "place", "location_name", "location_address"],
  ["participants", "attendees"],
  ["captured_at", "shot_time", "time_taken", "timestamp"],
  ["camera_model", "camera_info", "device"],
  ["photo_id", "pic_id", "id", "identifier"],
  ["image_url", "image_link", "source_url", "url"],
  ["instructions", "recipe_instructions", "steps"],
  ["ingredients", "ingredient_list", "recipe_ingredient", "components"],
  ["cook_time", "cook_time_minutes"],
  ["prep_time", "prep_time_minutes", "prep"],
  ["active_time", "inactive_time", "total_time"],
  ["transaction_id", "txn_id", "txn_code", "trans_id", "transaction_ref", "reference_number", "identifier"],
  ["transaction_date", "transaction_time", "date", "created"],
  ["amount", "transaction_amount", "cost"],
  ["merchant", "merchant_name", "business_name"],
  ["transaction_category", "category"],
  ["saved_at", "saved_timestamp", "bookmarked_on", "date_created", "created"],
  ["due_date", "deadline", "target_date", "end_date"],
  ["assigned_to", "assignee", "responsible_party"],
  ["current_status", "progress_status", "status"],
].map((g) => new Set(g.map(canonicalField)));

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
