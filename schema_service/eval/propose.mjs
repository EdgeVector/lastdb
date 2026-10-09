// Stage 1 — Proposal. Faithfully replicates the fold_db_node ingestion LLM
// step: raw JSON input -> proposed schema. Same model + verbatim prompt as the
// node (historical fold_db llm_registry ingestion prompts (removed)), so the
// proposals this emits match what the real node would produce.
//
// Proposals are content-addressed and cached, so each input is proposed ONCE
// (the only token-costly step) and then replayed cheaply in any order.
//
// Fidelity caveat: the node also injects a "known schemas" anchor block
// (ai/known_schemas.rs) that primes reuse of curated names. The harness OMITS
// it on purpose, to keep proposals independent of registry state (the whole
// point of decoupling Stage 1 from Stage 2). Bump PROMPT_VERSION to invalidate
// the cache if the prompt changes.

import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

export const MODEL = "claude-haiku-4-5-20251001";
const PROMPT_VERSION = "node-ingestion-2026-06-19-purpose";

// Verbatim from prompts/ingestion.rs PROMPT_HEADER.
const PROMPT_HEADER = `Create a schema for this sample json data. Return JSON with "new_schemas" (single schema) and "mutation_mappers" (top-level JSON keys only, e.g., {"id": "id"}). Keep nested objects as single fields — do NOT flatten.

Choose schema_type by asking "could there ever be MORE records of this kind?" — for entities (people, accounts, transactions, notes, documents, events) the answer is yes, so use a keyed COLLECTION (HashRange/Hash/Range) even when the sample shows only ONE object. Reserve Single for true singletons. CRITICAL: any hash_field/range_field MUST be a field that actually exists in the sample — never invent fields. Use dot-notation for nested values (e.g., "departure.date") but the parent must be in "fields" and "mutation_mappers".
- HashRange: data has a unique ID field (e.g., "flight_id", "order_id", "id", "uuid") -> hash_field=that ID (unique IDs prevent collisions); else a grouping field (e.g., "author", "category", "source_file"). range_field orders within a key (prefer a date/timestamp, else "id").
- Hash (hash_field only, NO range_field): records keyed by one natural identifier with no ordering — photos/images MUST use hash_field="source_file_name" (do NOT add date_taken as range_field); a roster keys by "name"; documents/notes key by "source_file".
- Range (range_field only): an ordered collection with no unique id — key by a date/timestamp or sequence field.
- Single (omit "key" entirely): ONLY when exactly one record will ever exist — app/editor config, feature flags, a settings blob, one URL/timeout. Do NOT use Single just because the sample is one object; if it describes an entity (a person, account, snapshot, note, document), use a keyed collection.

"name": short snake_case CONTENT TOPIC — for record/entity data name the entity (e.g., "users", "products", "recipes", "medical_records"). Include "descriptive_name", "field_descriptions" (EVERY field).

descriptive_name MUST be a CATEGORY, not an instance. Aim for ≤3 words, ≤30 characters. The same name should fit a *future* file of the same kind — never the title of the specific file you are looking at.

ALSO include "purpose_statement": ONE sentence stating what this schema is FOR — the concept or intent the records capture, i.e. WHY someone keeps this kind of record. It must be INDEPENDENT of the name and the field list: do NOT restate the descriptive_name, and do NOT enumerate the fields. Two schemas with the same name but different intent MUST get different purpose statements. Examples: a "Meeting Notes" schema that records what was discussed and decided → "Captures the discussion, decisions, and action items from a meeting." vs. a "Meeting Notes" schema that schedules an upcoming meeting → "Records when and where an upcoming meeting takes place and who attends." For "Recipes": "Describes how to cook a dish, with its ingredients and steps." For "Transactions": "Tracks money moving in or out of an account for budgeting and reconciliation." Bad (restates the name): "A collection of meeting notes." Bad (lists fields): "Has a title, a body, and a created_at."

REJECTED — TOO GENERIC (all structural/format words): NEVER fall back to a placeholder. "Content Items", "Items", "Data", "Records", "Content", "Document Collection", "Data Records", "Text Content", "Content Articles", "Personal Notes", "File Metadata", "Record List", "General Information" are all rejected.

REJECTED — METHOD OF INGESTION: NEVER name a schema after how the data was extracted, ingested, converted, or processed. "Document Extractions", "Extracted Documents", "PDF Imports", "Parsed Files", "Ingested Documents", "Markdown Conversions", "OCR Results" are all rejected. Extraction metadata (frontmatter keys like source/pages/extraction, file paths, converter or parser names) describes the pipeline, not the records — ignore it and name what the records are ABOUT.

REJECTED — TOO SPECIFIC (instance-level / contains the title of a single document): names that include proper nouns, dates, identifiers, or the dish/place/topic of one file. Examples of WRONG vs RIGHT:
- "Roasted Tomato Soup Recipe" -> "Recipes"
- "Bitcoin Macro Analysis" -> "Market Analysis Notes" (or "Research Notes")
- "Paris Trip 2024 Journal" -> "Travel Notes"
- "How Rust Borrow Checker Works" -> "Technical Notes" (or "How-To Guides")
- "Weekly Meeting Minutes 2025-01-15" -> "Meeting Notes"
- "Family Vacation Photos Hawaii" -> "Photos"

The name must identify WHAT KIND of records these are, not the topic of one file. Read the actual fields and name the category: structured records get the entity name ("Users", "Products", "Customer Orders"); documents/notes get the genre ("Recipes", "Journal Entries", "Meeting Notes", "Travel Notes", "Technical Notes", "Shopping Lists"). If two files of the same kind would land in different schemas under your proposed name, the name is too specific — generalise.

Array fields (e.g., tags, interests, items) MUST be included in both "fields" and "mutation_mappers" — they are stored as arrays, not flattened. Include ALL top-level JSON keys in "mutation_mappers", including arrays. ONLY map fields that exist in the sample data — never add fields the data doesn't have.

Example:
{"name": "social_media_posts", "descriptive_name": "Social Media Posts", "purpose_statement": "Captures what someone published on social media so it can be searched and revisited.", "key": {"hash_field": "author", "range_field": "created_at"}, "fields": ["created_at", "author", "content", "tags"], "field_descriptions": {"created_at": "...", "author": "...", "content": "...", "tags": "topic tags"}, "mutation_mappers": {"created_at": "created_at", "author": "author", "content": "content", "tags": "tags"}}`;

const PROMPT_ACTIONS = `Please analyze the sample data and create a new schema definition in new_schemas with mutation_mappers.

The response must be valid JSON.`;

function buildPrompt(input) {
  const sample = JSON.stringify(input, null, 2);
  const arrayNote = Array.isArray(input)
    ? "\n\nIMPORTANT: The user provided a JSON ARRAY of multiple objects. You MUST create a Range schema with a range_key to store multiple entities."
    : "";
  return `${PROMPT_HEADER}\n\nSample JSON Data:\n${sample}${arrayNote}\n\n${PROMPT_ACTIONS}`;
}

export async function callAnthropic(prompt, { model = MODEL } = {}) {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) throw new Error("ANTHROPIC_API_KEY not set — required for Stage 1");
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": key,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model,
      max_tokens: 16000,
      temperature: 0.1,
      messages: [{ role: "user", content: prompt }],
    }),
  });
  const j = await res.json();
  if (!res.ok)
    throw new Error(`anthropic ${res.status}: ${JSON.stringify(j).slice(0, 300)}`);
  return j.content?.[0]?.text ?? "";
}

function extractJson(text) {
  let t = text.trim();
  const fence = t.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) t = fence[1].trim();
  const start = t.indexOf("{");
  const end = t.lastIndexOf("}");
  if (start >= 0 && end > start) t = t.slice(start, end + 1);
  return JSON.parse(t);
}

// Convert the node's { new_schemas, mutation_mappers } into the POST shape the
// harness/schema-service expects. Mirrors validate_and_convert_response.
function toProposal(parsed) {
  let ns = parsed.new_schemas;
  if (Array.isArray(ns)) ns = ns[0];
  // unwrap a { "schema_name": {...} } single-key wrapper
  if (ns && !ns.name && !ns.fields) {
    const keys = Object.keys(ns);
    if (keys.length === 1 && ns[keys[0]] && typeof ns[keys[0]] === "object")
      ns = ns[keys[0]];
  }
  if (!ns || !ns.descriptive_name)
    throw new Error("Stage-1 proposal missing descriptive_name");

  const fields = ns.fields ?? Object.keys(ns.field_descriptions ?? {});
  const fieldDescriptions = { ...(ns.field_descriptions ?? {}) };
  // The service 400s on any field without a description. The node fills these
  // via a 2nd LLM pass; for the harness a generic fill is sufficient.
  for (const f of fields)
    if (!fieldDescriptions[f]) fieldDescriptions[f] = `The ${f} value.`;

  const proposal = {
    name: ns.name ?? ns.descriptive_name.replace(/[^A-Za-z0-9]/g, ""),
    schema_type: ns.schema_type ?? (ns.key ? "Hash" : "Single"),
    descriptive_name: ns.descriptive_name,
    fields,
    field_descriptions: fieldDescriptions,
  };
  if (ns.purpose_statement) proposal.purpose_statement = ns.purpose_statement;
  if (ns.key) proposal.key = ns.key;
  if (ns.field_classifications)
    proposal.field_classifications = ns.field_classifications;
  return proposal;
}

export async function proposeSchema(input, opts = {}) {
  const text = await callAnthropic(buildPrompt(input), opts);
  return toProposal(extractJson(text));
}

function cacheKey(input, model) {
  return createHash("sha256")
    .update(`${PROMPT_VERSION}\n${model}\n${JSON.stringify(input)}`)
    .digest("hex")
    .slice(0, 16);
}

export async function getOrGenerateProposal(
  input,
  { cacheDir, model = MODEL, log } = {},
) {
  mkdirSync(cacheDir, { recursive: true });
  const f = join(cacheDir, `${cacheKey(input, model)}.json`);
  if (existsSync(f)) return JSON.parse(readFileSync(f, "utf8"));
  log?.(`    · Stage-1 LLM proposing for input {${Object.keys(Array.isArray(input) ? (input[0] ?? {}) : input).join(", ")}}`);
  const proposal = await proposeSchema(input, { model });
  writeFileSync(f, JSON.stringify(proposal, null, 2));
  return proposal;
}

// Fill item.proposal for every corpus item. With { force: true }, ignore any
// hand-authored proposal and regenerate from item.input via Stage 1.
export async function ensureProposals(
  items,
  { cacheDir, model, log, force = false } = {},
) {
  for (const it of items) {
    if (it.proposal && !force) continue;
    if (!it.input)
      throw new Error(`corpus item ${it.id} has neither a proposal nor an input`);
    it.proposal = await getOrGenerateProposal(it.input, { cacheDir, model, log });
  }
  return items;
}
