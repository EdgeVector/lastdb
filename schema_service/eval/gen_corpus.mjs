// Corpus generator: synthesize a LABELED corpus of raw input documents at any
// scale. For each concept it asks the LLM for K diverse documents that describe
// the SAME kind of thing with DIFFERENT field names / shapes — which is exactly
// what stresses canonicalization (does the service recognize semantic
// equivalence across surface variation?).
//
//   node gen_corpus.mjs --per-concept 5 --out corpus_generated.json
//
// The concept label is assigned at generation time, so the deterministic scorer
// has ground truth without any hand-labeling.

import { writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { callAnthropic } from "./propose.mjs";

const EVAL_DIR = dirname(fileURLToPath(import.meta.url));

// Default concept set. Each is a real personal-data shape with room for surface
// variation (different field names for the same meaning).
const CONCEPTS = [
  { key: "contact", hint: "a person you can reach (name, email, phone, maybe company)" },
  { key: "photo", hint: "a photo/image with an id, a url, a capture time, maybe camera info" },
  { key: "recipe", hint: "a cooking recipe with a title, ingredients, steps, timing" },
  { key: "task", hint: "a to-do item with a title, status, due date, assignee" },
  { key: "note", hint: "a free-text note/journal entry with a title, body, created time, tags" },
  { key: "transaction", hint: "a financial transaction with amount, date, merchant, category" },
  { key: "event", hint: "a calendar event with a title, start, end, location, attendees" },
  { key: "bookmark", hint: "a saved web link with a url, title, saved time, tags" },
];

async function generateForConcept(concept, k, log) {
  const prompt = `Generate ${k} realistic but DIVERSE JSON documents that each describe ${concept.hint}.
Critical: vary the FIELD NAMES and structure across the ${k} documents (e.g. "name" vs "full_name" vs "contact_name"; "email" vs "email_address"; "url" vs "link" vs "image_url") and vary which optional fields are present — but every document must clearly be the same KIND of thing (a ${concept.key}).
Return ONLY a JSON array of ${k} objects. No prose, no markdown fences.`;
  const text = await callAnthropic(prompt);
  let t = text.trim();
  const fence = t.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) t = fence[1].trim();
  const start = t.indexOf("[");
  const end = t.lastIndexOf("]");
  if (start >= 0 && end > start) t = t.slice(start, end + 1);
  const docs = JSON.parse(t);
  log?.(`  · ${concept.key}: ${docs.length} inputs`);
  return docs.map((input, i) => ({
    id: `${concept.key}_${String(i + 1).padStart(2, "0")}`,
    expected_concept: concept.key,
    input,
  }));
}

async function main() {
  const perConcept = Number(
    process.argv.find((a) => a.startsWith("--per-concept="))?.split("=")[1] ??
      process.argv[process.argv.indexOf("--per-concept") + 1] ??
      5,
  );
  const out =
    process.argv.find((a) => a.startsWith("--out="))?.split("=")[1] ??
    "corpus_generated.json";

  console.log(`• generating ${perConcept} inputs × ${CONCEPTS.length} concepts via LLM…`);
  const items = [];
  for (const c of CONCEPTS) {
    try {
      items.push(...(await generateForConcept(c, perConcept, (m) => console.log(m))));
    } catch (e) {
      console.error(`  ! ${c.key} failed: ${e.message}`);
    }
  }
  const outPath = join(EVAL_DIR, out);
  writeFileSync(
    outPath,
    JSON.stringify(
      { _comment: `Generated ${items.length} labeled inputs across ${CONCEPTS.length} concepts.`, items },
      null,
      2,
    ),
  );
  console.log(`• wrote ${items.length} items → ${out}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
