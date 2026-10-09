// Persist every eval result so progress is trackable over time. Two sinks:
//   1. ~/.schema-eval/history.jsonl — append-only STATE source of truth.
//   2. brain reference `schema-eval-results-log` — a single rolling trend table
//      (newest first, last 200 runs), rebuilt from history.jsonl each time.
//
// NOTE: we deliberately do NOT write a per-run `schema-eval-run-<ts>` brain
// record. That accumulated ~24 immutable records/day and buried the brain; the
// jsonl is the granular truth and the rolling table is the queryable view, so the
// per-run record was pure noise. (Dropped 2026-06-18 at Tom's request.)
//
// Timestamps use new Date() (fine in a plain node script). brain is written via
// the `brain` CLI (`put <slug> --type reference`, body on stdin), so this works
// headless under launchd. A failed brain write is logged, not fatal — the local
// history is always written first.

import { execFileSync } from "node:child_process";
import { readFileSync, appendFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { analyze } from "./analyze.mjs";
import { historyPath, latestResultsPath, resolveBrainBin } from "./paths.mjs";

const ROLLING_SLUG = "schema-eval-results-log";
const MAX_ROWS = 200;

// One compact summary row from a results/latest.json.
export function summarizeResult(results, ts) {
  const score = results.scores?.[0];
  const scenario = results.scenarios?.[0];
  const s = score?.summary ?? {};
  const findings = analyze(results);
  return {
    ts,
    corpus: scenario?.steps?.length ?? 0,
    canonicals: Object.keys(scenario?.finalCanonicals ?? {}).length,
    dupExplosion: s.dupExplosion ?? null,
    reuse: s.reuse ?? null,
    // headline reuse-maximization metrics
    reuseRate: s.reuse_rate ?? null,
    composedCount: s.composed_count ?? null,
    composed: s.composed ?? null,
    avgFieldsPerCanonical: s.avg_fields_per_canonical ?? null,
    maxCanonicalFields: s.max_canonical_fields ?? null,
    wrongMerges: s.wrongMerges ?? null,
    errors: s.errors ?? null,
    fieldDrops: s.fieldDrops ?? null,
    judgeBad: results.judgement?.badCount ?? null,
    judgeTotal: results.judgement?.judgedCount ?? null,
    converges: results.confluence?.converges ?? null,
    findings: findings.map((f) => ({ slug: f.slug, severity: f.severity, title: f.title })),
  };
}

function brainPut(slug, body) {
  execFileSync(resolveBrainBin(), ["put", slug, "--type", "reference"], {
    input: body,
    encoding: "utf8",
  });
}

function readHistory() {
  const HISTORY = historyPath();
  if (!existsSync(HISTORY)) return [];
  return readFileSync(HISTORY, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function rollingBody(history) {
  const rows = history.slice(-MAX_ROWS).reverse(); // newest first
  const header =
    "| Run (UTC) | inputs | canon | dup | reuse | reuseRate | composed | avgF | maxF | wrong | err | judgeBad | conv |\n" +
    "|---|---|---|---|---|---|---|---|---|---|---|---|---|";
  const body = rows
    .map(
      (r) =>
        `| ${r.ts} | ${r.corpus} | ${r.canonicals} | ${r.dupExplosion} | ${r.reuse} | ${r.reuseRate ?? "—"} | ${r.composed ?? "—"} | ${r.avgFieldsPerCanonical ?? "—"} | ${r.maxCanonicalFields ?? "—"} | ${r.wrongMerges} | ${r.errors} | ${r.judgeBad}/${r.judgeTotal} | ${r.converges ? "✓" : "✗"} |`,
    )
    .join("\n");
  return [
    "---",
    "type: reference",
    `slug: ${ROLLING_SLUG}`,
    "title: Schema eval — results log (trend)",
    "tags: [schema-eval, metrics, trend]",
    "---",
    "# Schema eval — results over time",
    "",
    `Auto-updated by the schema-eval routine. Newest first; last ${MAX_ROWS} runs. The full per-run detail (incl. findings) lives in \`~/.schema-eval/history.jsonl\` locally — this table is the queryable rollup. Lower dup/wrong/err and higher reuse = better. **reuseRate** = reused-or-composed / eligible (the headline reuse-maximization number, [0,1]). **composed** = composed / expected-decompositions. **avgF / maxF** = avg & max field count across the corpus's canonicals (mega-schema-growth proxy — lower is leaner).`,
    "",
    header,
    body,
  ].join("\n");
}

export function logResult(resultsPath, { ts, noFbrain = false } = {}) {
  ts = ts ?? new Date().toISOString().replace(/\.\d+Z$/, "Z");
  const results = JSON.parse(readFileSync(resultsPath, "utf8"));
  const row = summarizeResult(results, ts);

  // 1. local append-only source of truth (STATE, not the eval/RUN tree)
  const HISTORY = historyPath();
  mkdirSync(dirname(HISTORY), { recursive: true });
  appendFileSync(HISTORY, JSON.stringify(row) + "\n");

  if (noFbrain) return { row, fbrain: "skipped" };

  // 2. brain rolling trend table (rebuilt from history.jsonl). No per-run record.
  const status = {};
  try {
    brainPut(ROLLING_SLUG, rollingBody(readHistory()));
    status.rolling = "ok";
  } catch (e) {
    status.rolling = `FAILED: ${String(e.message ?? e).split("\n")[0]}`;
  }
  return { row, fbrain: status };
}

// CLI: node log_result.mjs [results/latest.json] [--no-fbrain|--no-brain]
if (import.meta.url === `file://${process.argv[1]}`) {
  const path =
    process.argv.find((a) => a.endsWith(".json")) ?? latestResultsPath();
  const out = logResult(path, {
    noFbrain:
      process.argv.includes("--no-fbrain") || process.argv.includes("--no-brain"),
  });
  console.log(`• logged run ${out.row.ts}: dup=${out.row.dupExplosion} reuse=${out.row.reuse} wrong=${out.row.wrongMerges} err=${out.row.errors} judgeBad=${out.row.judgeBad}/${out.row.judgeTotal}`);
  console.log(`• brain: ${JSON.stringify(out.fbrain)}`);
}
