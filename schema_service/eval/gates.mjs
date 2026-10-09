// Focused locked-gate checker for schema match/decomposition cutover work.
//
// This reads artifacts produced by:
//   node run.mjs --orders=original
//   node component_cover.mjs [--field-embeddings]
//
// It intentionally checks the card's locked gates directly instead of reusing
// score.summary.pass, which also contains broader eval-health goals such as
// zero dup explosion.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const EVAL_DIR = dirname(fileURLToPath(import.meta.url));

const stage2File =
  process.argv.find((a) => a.startsWith("--stage2="))?.split("=")[1] ??
  join(EVAL_DIR, "results", "latest.json");
const coverFile =
  process.argv.find((a) => a.startsWith("--component-cover="))?.split("=")[1] ??
  join(EVAL_DIR, "results", "component-cover-latest.json");
const label = process.argv.find((a) => a.startsWith("--label="))?.split("=")[1] ?? "original";
const minFieldCoverage = numberArg("--min-field-coverage", 0.86);
const minReuseRate = numberArg("--min-reuse-rate", null);

const stage2 = readJson(stage2File, "stage2");
const cover = readJson(coverFile, "component-cover");

const score = stage2.scores?.find((s) => s.label === label);
if (!score) fail(`stage2 score for label '${label}' not found in ${stage2File}`);

const failures = [];
if (score.summary.errors !== 0) failures.push(`stage2 errors=${score.summary.errors}`);
if (score.summary.wrongMerges !== 0)
  failures.push(`stage2 wrongMerges=${score.summary.wrongMerges}`);

const composed = parseFraction(score.summary.composed);
if (composed.den > 0 && composed.num !== composed.den)
  failures.push(`stage2 composed=${score.summary.composed}`);

if (minReuseRate != null) {
  const reuseRate = score.summary.reuse_rate;
  if (typeof reuseRate !== "number" || reuseRate < minReuseRate)
    failures.push(`stage2 reuse_rate=${reuseRate ?? "n/a"} < ${minReuseRate}`);
}

const bestCover = [...(cover.results ?? [])].sort(
  (a, b) =>
    b.summary.field_coverage_rate - a.summary.field_coverage_rate ||
    b.summary.record_full_cover_rate - a.summary.record_full_cover_rate ||
    a.summary.avg_component_count - b.summary.avg_component_count,
)[0];
if (!bestCover) fail(`component-cover results not found in ${coverFile}`);
if (minFieldCoverage >= 0.86 && cover.useFieldEmbeddings !== true)
  failures.push(
    `component cover artifact was produced without --field-embeddings; rerun component_cover.mjs --field-embeddings for the ${minFieldCoverage} floor`,
  );
if (bestCover.summary.field_coverage_rate < minFieldCoverage)
  failures.push(
    `component cover ${bestCover.variant} field_coverage_rate=${bestCover.summary.field_coverage_rate} < ${minFieldCoverage}`,
  );

if (failures.length) fail(failures.join("; "));

console.log(
  [
    "locked gates: PASS",
    `stage2=${label}`,
    `wrongMerges=${score.summary.wrongMerges}`,
    `composed=${score.summary.composed}`,
    `reuse_rate=${score.summary.reuse_rate ?? "n/a"}`,
    `component_cover=${bestCover.variant}`,
    `field_coverage=${bestCover.summary.field_coverage_rate}`,
  ].join(" "),
);

function parseFraction(s) {
  const m = String(s ?? "0/0").match(/^(\d+)\/(\d+)$/);
  if (!m) return { num: 0, den: 0 };
  return { num: Number(m[1]), den: Number(m[2]) };
}

function numberArg(name, fallback) {
  const raw = process.argv.find((a) => a.startsWith(`${name}=`))?.split("=")[1];
  if (raw == null || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) fail(`${name} must be a finite number`);
  return n;
}

function readJson(file, label) {
  try {
    return JSON.parse(readFileSync(file, "utf8"));
  } catch (error) {
    fail(`${label} artifact unreadable at ${file}: ${error.message}`);
  }
}

function fail(message) {
  console.error(`locked gates: FAIL ${message}`);
  process.exit(1);
}
