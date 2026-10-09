// The hourly routine driver. Runs the eval on a stable corpus, analyzes the
// result into improvement findings, and UPSERTS one kanban card per theme.
//
//   node routine.mjs               # dry-run: print the cards it WOULD file
//   node routine.mjs --file        # actually file/refresh the kanban cards
//
// Discipline (per the scheduled-routines rule): this routine FILES cards and
// follows the board — it never ships code. The hourly RUN path execs a
// prebuilt SCHEMA_EVAL_SERVER_BIN and never cargo-builds. The eval runs
// against an ephemeral, isolated schema_service — never the primary brain;
// brain is only touched to append the trend log.

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { analyze, summarize } from "./analyze.mjs";
import { logResult } from "./log_result.mjs";
import { EVAL_DIR, latestResultsPath, resolveKanbanBin } from "./paths.mjs";

const file = process.argv.includes("--file");
// --from-results: skip the eval, analyze the existing results/latest.json.
const fromResults = process.argv.includes("--from-results");
const corpusFile =
  process.argv.find((a) => a.startsWith("--corpus="))?.split("=")[1] ??
  "corpus_generated.json";

function sh(cmd, args, opts = {}) {
  return execFileSync(cmd, args, { encoding: "utf8", ...opts });
}

function fileCard(f) {
  // Stable slug => upsert. `add --body` replaces the whole body, which is what
  // we want: each theme card is a living card refreshed with the latest run.
  // Use the installed host-track kanban CLI — never a sibling fkanban checkout.
  sh(resolveKanbanBin(), [
    "add",
    f.slug,
    "--title",
    f.title,
    "--tags",
    f.tags.join(","),
    "--body",
    f.body,
  ]);
}

async function main() {
  console.log(`• schema-eval routine — ${file ? "FILE" : "dry-run"} mode`);

  // 1. Run the eval (Stage 1 proposals + Stage 2 canonicalization + judge).
  //    Proposals and judge verdicts are cached, so steady-state cost is ~0.
  if (fromResults) {
    console.log("• --from-results: reusing existing results/latest.json");
  } else {
    console.log("• running eval (seeded baseline, stage1, judge)…");
    sh(
      "node",
      [
        join(EVAL_DIR, "run.mjs"),
        "--stage1",
        "--judge",
        "--baseline=seeded",
        `--corpus=${corpusFile}`,
      ],
      { stdio: "inherit" },
    );
  }

  // 2. Analyze the result into improvement findings.
  const results = JSON.parse(readFileSync(latestResultsPath(), "utf8"));
  const findings = analyze(results);
  console.log(`\n• findings:\n${summarize(findings)}`);

  // 2b. Persist this result to brain (+ local STATE history) for progress tracking.
  const logged = logResult(latestResultsPath(), {
    noFbrain:
      process.argv.includes("--no-fbrain") || process.argv.includes("--no-brain"),
  });
  console.log(
    `• logged result ${logged.row.ts} → brain ${JSON.stringify(logged.fbrain)}`,
  );

  // 3. File / refresh one fkanban card per finding.
  for (const f of findings) {
    if (!file) {
      console.log(`\n┌─ [dry-run] card ${f.slug} (${f.severity})`);
      console.log(`│  ${f.title}`);
      console.log(
        f.body
          .split("\n")
          .map((l) => `│  ${l}`)
          .join("\n"),
      );
      console.log("└─");
      continue;
    }
    try {
      fileCard(f);
      console.log(`  ✓ filed/refreshed ${f.slug}`);
    } catch (e) {
      console.error(
        `  ✗ ${f.slug}: ${String(e.message ?? e).split("\n").find((l) => l.trim()) ?? "failed"}`,
      );
    }
  }

  if (file && !findings.length) console.log("  (nothing to file — clean run)");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
