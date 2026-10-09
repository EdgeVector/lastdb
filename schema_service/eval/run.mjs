// Orchestrator: boot an ephemeral schema_service, replay the corpus in several
// ORDERS against a fresh registry each time, score every run on the Stage-2
// criteria, and report order-sensitivity (confluence). Writes results JSON.
//
//   node run.mjs                 # default: original / reversed / rotated orders
//   node run.mjs --keep-server   # leave the server up after (for poking)
//
// Dev-only. Ephemeral instance, isolated $HOME — never the :9001 brain, never
// the dev/prod Lambda.

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { EphemeralSchemaService } from "./server.mjs";
import { runScenario } from "./harness.mjs";
import { scoreScenario, confluence } from "./score.mjs";
import { ensureProposals } from "./propose.mjs";
import { judgeScenario } from "./judge.mjs";
import {
  EVAL_DIR,
  cacheDir,
  latestResultsPath,
  resultsDir,
  shouldSkipCargoBuild,
} from "./paths.mjs";
const keepServer = process.argv.includes("--keep-server");
// --stage1: ignore hand-authored proposals; generate them from each item's raw
// `input` via the real Stage-1 LLM (cached). This exercises the full pipeline.
const stage1 = process.argv.includes("--stage1");
// --judge: run the LLM-as-judge over the original-order scenario.
const judgeFlag = process.argv.includes("--judge");
// --corpus=<file>: load an alternate corpus (e.g. a generated one).
const corpusFile =
  process.argv.find((a) => a.startsWith("--corpus="))?.split("=")[1] ??
  "corpus.json";
// --baseline=seeded (default, faithful: 942 seeds present) | empty (reset-only)
const baselineArg =
  process.argv.find((a) => a.startsWith("--baseline="))?.split("=")[1] ??
  "seeded";
// --orders=original,reversed,rotated limits the replay for focused gate checks.
const orderArg = process.argv.find((a) => a.startsWith("--orders="))?.split("=")[1] ?? null;
// The compositional decompose/apply path is the metric under test, so the
// ephemeral service boots with SCHEMA_COMPOSITIONAL_DECOMPOSITION=apply by
// default: a proposal whose nested `ref_fields` components reuse existing
// canonicals registers as COMPOSED instead of re-inlining a mega-schema.
// `--no-compositional` boots with the path OFF (today's flat behavior) for an
// explicit A/B baseline; an explicit `SCHEMA_COMPOSITIONAL_DECOMPOSITION` env
// still wins. Dev-only + ephemeral — never the :9001 brain, never prod.
const compositional = process.argv.includes("--no-compositional")
  ? null
  : (process.env.SCHEMA_COMPOSITIONAL_DECOMPOSITION ?? "apply");

function permutations(ids) {
  const reversed = [...ids].reverse();
  // a rotation puts a later concept's first occurrence ahead of an earlier one
  const rotated = [...ids.slice(3), ...ids.slice(0, 3)];
  const all = [
    { label: "original", order: ids },
    { label: "reversed", order: reversed },
    { label: "rotated", order: rotated },
  ];
  if (!orderArg) return all;
  const wanted = new Set(orderArg.split(",").map((s) => s.trim()).filter(Boolean));
  const selected = all.filter((o) => wanted.has(o.label));
  if (!selected.length) throw new Error(`--orders selected no known orders: ${orderArg}`);
  return selected;
}

function fmtTrace(scenario) {
  const rows = scenario.steps.map((s) => {
    const tag =
      s.decision === "NEW"
        ? "🆕 NEW     "
        : s.decision === "REUSED"
          ? "♻️  REUSED  "
          : s.decision === "EXPANDED"
            ? "➕ EXPANDED"
            : s.decision === "COMPOSED"
              ? "🧩 COMPOSED"
              : s.decision === "ERROR"
                ? "🛑 ERROR   "
                : "❓ " + s.decision;
    // A composed step lists the existing canonicals it reused via SchemaRef.
    const composedSuffix =
      s.decision === "COMPOSED" && Array.isArray(s.composedFrom) && s.composedFrom.length
        ? `  (reused: ${s.composedFrom.join(", ")})`
        : "";
    const errMsg =
      typeof s.error === "string" ? s.error : JSON.stringify(s.error);
    const suffix =
      s.decision === "ERROR" ? `  (${s.status}: ${errMsg})` : composedSuffix;
    return `   ${tag} ${s.id.padEnd(22)} → ${s.resolvedTo}${suffix}`;
  });
  return rows.join("\n");
}

async function main() {
  const corpus = JSON.parse(
    readFileSync(join(EVAL_DIR, corpusFile), "utf8"),
  ).items;
  const ids = corpus.map((c) => c.id);

  // Stage 1: resolve every item's proposal (LLM-generated + cached when
  // --stage1, else the hand-authored proposal in corpus.json).
  console.log(
    stage1
      ? "• Stage 1: generating proposals from raw inputs via LLM (cached)…"
      : "• Stage 1: using hand-authored proposals from corpus.json",
  );
  await ensureProposals(corpus, {
    cacheDir: join(cacheDir(), "proposals"),
    force: stage1,
    log: (m) => console.log(m),
  });

  const svc = new EphemeralSchemaService({ compositional });
  if (shouldSkipCargoBuild()) {
    console.log(`• using prebuilt schema_service at ${svc.bin}`);
  } else {
    console.log("• building schema_service (once)…");
  }
  await svc.build();
  console.log("• starting ephemeral schema_service (downloads MiniLM on first run)…");
  await svc.start();

  // Seeded baseline restarts the service (cold-start reloads 942 seeds);
  // empty baseline just resets (wipes seeds, user-vs-user only).
  const prepare =
    baselineArg === "seeded"
      ? () => svc.restart()
      : () => svc.client.reset();
  console.log(`• baseline mode: ${baselineArg}`);
  console.log(
    `• compositional decompose/apply: ${compositional ?? "OFF (--no-compositional)"}`,
  );

  const scenarios = [];
  const scores = [];
  try {
    for (const { label, order } of permutations(ids)) {
      const sc = await runScenario(svc.client, corpus, order, { label, prepare });
      const score = scoreScenario(sc, corpus);
      scenarios.push(sc);
      scores.push(score);

      const ps = score.summary;
      console.log(`\n══ order: ${label} ${ps.pass ? "✅" : "❌"}`);
      console.log(`   baseline schemas after reset: ${sc.baselineCount}`);
      console.log(fmtTrace(sc));
      console.log(
        `   errors=${ps.errors}  dup-explosion=${ps.dupExplosion}  reuse=${ps.reuse}  wrong-merges=${ps.wrongMerges}  field-drops=${ps.fieldDrops}`,
      );
      console.log(
        `   reuse_rate=${ps.reuse_rate ?? "n/a"}  composed=${ps.composed} (count=${ps.composed_count})  avg_fields/canon=${ps.avg_fields_per_canonical}  max_canon_fields=${ps.max_canonical_fields}`,
      );
      if (score.detail.decomposeMisses.length)
        console.log(
          "   ⚠ decompose-misses:",
          JSON.stringify(score.detail.decomposeMisses),
        );
      if (score.detail.wrongMerges.length)
        console.log("   ⚠ wrong-merges:", JSON.stringify(score.detail.wrongMerges));
    }

    let judgement = null;
    if (judgeFlag) {
      console.log("\n══ LLM-as-judge (original order)");
      judgement = await judgeScenario(scenarios[0], corpus, {
        cacheDir: join(cacheDir(), "judge"),
      });
      console.log(
        `   judged ${judgement.judgedCount} decisions, ${judgement.badCount} rated BAD`,
      );
      for (const b of judgement.bad)
        console.log(
          `   ✗ ${b.id.padEnd(18)} [${b.decision}] ${b.rationale}` +
            (b.suggested_fix ? `\n        ↳ fix: ${b.suggested_fix}` : ""),
        );
    }

    const conf = confluence(scenarios);
    console.log("\n══ order-sensitivity (confluence)");
    console.log(
      conf.converges
        ? `   ✅ all ${scenarios.length} orderings converge to the same canonical set`
        : `   ⚠ ${conf.distinctOutcomes} DIFFERENT outcomes across orderings — order matters here`,
    );
    for (const o of conf.perOrder) console.log(`     ${o.label.padEnd(10)} ${o.sig}`);

    const outDir = resultsDir();
    mkdirSync(outDir, { recursive: true });
    const out = { scores, confluence: conf, judgement, scenarios };
    writeFileSync(latestResultsPath(), JSON.stringify(out, null, 2));
    console.log(`\n• wrote ${latestResultsPath()}`);

    // --log: persist this result to brain + local history (off by default for
    // ad-hoc dev runs; the hourly routine always logs).
    if (process.argv.includes("--log")) {
      const { logResult } = await import("./log_result.mjs");
      const logged = logResult(latestResultsPath(), {});
      console.log(`• logged result ${logged.row.ts} → brain ${JSON.stringify(logged.fbrain)}`);
    }
  } finally {
    if (!keepServer) await svc.stop();
    else console.log(`\n• server left running on :${svc.port} (--keep-server)`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
