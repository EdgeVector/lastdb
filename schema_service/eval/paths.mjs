// RUN / STATE path resolution for the schema-eval harness.
//
// Product code lives in fold (DEV). The hourly job copies this eval tree into
// a RUN home (`~/.local/share/edgevector/schema-eval/`) at a pinned SHA and
// execs a prebuilt `schema_service`. Results and history live in STATE
// (`~/.schema-eval/`), never inside the code tree or a portal checkout.

import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const EVAL_DIR = dirname(fileURLToPath(import.meta.url));
export const FOLD_DIR = join(EVAL_DIR, "..", "..");
export const FOLD_MANIFEST = join(FOLD_DIR, "Cargo.toml");

export function stateDir() {
  return process.env.SCHEMA_EVAL_STATE_DIR || join(homedir(), ".schema-eval");
}

export function resultsDir() {
  return join(stateDir(), "results");
}

export function latestResultsPath() {
  return join(resultsDir(), "latest.json");
}

export function historyPath() {
  return join(stateDir(), "history.jsonl");
}

export function cacheDir() {
  return process.env.SCHEMA_EVAL_CACHE_DIR || join(stateDir(), "cache");
}

export function resolveKanbanBin() {
  if (process.env.KANBAN_BIN) return process.env.KANBAN_BIN;
  const homeBin = join(homedir(), ".local", "bin", "kanban");
  if (existsSync(homeBin)) return homeBin;
  return "kanban";
}

export function resolveBrainBin() {
  if (process.env.BRAIN_BIN) return process.env.BRAIN_BIN;
  if (process.env.FBRAIN_BIN) return process.env.FBRAIN_BIN;
  const homeBin = join(homedir(), ".local", "bin", "brain");
  if (existsSync(homeBin)) return homeBin;
  return "brain";
}

export function resolveServerBin({ requested } = {}) {
  const fromArg = requested || process.env.SCHEMA_EVAL_SERVER_BIN;
  if (fromArg) return resolve(fromArg);
  if (!existsSync(FOLD_MANIFEST)) {
    return join(EVAL_DIR, "bin", "schema_service");
  }
  return join(FOLD_DIR, "target", "debug", "schema_service");
}

// Hourly RUN path must not cargo-build. Skip when the operator supplied a
// prebuilt binary, or when this eval tree is not sitting inside a fold
// checkout (no Cargo.toml two levels up).
export function shouldSkipCargoBuild({
  envBin = process.env.SCHEMA_EVAL_SERVER_BIN,
} = {}) {
  if (envBin) return true;
  if (!existsSync(FOLD_MANIFEST)) return true;
  return false;
}
