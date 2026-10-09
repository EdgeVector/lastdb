// Boots an EPHEMERAL, ISOLATED schema_service for Stage-2 canonicalization
// experiments. Real fastembed embedder + real seeds (faithful merge behaviour),
// throwaway Sled db per run, isolated $HOME and fastembed cache.
//
// Safety: this never touches Tom's :9001 brain, never hits dev/prod Lambda,
// never writes to ~/.folddb. It's a throwaway instance, consistent with how
// the existing schema_service Rust tests stand up state.

import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SchemaServiceClient } from "./client.mjs";
import {
  FOLD_MANIFEST,
  cacheDir as defaultCacheDir,
  resolveServerBin,
  shouldSkipCargoBuild,
} from "./paths.mjs";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: "inherit", ...opts });
    p.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${cmd} exited ${code}`)),
    );
    p.on("error", reject);
  });
}

export class EphemeralSchemaService {
  // cacheDir is reused across runs so the ~30MB MiniLM model downloads once
  // (.fastembed_cache lands in cwd). The Sled db is fresh per start().
  //
  // `compositional` controls the `SCHEMA_COMPOSITIONAL_DECOMPOSITION` env the
  // ephemeral service boots with. The eval's whole job is to MEASURE the
  // compositional decompose/apply path, so it defaults to `apply` — a proposal
  // whose nested `ref_fields` components reuse existing canonicals registers as
  // `Composed` (reused via typed SchemaRefs) rather than re-inlining a
  // mega-schema. This is dev-only + ephemeral; it never touches the :9001 brain
  // or any prod surface, so turning the dev flag on here is exactly the
  // measurement the card asks for. An operator can still override with the env
  // var or pass `compositional: null` to boot with the path off (today's flat
  // behavior) for an A/B baseline.
  constructor({
    port = 9102,
    cacheDir = defaultCacheDir(),
    compositional = process.env.SCHEMA_COMPOSITIONAL_DECOMPOSITION ?? "apply",
    bin = undefined,
  } = {}) {
    this.port = port;
    this.cacheDir = cacheDir;
    this.compositional = compositional;
    this.bin = resolveServerBin({ requested: bin });
    this.proc = null;
    this.watchdog = null;
    this.dbDir = null;
    this.logTail = [];
    this.client = new SchemaServiceClient(`http://127.0.0.1:${port}`);
  }

  // Compile once up front so start() is fast — unless the hourly RUN path
  // supplied SCHEMA_EVAL_SERVER_BIN (or this tree is not a fold checkout).
  // `--features fastembed` is required for embedding-beam field-match probe
  // and faithful dual-signal merge behaviour (real MiniLM embedder).
  build() {
    if (shouldSkipCargoBuild()) {
      if (!existsSync(this.bin)) {
        return Promise.reject(
          new Error(
            `prebuilt schema_service missing at ${this.bin} (set SCHEMA_EVAL_SERVER_BIN or run scripts/install-launchd.sh refresh)`,
          ),
        );
      }
      return Promise.resolve();
    }
    return run("cargo", [
      "build",
      "--quiet",
      "-p",
      "schema_service_server_http",
      "--bin",
      "schema_service",
      "--features",
      "fastembed",
      "--manifest-path",
      FOLD_MANIFEST,
    ]);
  }

  async start({ waitMs = 120000 } = {}) {
    mkdirSync(this.cacheDir, { recursive: true });
    this.dbDir = mkdtempSync(join(tmpdir(), "schema-eval-db-"));
    // Exec the prebuilt binary directly (NOT `cargo run`): going through
    // cargo/rustup with an overridden HOME breaks toolchain resolution.
    const env = { ...process.env, HOME: this.cacheDir }; // isolate from ~/.folddb
    // Exercise the compositional decompose/apply path (the metric under test).
    // `compositional: null` boots with it off for an explicit flat baseline.
    if (this.compositional == null) delete env.SCHEMA_COMPOSITIONAL_DECOMPOSITION;
    else env.SCHEMA_COMPOSITIONAL_DECOMPOSITION = this.compositional;
    this.proc = spawn(
      this.bin,
      ["--port", String(this.port), "--db-path", this.dbDir],
      {
        cwd: this.cacheDir, // .fastembed_cache persists here, reused across runs
        env,
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    this.#startWatchdog();
    const capture = (buf) => {
      const s = buf.toString();
      this.logTail.push(s);
      if (this.logTail.length > 200) this.logTail.shift();
    };
    this.proc.stdout.on("data", capture);
    this.proc.stderr.on("data", capture);
    this.proc.on("exit", (code, signal) => {
      if ((code || signal) && code !== 0 && !this._stopping) {
        process.stderr.write(
          `\n[schema_service exited code=${code} signal=${signal}]\n` +
            this.logTail.join("") +
            "\n",
        );
      }
    });
    await this.#waitHealthy(waitMs);
  }

  async #waitHealthy(waitMs) {
    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      try {
        const r = await this.client.health();
        if (r.status === 200) return;
      } catch {
        /* not up yet */
      }
      await sleep(500);
    }
    throw new Error(
      "schema_service did not become healthy in time. Recent logs:\n" +
        this.logTail.join(""),
    );
  }

  async #killProc() {
    if (!this.proc) return;
    const p = this.proc;
    this._stopping = true;
    await new Promise((resolve) => {
      let done = false;
      const fin = () => {
        if (!done) {
          done = true;
          resolve();
        }
      };
      p.once("exit", fin);
      this.#killProcessGroup(p.pid, "SIGTERM");
      setTimeout(() => {
        this.#killProcessGroup(p.pid, "SIGKILL");
        fin();
      }, 5000);
    });
    this.#stopWatchdog();
    this.proc = null;
    this._stopping = false;
  }

  #killProcessGroup(pid, signal) {
    if (!pid) return;
    try {
      process.kill(-pid, signal);
    } catch {
      try {
        process.kill(pid, signal);
      } catch {
        /* already gone */
      }
    }
  }

  #startWatchdog() {
    if (!this.proc?.pid) return;
    const script = `
const { rmSync } = require("node:fs");
const parentPid = Number(process.argv[1]);
const servicePid = Number(process.argv[2]);
const dbDir = process.argv[3];
function alive(pid) {
  try { process.kill(pid, 0); return true; } catch { return false; }
}
function killGroup(signal) {
  try { process.kill(-servicePid, signal); } catch {
    try { process.kill(servicePid, signal); } catch {}
  }
}
const interval = setInterval(() => {
  if (!alive(servicePid)) process.exit(0);
  if (alive(parentPid)) return;
  killGroup("SIGTERM");
  setTimeout(() => {
    killGroup("SIGKILL");
    if (dbDir) {
      try { rmSync(dbDir, { recursive: true, force: true }); } catch {}
    }
    process.exit(0);
  }, 2000);
}, 1000);
`;
    this.watchdog = spawn(process.execPath, ["-e", script, String(process.pid), String(this.proc.pid), this.dbDir ?? ""], {
      detached: true,
      stdio: "ignore",
    });
    this.watchdog.unref();
  }

  #stopWatchdog() {
    if (!this.watchdog) return;
    try {
      this.watchdog.kill("SIGTERM");
    } catch {
      /* already gone */
    }
    this.watchdog = null;
  }

  #cleanupDb() {
    if (this.dbDir) {
      try {
        rmSync(this.dbDir, { recursive: true, force: true });
      } catch {
        /* best effort */
      }
      this.dbDir = null;
    }
  }

  // Re-seed a clean registry: kill, drop the db, cold-start fresh (reloads the
  // 942 seeds with pre-baked embeddings). This is the "seeded baseline".
  async restart() {
    await this.#killProc();
    this.#cleanupDb();
    await this.start();
  }

  async stop() {
    await this.#killProc();
    this.#cleanupDb();
  }
}
