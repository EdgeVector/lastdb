/**
 * App-side capability storage.
 *
 * The design mandates the OS keychain for capability tokens, with a file
 * fallback only on headless machines (`app_identity.md`, decision
 * 2026-05-27: "OS keychain mandated; file fallback only on headless Linux").
 *
 * We implement the keychain via the platform's native credential CLI rather
 * than a native node addon (keytar is unmaintained and a build-time burden):
 * - macOS: the `security` binary (`add-generic-password` / `find-generic-password`).
 * - Other platforms / headless: a `0o600` file under `$FOLDDB_APP_SDK_HOME`
 *   (default `~/.folddb-app-sdk/capabilities/`), the same SSH-like model the
 *   node uses when its `removed-keychain-feature` feature is off.
 *
 * ## Per-node keying (gap #2)
 *
 * A capability is minted by ONE node for ONE app. An app that connects to more
 * than one node (or whose node URL changes) must not replay node A's capability
 * against node B. So an entry is keyed by **(appId, nodeTarget)**, not by
 * `appId` alone, and the stored value is a small JSON envelope that *also*
 * records the `boundNode` it was minted for. `load(key, { expectedNode })`
 * returns the capability only when its `boundNode` matches the connection's
 * node; a mismatch (or a legacy bare-string entry from before this envelope)
 * is treated as **absent** (`null`), never blindly replayed. The composite key
 * already makes a cross-node hit structurally unlikely; the `boundNode` check
 * is the defense-in-depth that also catches a hand-injected or stale entry.
 *
 * The keychain **service name** is configurable per store (see
 * `ConnectOptions.keychainService`); the default keeps every existing entry
 * working.
 */

import { spawnSync } from 'node:child_process';
import { homedir, platform } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import {
  mkdirSync,
  readFileSync,
  writeFileSync,
  rmSync,
  existsSync,
} from 'node:fs';

import { CapabilityNotFoundError } from './errors.js';

/**
 * Options for a {@link CapabilityStore.load}. `expectedNode` is the canonical
 * target string of the connection asking for the capability (the transport's
 * `target`, e.g. `http://127.0.0.1:9101` or `unix:/path/to.sock`). When set,
 * a stored capability whose `boundNode` differs is treated as absent — the
 * wrong-node guard.
 */
export interface LoadOptions {
  /** The node the caller is currently connected to (transport `target`). */
  expectedNode?: string;
}

/**
 * A pluggable capability store. Entries are addressed by an opaque `key` the
 * client builds from `(appId, nodeTarget)` via {@link capabilityStoreKey}, so
 * the store itself never has to know the keying scheme. The default store is
 * {@link defaultCapabilityStore}.
 *
 * NOTE (v2 — gap #2): the interface keys by an opaque `key: string`, not by
 * `appId`. Pre-v2 the methods took an `appId`; the client now passes a
 * node-scoped composite key so a capability minted for one node is stored
 * separately from another node's. A custom store written against the old
 * single-arg shape only needs to treat the first arg as an opaque id.
 */
export interface CapabilityStore {
  /** Persist the base64 `capability` bound to `boundNode` under `key`. */
  store(key: string, capability: string, boundNode: string): Promise<void>;
  /**
   * Load the base64 capability under `key`, or `null` if none is stored —
   * including the wrong-node case (`options.expectedNode` set and the stored
   * `boundNode` differs).
   */
  load(key: string, options?: LoadOptions): Promise<string | null>;
  /** Remove any stored capability under `key` (idempotent). */
  remove(key: string): Promise<void>;
}

/** The default keychain service label all SDK entries live under. */
export const DEFAULT_KEYCHAIN_SERVICE = 'com.folddb.app-sdk.capability';

/** How long a keychain subprocess may run before we treat it as hung. */
const KEYCHAIN_TIMEOUT_MS = 5000;

/**
 * Thrown by {@link MacKeychainStore} when the current security session has no
 * usable default keychain — the exact `errSecNoDefaultKeychain` state that
 * makes `add-/find-generic-password` pop the blocking SecurityAgent modal
 * ("Keychain Not Found … Reset To Defaults"). We detect it with a GUI-less
 * `security default-keychain` probe and throw this BEFORE any interactive
 * call, so the modal is never triggered; {@link KeychainWithFileFallbackStore}
 * catches it and degrades to the file store. This is the common case when the
 * SDK runs in a process spawned without a login/Aqua session (an MCP server, a
 * launchd job, an SSH shell), where the on-disk default keychain config does
 * not resolve in-process.
 */
export class KeychainUnavailableError extends Error {
  constructor(detail: string) {
    super(`macOS keychain unavailable: ${detail}`);
    this.name = 'KeychainUnavailableError';
  }
}

/**
 * The outcome of one `security` CLI invocation, as the keychain store
 * consumes it. Mirrors the `spawnSync` fields the store reads — a custom
 * {@link SecurityRunner} fills the same shape.
 */
export interface SecurityRunResult {
  /** Process exit status. `null` when the process was killed (e.g. timeout). */
  status: number | null;
  /** Captured stdout (`''` when none). */
  stdout: string;
  /** Captured stderr (`''` when none). */
  stderr: string;
  /** Spawn-level failure (ENOENT, ETIMEDOUT, ...), when one occurred. */
  error?: Error;
}

/**
 * The injectable runner seam for {@link MacKeychainStore}: executes one
 * `security <args>` invocation and reports its outcome. The default runner
 * spawns the real CLI bounded by a hang timeout; a test suite injects a fake
 * to simulate a hung keychain prompt (`status: null` + `error`), a locked
 * keychain (non-zero `status`), or a missing item (`status: 44` on
 * `find-generic-password`) without touching a real keychain — the seam apps
 * like fbrain previously had to hand-roll (`__setSecuritySpawn`).
 */
export type SecurityRunner = (args: string[]) => SecurityRunResult;

/**
 * The real runner: `spawnSync('security', args)` bounded by
 * {@link KEYCHAIN_TIMEOUT_MS} — a hung keychain prompt must not wedge the
 * app (see the fbrain keychain first-write-hang lesson). A timeout kills the
 * subprocess and reports `status: null` + `error`.
 */
function spawnSecurityRunner(args: string[]): SecurityRunResult {
  const res = spawnSync('security', args, {
    timeout: KEYCHAIN_TIMEOUT_MS,
    encoding: 'utf8',
  });
  return {
    status: res.status,
    stdout: typeof res.stdout === 'string' ? res.stdout : '',
    stderr: typeof res.stderr === 'string' ? res.stderr : '',
    error: res.error,
  };
}

/**
 * Build the storage key for an app's capability on a specific node. The node
 * target is hashed (sha256, first 16 hex) and suffixed onto the app id so the
 * key stays a safe, bounded identifier regardless of the node URL/socket-path
 * shape, while remaining stable for the same (appId, nodeTarget) pair.
 *
 * Example: `capabilityStoreKey('fbrain', 'http://127.0.0.1:9101')` →
 * `fbrain@<16-hex>`.
 */
export function capabilityStoreKey(appId: string, nodeTarget: string): string {
  if (!/^[a-z][a-z0-9-]{0,39}$/.test(appId)) {
    throw new Error(
      `invalid app id '${appId}' (must match ^[a-z][a-z0-9-]{0,39}$)`,
    );
  }
  const nodeHash = createHash('sha256')
    .update(nodeTarget)
    .digest('hex')
    .slice(0, 16);
  return `${appId}@${nodeHash}`;
}

/**
 * The on-disk / in-keychain value: a versioned envelope so the bound node
 * travels with the capability and the wrong-node guard can compare it.
 */
interface StoredEnvelope {
  v: 1;
  capability: string;
  boundNode: string;
}

/** Serialize a capability + its bound node to the stored value. */
function encodeEnvelope(capability: string, boundNode: string): string {
  const env: StoredEnvelope = { v: 1, capability, boundNode };
  return JSON.stringify(env);
}

/**
 * Parse a stored value into `{ capability, boundNode }`, applying the
 * wrong-node guard. Returns the capability when it is usable for
 * `expectedNode`, or `null` when the entry is for a different node. A
 * legacy bare-string value (pre-envelope) has no `boundNode`, so it is
 * accepted only when no `expectedNode` is requested — otherwise it is
 * treated as absent (we cannot prove it was minted for this node).
 */
function decodeEnvelope(
  raw: string,
  expectedNode: string | undefined,
): string | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    parsed = undefined;
  }
  if (
    parsed &&
    typeof parsed === 'object' &&
    (parsed as StoredEnvelope).v === 1 &&
    typeof (parsed as StoredEnvelope).capability === 'string'
  ) {
    const env = parsed as StoredEnvelope;
    if (expectedNode !== undefined && env.boundNode !== expectedNode) {
      return null; // wrong-node: treat as absent
    }
    return env.capability;
  }
  // Legacy bare-string value (no bound node recorded). Usable only when the
  // caller is not asserting a specific node.
  if (expectedNode !== undefined) {
    return null;
  }
  return raw.trim();
}

/**
 * macOS Keychain store via the `security` CLI. Each call is bounded by a
 * timeout — a hung keychain prompt must not wedge the app (see the fbrain
 * keychain first-write-hang lesson). On any keychain failure the caller falls
 * back to the file store.
 *
 * The CLI is invoked through an injectable {@link SecurityRunner} (default:
 * the real `spawnSync('security', ...)` bounded by the hang timeout) so an
 * app's test suite can simulate a hung/locked/missing keychain without a
 * real one.
 */
export class MacKeychainStore implements CapabilityStore {
  private readonly run: SecurityRunner;

  /**
   * Cached result of the non-interactive default-keychain probe. `undefined`
   * until first checked; thereafter `true`/`false` for the lifetime of the
   * store (whether a session has a default keychain does not change under us).
   */
  private defaultKeychainOk?: boolean;

  /**
   * @param service the keychain service label entries live under.
   * @param runner OPT-IN testability seam — overrides how the `security`
   *   CLI is executed. Omit for the real CLI.
   */
  constructor(
    private readonly service: string,
    runner?: SecurityRunner,
  ) {
    this.run = runner ?? spawnSecurityRunner;
  }

  /**
   * Throw {@link KeychainUnavailableError} if this security session has no
   * default keychain — checked with `security default-keychain`, a read that
   * returns `errSecNoDefaultKeychain` on stderr WITHOUT a GUI prompt (unlike
   * `add-/find-generic-password`, which pop the blocking "Keychain Not Found"
   * modal in that state). Running this first means we never trigger the modal:
   * we either confirm a keychain exists and proceed, or bail to the typed
   * error the fallback store handles. The result is cached so a store/load
   * pair costs one extra probe at most, not one per op.
   */
  private ensureKeychainAvailable(): void {
    if (this.defaultKeychainOk === undefined) {
      const res = this.run(['default-keychain']);
      // status 0 → a default keychain resolves. Anything else — the
      // errSecNoDefaultKeychain that drives the modal, or a probe timeout —
      // means we must not make an interactive call in this session.
      this.defaultKeychainOk = res.status === 0;
    }
    if (!this.defaultKeychainOk) {
      throw new KeychainUnavailableError(
        'no default keychain in this security session ' +
          '(an interactive keychain call here would prompt the ' +
          '"Keychain Not Found" dialog)',
      );
    }
  }

  store(key: string, capability: string, boundNode: string): Promise<void> {
    this.ensureKeychainAvailable();
    // -U updates an existing item in place rather than erroring on duplicate.
    const res = this.run([
      'add-generic-password',
      '-a',
      key,
      '-s',
      this.service,
      '-w',
      encodeEnvelope(capability, boundNode),
      '-U',
    ]);
    if (res.status !== 0) {
      throw new Error(
        `security add-generic-password failed: ${securityFailureText(res)}`,
      );
    }
    return Promise.resolve();
  }

  load(key: string, options: LoadOptions = {}): Promise<string | null> {
    this.ensureKeychainAvailable();
    const res = this.run([
      'find-generic-password',
      '-a',
      key,
      '-s',
      this.service,
      '-w',
    ]);
    if (res.status === 0) {
      return Promise.resolve(
        decodeEnvelope(res.stdout.trim(), options.expectedNode),
      );
    }
    // Exit 44 = item not found; any other non-zero is a real keychain error.
    if (res.status === 44) {
      return Promise.resolve(null);
    }
    throw new Error(
      `security find-generic-password failed: ${securityFailureText(res)}`,
    );
  }

  remove(key: string): Promise<void> {
    this.ensureKeychainAvailable();
    this.run(['delete-generic-password', '-a', key, '-s', this.service]);
    // delete is best-effort: a missing item is success for our purposes.
    return Promise.resolve();
  }
}

/** Render a failed {@link SecurityRunResult} for an error message. */
function securityFailureText(res: SecurityRunResult): string {
  const stderr = res.stderr.trim();
  if (stderr.length > 0) {
    return stderr;
  }
  return res.error?.message ?? `exit status ${res.status ?? 'null (killed)'}`;
}

/**
 * File-backed store: a `0o600` file per (app, node) key. The fallback for
 * headless machines and non-macOS platforms (and the macOS path when the
 * keychain CLI errors). Honors `$FOLDDB_APP_SDK_HOME` for the base directory.
 */
export class FileCapabilityStore implements CapabilityStore {
  private readonly dir: string;

  constructor(baseDir?: string) {
    const base =
      baseDir ??
      process.env.FOLDDB_APP_SDK_HOME ??
      join(homedir(), '.folddb-app-sdk');
    this.dir = join(base, 'capabilities');
  }

  private pathFor(key: string): string {
    // The composite key is `^[a-z][a-z0-9-]{0,39}@[0-9a-f]{16}$` when built by
    // `capabilityStoreKey`; guard against a caller passing something that
    // isn't a safe filename.
    if (!/^[a-z][a-z0-9-]{0,39}(@[0-9a-f]{1,64})?$/.test(key)) {
      throw new Error(
        `invalid capability key '${key}' (must be an app id, optionally @<node-hash>)`,
      );
    }
    return join(this.dir, `${key}.cap`);
  }

  async store(
    key: string,
    capability: string,
    boundNode: string,
  ): Promise<void> {
    const p = this.pathFor(key);
    mkdirSync(this.dir, { recursive: true, mode: 0o700 });
    writeFileSync(p, encodeEnvelope(capability, boundNode), { mode: 0o600 });
  }

  async load(key: string, options: LoadOptions = {}): Promise<string | null> {
    const p = this.pathFor(key);
    if (!existsSync(p)) {
      return null;
    }
    return decodeEnvelope(readFileSync(p, 'utf8').trim(), options.expectedNode);
  }

  async remove(key: string): Promise<void> {
    rmSync(this.pathFor(key), { force: true });
  }
}

/**
 * Keychain-first store with a transparent file fallback: on macOS it tries
 * the Keychain and degrades to the file store if the `security` CLI errors
 * (locked keychain, headless, no GUI session). On every other platform it is
 * the file store directly.
 */
export class KeychainWithFileFallbackStore implements CapabilityStore {
  private readonly keychain: CapabilityStore | null;
  private readonly file: FileCapabilityStore;
  /** Whether this store has already logged a keychain-fallback warning. */
  private warned = false;

  /**
   * Emit a one-time, non-fatal note to stderr explaining why we fell back to
   * the file store, then go quiet for the rest of this store's life so a
   * keychain-less session (the common MCP-server / launchd case) is visible
   * once instead of either silent or — far worse — surfaced as the opaque OS
   * "Keychain Not Found" modal. stderr (not stdout) keeps it clear of an MCP
   * stdio JSON-RPC channel.
   */
  private warnFallback(op: string, err: unknown): void {
    if (this.warned) {
      return;
    }
    this.warned = true;
    const reason =
      err instanceof KeychainUnavailableError
        ? err.message
        : `keychain ${op} failed (${
            err instanceof Error ? err.message : String(err)
          })`;
    process.stderr.write(
      `[folddb-app-sdk] ${reason}; using the 0600 file capability store instead.\n`,
    );
  }

  /**
   * @param baseDir override the file-fallback base directory.
   * @param keychainService the macOS Keychain service label entries live under
   *   (default {@link DEFAULT_KEYCHAIN_SERVICE}). Configurable so two apps /
   *   environments can keep their keychain namespaces apart.
   * @param securityRunner OPT-IN testability seam — overrides how the
   *   keychain's `security` CLI is executed (see {@link SecurityRunner}).
   *   When injected, the darwin platform gate is skipped so the keychain
   *   path (and its file fallback on hang/failure) is exercisable on any OS;
   *   omit for the real CLI on macOS / file-only elsewhere.
   */
  constructor(
    baseDir?: string,
    keychainService: string = DEFAULT_KEYCHAIN_SERVICE,
    securityRunner?: SecurityRunner,
  ) {
    this.file = new FileCapabilityStore(baseDir);
    this.keychain =
      securityRunner !== undefined
        ? new MacKeychainStore(keychainService, securityRunner)
        : platform() === 'darwin'
          ? new MacKeychainStore(keychainService)
          : null;
  }

  async store(
    key: string,
    capability: string,
    boundNode: string,
  ): Promise<void> {
    if (this.keychain) {
      try {
        await this.keychain.store(key, capability, boundNode);
        return;
      } catch (err) {
        this.warnFallback('store', err);
        // fall through to file store
      }
    }
    await this.file.store(key, capability, boundNode);
  }

  async load(key: string, options: LoadOptions = {}): Promise<string | null> {
    if (this.keychain) {
      try {
        const fromKeychain = await this.keychain.load(key, options);
        if (fromKeychain !== null) {
          return fromKeychain;
        }
      } catch (err) {
        this.warnFallback('load', err);
        // fall through to file store
      }
    }
    return this.file.load(key, options);
  }

  async remove(key: string): Promise<void> {
    if (this.keychain) {
      try {
        await this.keychain.remove(key);
      } catch (err) {
        this.warnFallback('remove', err);
        // best-effort
      }
    }
    await this.file.remove(key);
  }
}

/**
 * The store `connect` uses when none is supplied. `keychainService` overrides
 * the macOS Keychain service label (default {@link DEFAULT_KEYCHAIN_SERVICE}).
 */
export function defaultCapabilityStore(
  keychainService: string = DEFAULT_KEYCHAIN_SERVICE,
): CapabilityStore {
  return new KeychainWithFileFallbackStore(undefined, keychainService);
}

/**
 * Convenience: load a capability or throw {@link CapabilityNotFoundError}.
 * Used where a missing capability is a hard error (vs. `load` which returns
 * `null`). `appId` is for the error message only; `key` is the actual store
 * key (which may be node-scoped).
 */
export async function loadCapabilityOrThrow(
  store: CapabilityStore,
  key: string,
  options: LoadOptions & { appId?: string } = {},
): Promise<string> {
  const cap = await store.load(key, options);
  if (cap === null) {
    throw new CapabilityNotFoundError(options.appId ?? key);
  }
  return cap;
}
