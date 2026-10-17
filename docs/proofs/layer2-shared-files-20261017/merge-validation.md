# Layer 2 merge validation

Date: 2026-10-17.

The storage branch includes current main through `5f64d115`. Merge commit `908f355c` keeps the native batch path and main's write conflict rules.

One `BatchSlotHeads` instance carries accepted heads across a native request. The common rebase helper selects winning sources before related molecule writes. The native path does not read the resident graph.

The document store now preserves the new write-capacity error as a distinct error. Its conversion from Last Store remains exhaustive.

## Local checks

The following checks pass on the combined source:

- Full workspace Clippy for libraries and binaries, with the two ONNX packages excluded.
- Separate Clippy for the two ONNX package binaries.
- Workspace format check.
- SDK type check, lint, and build after `npm ci`.
- Changed Rust function size, file size, and new-allow checks against `origin/main` at `5f64d115`.
- App registry shelf, write-path store, fastembed unification, workspace Clippy coverage, and hardcoded URL source checks.
- Whitespace check.

The size check examined 928 changed files. The allow check found no new allowances.

The earlier no-build-artifact check passed on the committed branch. The PR uses the required remote `ci-required` gate. No test or coverage command ran.

## Scope and limits

The original benchmark receipts remain in this folder. Their source reference is `5b918355`, before the merge from main.

The repeated range slowdown remains a disclosed limit. A separate investigation measures its cost. A proposed warm range set stays outside this merge.

No primary database, primary archive, shared daemon, or cloud service changed during these checks.
