## Summary

Reads and accepted writes now share the core logical model for schema and molecule definitions, current keys and tips, confirmed absence, and full atom values. Native packed storage is the sole logical engine for normal startup and direct constructors. The private native queue holds prepared disk output and resource owners until flush; ordinary reads cannot use it.

This removes the old serving graph, duplicate logical caches, alternate write publication path, engine/mode controls, and unreachable historical query branches. Public historical requests still receive explicit errors. Event inspection, conflict records, and purge traversal remain. Current instructions, telemetry, and design documents now describe the implemented model; old proposals are marked historical.

Captured reads retain their memory charges and disk roots. Warm secondary fields retain the selected physical atom location. Raw operations that lack complete typed coverage stay private until disk commit. Selected HashRange schemas retain complete candidate keys while tips and bodies remain partial.

Empty homes initialize directly. Older nonempty formats receive an error. No migration code is added. Cloud activation remains disabled throughout startup, enrollment, attach, and resume paths.

## Validation

- Workspace clippy and separate off-lane binary clippy pass with warnings denied. Format, Rust file/function size, and allow checks pass; zero allows added and seven removed.
- Write-path synchronization, app registry, dependency, and workspace-clippy coverage checks pass.
- A normal default release and frozen synthetic API workflows validate the final cleanup source. Exact source, executable, driver, and receipt identities are in [the final merge review](docs/proofs/core-clarity-20261018/MERGE-REVIEW.md).
- All 124 keyed read checks pass, including 66 required zero-file checks, across Hash, HashRange, proteins, and named databases. The ten node sessions stop normally.
- Earlier matched benchmarks remain specific to their recorded source. This cleanup adds no new performance comparison.

Validation uses builds, lints, and ordinary synthetic API calls under the repository's no-tests rule. Cloud transfer and a primary installation are outside this change. The logical record cap and separate memory allowances do not define a whole-process memory maximum.
