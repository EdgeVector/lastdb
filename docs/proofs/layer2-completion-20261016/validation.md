# Local candidate checks

Final product source: `7110c7f5`, including event batches and complete explicit-key warm reads.
Optional query metadata stays within the call budget and can decline admission.
The required data read retains its strict limits.
Complete write preparation uses one held view with a separate 40,000-key work budget.
Queries retain 10,000 keys; physical batches retain 10,000 slots.
The write owner cannot lend its larger budget to a later query.

Fresh native homes publish feature `0x20` before admission and permit 16,384 ordered stored operations.
Existing homes and standalone artifacts retain 4,096. The batch byte cap stays at 128 MiB.
Changed-key write preparation uses grouped reads for tips, delete barriers, and molecule generation pointers.
A new hash group writes its sorted locator once. Existing locator trees keep their original update path.

Later receipt and status documents do not change the product binaries.
The receipts identify each exact executable by SHA-256.
The [final build record](final-build.json) lists the clean final node version and exact binary hashes.

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| Product Clippy, warnings denied | Pass |
| Optimized daemon, CLI, disk-home, recovery, and maintenance builds | Pass |
| Changed Rust file size | Pass |
| Changed Rust function size | Pass |
| New lint allowances | Pass; none added |
| Sensitive trace fields | Pass |
| UTF-8 byte slices | Pass |
| HTTP egress classification | Pass |
| Background task trace context | Pass |
| Hardcoded URLs | Pass |
| Write-path synchronous-store rule | Pass |
| Whitespace and patch integrity | Pass |

Heavy Cargo commands use `scripts/ci/with-fold-host-cargo-lock.sh --timeout 600 --`.
The product commands use `--locked`.

```text
cargo clippy -p lastdb_node --lib --bins -p lastdb_host -p fold_db -p laststore --locked -- -D warnings
cargo build --release -p lastdb_node --bin lastdbd --bin lastdb -p laststore --bin lastdb-disk-home --bin lastdb-disk-recovery --bin lastdb-pack-maintain --locked
python3 scripts/lints/lint-diff-file-size.py --base 5c0a415a --head HEAD
python3 scripts/lints/lint-diff-fn-size.py --base 5c0a415a --head HEAD
python3 scripts/lints/lint-diff-new-allows.py --base 5c0a415a --head HEAD
```

The local observability check found pre-existing HTTP client classifications and background task span gaps.
The candidate adds the classifications and trace context, then passes both checks.
These changes add no network call or shared-system change.

These are local build and source checks.
The separate product receipts cover the actual record and recovery results.
No test, fixture harness, coverage command, primary node, primary copy, archive, or cloud account participates.
Native cloud configuration remains refused.
