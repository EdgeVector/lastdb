# Local candidate checks

Product source: `55c6d9aed`, including implementation commit `b0679bb7`.
Later receipt and status documents do not change the product binaries.
The receipts identify each exact executable by SHA-256.

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
