# Final source validation

The final product executable is candidate 5.
Its SHA256 is `2c14d7d2f04865454ba9b1a41ba441ee7b69e2bad11a577f1056fe5b43c1324b`.
[candidate5-source.json](candidate5-source.json) records its 1,656 source file hashes.
[The final identity check](candidate5-final-identity-check.json) finds no source mismatch and confirms the same executable hash.
The source includes the earlier uncommitted logical-model work and this selected-schema implementation.
The committed base alone does not identify the candidate.

The release build passes:

```sh
scripts/ci/with-fold-host-cargo-lock.sh --timeout 600 -- \
  cargo build -p lastdb_node --bin lastdbd --release
```

[The build log](candidate5-build.stderr) records successful completion with no warnings.
The earlier candidate 5 build emits two unused-import warnings.
The final source removes those imports and the final release build follows that change.
[The initial log](candidate5-initial-build.stderr) remains diagnostic evidence.

The final lint check passes with warnings treated as errors:

```sh
scripts/ci/with-fold-host-cargo-lock.sh --timeout 600 -- \
  cargo clippy -p fold_db -p lastdb_node --lib --bins -- -D warnings
```

[The lint log](candidate5-clippy.stderr) records successful completion.
`cargo fmt --all -- --check` and `git diff --check` also pass.
[The source check](source-checks-final.json) covers 110 changed Rust files and 348 touched functions.
It includes tracked and untracked source, with zero added lint allowances and no size failures.

These source checks do not establish runtime behavior.
The separate [product report](../../lastdb-schema-range-preload-results.md) records actual Mini calls, values, costs, and limits.
No test command or test suite supplies these results.
