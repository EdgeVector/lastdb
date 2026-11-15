# schema_service

Advisory Schema Service of the `EdgeVector/lastdb` workspace
(`crates/{schema_types,core,server_lambda}`).
Cloud-only: deployed as the AWS Lambda behind `schema.folddb.com` via
`EdgeVector/schema-infra`. Serves `/v1/health`, `/v1/fields/vocabulary`, and
deterministic `POST /v1/advice/schema` from the built-in canonical field
vocabulary (no ONNX, no S3/DynamoDB/SQS, no HTTP dev binary).

## Canonical commands

**No tests** (Tom, 2026-10-09). The repo has no test suite.
Do not write, add, run, restore, or require tests.
Remove test and test coverage requirements when you find them.
Use build, format, and lint checks.
Situation: `no-tests-all-repos-20261009`.

```bash
cargo build --workspace
cargo clippy --workspace --lib --bins -- -D warnings
cargo fmt --all
```

CI lints (canonical script in the monorepo root; run against just this crate):

```bash
python3 ../scripts/lints/source_lints.py --scope=schema_service/crates
```

PR / merge: `gh pr create -R EdgeVector/lastdb --base main --head <branch>`.
Use `gh pr merge <PR> -R EdgeVector/lastdb --auto --squash` after the required
`ci-required` check succeeds.

See also: `README.md`.
