# Non-test validation status

This report distinguishes root-reported checks from operational receipts and final required CI.
No test suite, fixture harness, or test-coverage check runs in this work.
The primary, archive, brain, situations, and kanban stay unused.

## Committed discovery source

Source commit: `7a7a7ab74595c277766eb33c1603e0d991daa530`.
The root agent reports these checks pass for that committed source:

- Core and node library/binary clippy: 38.98 seconds.
- Function-size check and file-size check: 229 changed files.
- New-allow check: zero new allow entries.
- Write-path synchronization source check.
- Workspace fastembed source check.
- Heavy-clippy workspace coverage: 23 of 23 packages.
- App registry source check.
- Staged artifact check and format check.

The v3 release build passes in 2 minutes 4 seconds.
The executable SHA is `8cff7b4c56c946e98f31b6b8e38eec09f4f5046db3b9b2c7d300bc79df74eac1`.
All 1,931 source inputs, host settings, and prior identities match after the build.
An exact Git check matches each input against the committed tree `cda093605abc68ec6dcf8bb18a45e3acc4d438d7`.
The [Git verification](candidate-v3-committed-source-verification.json) records those results.
The strict scoped workflow still fails one file-count gate. Its diagnostic values do not establish acceptance.
This discovery source is not yet the final benchmark candidate.

The root later reports full workspace clippy passes in 41.61 seconds, with the existing 23-package default lane.
The separate off-lane binary clippy check passes in 0.31 seconds.
SDK typecheck, lint, and build checks also pass.
The final source still requires its own release identity and final committed-tree checks.

## V6 product source

Final product source commit: `7876211b6d64dff43ceddd602656017b74860fe9`.
Its Git tree is `ff994d7740590578d52e2ed85418896fb236db32`.
The release build passes in 2 minutes. Its executable SHA is `c19a25ea358b06dea5a0612e206524f4e282d5de9ff0eca46570c592b57b6564`.
All 1,931 source inputs, host settings, and prior identities match after the build.
The [committed-source check](candidate-v6-committed-source-verification.json) matches every product input against that exact Git tree.

The root reports these V6 checks pass:

- Core and node library/binary clippy: 38.83 seconds.
- Function-size and file-size checks: 574 changed files.
- New-allow check: zero new allow entries.
- Staged artifact and whitespace checks.
- Format check for the final metadata-accounting source change.
- Independent review of the final metadata-accounting change, with no blocker.

The exact V6 source passes the full workspace clippy check in 40.02 seconds.
That check uses the existing 23-package default lane and excludes `generate_validated_schema_org_seeds` and `schema_service_worker`.
The separate exact-V6 off-lane binary clippy check passes in 0.46 seconds. The full workspace format check also passes.
SDK typecheck, lint, and build pass before the final Rust-only change. SDK source stays unchanged.
The final report update changes only documentation, drivers, and receipts. Product source stays at the verified V6 commit.
The root repeats the function-size, file-size, and new-allow checks against merged main. All pass; zero new allows exist.
The staged reports pass the artifact and whitespace checks. The V6 workspace format result remains valid for unchanged product source.
This task creates no new PR or merge.
No shared policy, CI, instruction, or deployment change is part of this proof.

The final ordinary API matrix passes all seven supported workflows.
The separate ACK discovery passes all five warm HashRange updates with zero model refusals and pending work after ACK.
All eight matched cases pass, with 696 read checks and 112 write checks.
All 1,931 product inputs, host settings, prior identities, and the executable match again after the workloads.
The [post-timing check](v6-post-timing-source-verification.json) records that final source comparison.

Operational API receipts must pass their own exact value, membership, memory charge, and file-call gates.
A source check does not establish a live product result.
Diagnostics that continue a failed file-count gate remain separate from strict acceptance.
Raw replay remains an explicit native activation limit unless the supported product path changes.
