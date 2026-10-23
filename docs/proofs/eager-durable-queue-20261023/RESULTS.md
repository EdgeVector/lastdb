# Durable write concurrency results

The queue change reduces the observed wait for concurrent batches.
The clean memory publication also removes the obsolete unflushed-record guards.
The latest capture does not meet the 20 ms average target or establish compaction progress during continuous writes.
PR #282 remains draft and unmerged.

All captures in this report precede Tom's 2026-10-23 CI-only instruction.
Further LastDB builds and benchmarks must use GitHub Actions.
The local build runner is removed, and the capture entry points require a GitHub-hosted Actions runner.
The owned local capture home is removed after the committed receipts are preserved.
`local-cleanup.json` records its exact path, owner, process check, and completed removal.

## Latest completed comparison

The source is `e6e31dce37a6562d261d874230d422c17193c81a`.
The control is durable main `9cb89c6776bd654aa8ac93b553db278e5938c33a`.
The `barrier-identity.json` receipt records both executable hashes and the checked implementation markers.
Both executables use the optimized `dev-release` profile.

Each comparison uses 1000 complete client calls across 64 schemas.
The four clients use disjoint schema sets.
The latency unit is the complete request; no value divides latency by its record count.
Other host work overlaps this capture, so the results describe those observed periods rather than an isolated source effect.

| Workload | Main average | Candidate average | Main P99 | Candidate P99 |
| --- | ---: | ---: | ---: | ---: |
| One client and one record per call | 24.60 ms | 42.95 ms | 63.88 ms | 106.63 ms |
| Four clients and eight records per call | 162.76 ms | 95.48 ms | 542.75 ms | 180.47 ms |

The next CI comparison uses eight records per call with both one client and four clients.
The Complete atom lifecycle work chat owns that comparison and the remaining metadata optimization.

## Queue and commit costs

The candidate combines the 1000 concurrent calls into 253 physical commits.
Of those commits, 247 contain four requests.
Main uses 1000 separate physical commits for the same workload.
Mean admission wait falls from 119.59 ms to 10.11 ms in these captured periods.

The candidate's mean physical commit takes 62.60 ms for this workload.
The earlier corrected-count capture attributes 41.11 ms of a 58.89 ms physical commit to metadata work.
Its final device drain takes 6.08 ms.
These costs require a measured metadata change before a new performance claim.

## Compaction during continuous writes

The sustained capture completes 4096 eight-record calls with zero client errors.
It averages 261.16 ms per complete call, with a P99 of 615.38 ms.
The host also runs another database capture and heavy LastSeek work during part of this interval.

Compaction returns nine retryable 503 responses during writes.
The write-end status reports zero successful maintenance runs.
Private preparation takes tens to hundreds of milliseconds in the trace, then finds the publication gate busy.
The count-underflow error from the intermediate revision does not recur.

Compaction succeeds after the client writes stop.
It reduces the current dependency count from 2582 to 61 and removes 972711700 bytes.
The disk inventory peaks at 3579183905 bytes and ends at 2626894600 bytes.
This result proves space release after the workload; it does not establish bounded waste during continuous writes.

## Disk recovery and memory checks

All measured calls return durable receipts.
The forced-stop reopen checks all final values and the author clock.
All 32768 measured mutation notices return exactly once after the sustained capture.
The erase checks also preserve Delete and Purge results after an immediate forced stop.
The separate large call preserves all 1025 mutation notices.

The same-key conditional capture runs 24 rounds with four clients.
Each round returns one durable success and three conflicts.
The memory capture completes 32 large batch reads with correct values and active read holds.
It purges 20756 warm paths and ends at 254736 bytes under its 262144-byte warm limit.
Held records temporarily exceed that warm limit; the limit does not describe all process memory.

The active-cap control recovers all 304 acknowledged mutations from 38 accepted calls.
Four calls receive the expected 507 capacity refusal.
The refused new keys remain absent and the recovered notices match exactly.
The capacity-refusal message still uses generic filesystem wording.

## Earlier observations and limits

The first comparison against the same durable-main revision observes 17.75 ms average and 38.99 ms P99 for candidate single-record calls.
Its main control observes 16.07 ms average and 32.96 ms P99.
The corrected-count repeat observes 25.54 ms average for the candidate and 27.61 ms for main.
The latest capture is slower for both programs during some periods.
The 20 ms average is therefore a target, not an established guarantee under this host load.

The host build lock excludes cooperating builds.
It does not exclude all disk activity; the receipts record that limit.
The recovery captures use process termination rather than physical power removal.
The source still requires the ordered publication and the final full-device flush before success.
All homes contain fresh generated data; the primary and archive remain unchanged.

Required CI passes for the measured source in [run 40006535358](https://github.com/EdgeVector/lastdb/actions/runs/40006535358).
Required CI also passes for the rebased proof head `0f33ee9f1` in [run 40010502360](https://github.com/EdgeVector/lastdb/actions/runs/40010502360).
The checks include format, strict workspace Clippy, size, and the existing source rules.
No tests or coverage checks run.
These checks do not supply the pending equal-size benchmark or the continuous-write compaction result.

## Next actions

1. Integrate the metadata change with the rebased PR and preserve the disk barriers.
2. Run the equal-size client comparison in GitHub Actions.
3. Check automatic tier compaction and explicit full compaction during writes in CI.
4. Require active compaction progress and disk-space relief before the final merge review.

The source review is in `SOURCE_REVIEW.md`.
The raw capture receipts use gzip where their size exceeds 64 KiB.
The `compressed.json` receipt records the original byte counts and hashes.
