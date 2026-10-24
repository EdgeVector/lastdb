# Capture plan and validation policy

## Current policy

Tom requires GitHub Actions for all further LastDB builds and benchmarks from 2026-10-23.
Local builds, benchmarks, Cargo checks, Clippy, and host measurement locks are retired.
The local build runner is removed.
The capture entry points require a GitHub-hosted Actions runner.
No local replacement capture is permitted while CI waits.
Raw output stays outside the source tree and Git.
CI must upload the output as a workflow artifact with a verified download URL, checksum, source revision, and retention period.

The Complete atom lifecycle work chat owns the equal-size comparison and the narrow metadata change.
The next comparison uses eight records per request with one client and four clients.
It must record source revisions, executable hashes, complete request latency, metadata costs, and disk receipts.
It must use the same build profile and request shape for each source.
The workload must also establish automatic and explicit compaction progress during client writes.
The PR remains draft until CI supplies these results and the final review accepts them.

## Historical capture method

The archived captures precede the CI-only instruction.
The first control is main `bcdcbba38`; the latest completed control is main `9cb89c6776bd654aa8ac93b553db278e5938c33a`.
Both programs use the optimized `dev-release` profile.
All daemon homes contain fresh generated data.
No primary data or archived data enters this work.
The former host lock excludes cooperative builds but does not exclude other host work.

Each source comparison uses 1000 calls across 64 schemas with distinct field descriptions.
Four clients use disjoint schema sets and eight records per request.
The single-client workload uses one record per request.
These workloads therefore do not establish an equal-size one-client versus four-client comparison.
The measured latency covers the complete request and never divides by its record count.
The sustained capture uses 4096 calls and repeated explicit compaction.
Disk inventory uses the exact owned home and a six-level bound.

Each workload checks values by known keys after a durable sentinel ack, forced stop, and reopen.
The reopened daemon checks the author clock and every measured mutation notice exactly once.
Separate captures check same-key conditions, active read holds, warm-set purge, and an active 1 GiB disk cap.
The explicit compaction capture uses zero grace for prompt space release.
Retained live mutations and notices can still increase the total disk size.

Local lightweight source, syntax, and format checks remain permitted.
CI supplies build, Clippy, and benchmark validation.
The work does not add or run tests.
