# Capture plan

The baseline is main `bcdcbba38`.
The candidate uses the same main plus the eager durable queue change.
Both programs use the optimized `dev-release` profile.

All daemon homes contain fresh generated data.
No primary data or archived data enters this work.
The host Cargo lock excludes cooperative builds during each capture.
Other host work remains outside this control.

The capture uses 64 schemas with distinct field descriptions.
Four clients use disjoint schema sets.
Each batch contains eight records.
Each single-client call contains one record.
The measured latency covers the complete client request.
No latency value is divided by the number of records.

The matched comparison uses 1000 calls for each program and workload.
A separate trace capture checks the sequence coverage of shared commits.
The sustained capture uses 4096 calls and repeated explicit compaction.
Disk inventory uses the exact owned home and a six-level bound.

Each workload checks values by known keys.
A forced stop follows a durable sentinel ack.
The reopened daemon checks all final values and the author clock.
Every measured mutation notice must appear exactly once after the reopen.

A separate capture uses same-key create-if-absent calls.
It also uses a 256 KiB warm-memory limit and concurrent large batch reads.
The capture checks values, read holds, purge counts, and the final warm size.

The source checks use format, strict Clippy, size, and existing source lints.
The work does not add or run tests.

The final campaign repeats both controls and the revised candidate.
It also checks an active 1 GiB disk cap on another generated home.
The production cap and grace policy stay unchanged.
The explicit compaction capture uses zero grace for prompt space release.
Retained live mutations and notices can still increase the total disk size.
