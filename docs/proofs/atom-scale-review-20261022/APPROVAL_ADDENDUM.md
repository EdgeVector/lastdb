# Final source and product approval

Date: 2026-10-22. Reviewer: the independent Codex scale agent.

I approve source and product revision `bb7a4a7f5219265cb660ba8084b72fdbb2833554`.
No further reviewer block remains.
The final PR head still requires green CI before merge.

The parent checkout `c8ca55b177d10b938e4d11e2d06c142e0ee3b9ed` adds only the independent review evidence to that source.
I checked the final `run-5` and `automatic-final` inputs, request receipts, summaries, and cleanup receipts.
Both name source revision `bb7a4a7f5` and the same corrected daemon.
The daemon SHA-256 is `359ec9b3f1b3348413e39c0deab1ea78c34b59e1812b1ad103d8e6f1684b9d8a`.
I also checked that hash against the executable.
The recorded driver hashes match the source files I read.

The lifecycle capture completed every exact readback phase and ended with 4,090 live user records.
The driver creates its first long key before the full copy.
It updates only that key's value before each later recent copy.
This exercises the recent-page path with an old key overflow chain.
Recent copies completed, and all accepted values remained correct.

The data-only backup completed during a copy and excluded identity files.
The bundle restore, graceful reopen, forced reopen, and interrupted-copy reopen completed their value checks.
The TTL sweep expired 24 generated records while eight foreground updates completed.
Schema drop and molecule drain left no candidates.
The complete verifier read 24,546 counters and 16,360 tips with zero mismatches.

The final automatic worker refused one final delta above its bounded budget.
It retried, reduced 263 selected parts to 12, and accepted a later durable update.
The final selected count was 24, and `writer_failed` remained false.
Its complete verifier read 24,582 counters and 16,384 tips with zero mismatches.

Both captures report complete success and stopped owned processes.
Their cleanup receipts match the original ownership records and summary paths.
Both receipts confirm removal of the exact owned temporary homes.

The real 85-percent threshold and a configured disk-cap stress case remain unexercised.
These captures establish no power-loss result or production latency bound.
The earlier independent large-home timings remain tied to frozen `43ca120`.
The corrected binary's functional result satisfies the product condition in the earlier review.
