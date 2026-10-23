# Independent profile and AUTO-on method review

This review uses local tool output and source inspection. It does not start a LastDB node or a build.

## Available profile route

`/usr/bin/sample` successfully sampled one owned temporary Python process for one second.
The temporary output and its directory are removed.
`xctrace` requires full Xcode; this host selects `/Library/Developer/CommandLineTools`.
No tool installation or developer-directory change is required for the sample route.

The existing optimized release executable retains native symbols for `resolve_pending`, `payload::resolve`, `observe_new_parts`, and `publish_prepared`.
It has no DWARF sections. Function stacks can support attribution, but inline frames and source lines can remain absent.
Use the exact copied release executable and record its version, SHA-256, and Mach-O UUID.
Do not change debug or optimization flags for the matched performance binaries.
`dsymutil` cannot create source information that the compiler did not emit.

## What the profile can establish

Capture the draft first, with automatic maintenance off, on a fresh owned generated home.
Use consecutive durable batches of 32 after the seed, with request intervals and ordered input hashes.
Record the sample command, exact target identity, capture interval, and completed or overlapping request count.
A fixed mutation count gives equal final histories; a fixed capture duration can contain different request counts.
Keep profiled latency separate from the unprofiled matched benchmark.

Classify symbolized stacks by both context and leaf operation.
Separate count-path hash/map work, count-path file access, durability calls, and lock or idle waits.
Keep unknown frames and do not count inclusive parent and child weights twice.
Sample weights describe wall-clock stack residency across threads. They are not CPU percentages or physical disk activity.
Kernel process counters provide the separate CPU observation.

On this host, `ri_user_time` and `ri_system_time` use Mach ticks.
Capture `mach_timebase_info` and convert the raw delta with `numer / denom / 1e9` for seconds.
The observed timebase is `125 / 3`; dividing a raw counter by `1e9` understates CPU by about 41.67 times.
An owned-process observation converted 711,772 raw ticks to 29,657,167 ns.
The bracketing `process_time_ns` values were 29,609,000 and 29,666,000 ns.
Keep the original raw counters in each receipt.

## AUTO-on comparison

The reviewed method uses exactly 330 durable batches of 32 on a two-second nominal cadence.
It follows the last request with a fixed 60-second quiet tail.
All three variants use the same 64-key seed, ordered changes, and total mutation count.
Do not skip late requests. Record nominal time, actual time, lateness, and actual active completion.

Keep default maintenance settings: 30-second startup delay, zero grace, high-water mark 4,096, and 600-second orphan minimum age.
Capture logical and allocated file sizes, file families, native maintenance status, and process memory every 30 seconds.
Retain exact reads and ranges, resource bounds, process ownership, and cleanup receipts.
Record pack-run changes or worker events with request intervals before an attribution of a long write to a pack.
Use a write timeout that permits a compaction pause; a short metadata-sample deadline must not truncate write latency.

The 660-second active interval and quiet tail cover startup and the first default orphan repeat.
They do not cover two complete orphan-age cycles or establish steady-state disk use.
The visible key set can stay fixed while stored atom-key tombstones grow.
One long run per variant is descriptive evidence, with shared-host order and cache uncertainty.

No primary, archive, shared service, or test suite is part of this method.
