# Bounded tips-plane compaction — real-data CoW proof

`scripts/run-tips-compaction-memory-probe.sh` measures what one plane
compaction costs in `phys_footprint` on a copy-on-write clone of the real
primary home. This document records the method and the measurement.

## Why a second proof exists

PR #1849 rewrote plane compaction to work one bounded frame at a time, and
proved it with `vendor/laststore/tests/compact_memory_bound.rs`: peak live heap
stays flat as a generated plane grows (old x4.00 with the plane, new x1.34).
That is a real regression bar and it is red on the old code.

It is not the claim the memory guard kills over. The guard enforces
`LASTDBD_RSS_LIMIT_MB` (16384 MiB, metric `footprint`) against the *whole
process*, and the failure it produced on 2026-08-31 was specific: the primary
sat at a ~14.4 GiB steady baseline, the automatic tips rewrite added ~2.9 GiB
of transient footprint, and the guard SIGKILLed `lastdbd` mid-rewrite at
02:39:08Z, 05:03:47Z and 06:06:42Z. One rewrite completed at 07:16:44Z on a
fresher 13.3 GiB baseline and peaked at 16314 MiB — 70 MiB under the kill line.
A rewrite the guard interrupts reclaims nothing, so the overhang survives and
the next probe repeats the kill.

A synthetic heap-accounting test cannot answer "does the primary's 11M-key,
5.3 GiB tips plane now rewrite under that guard". Only a measurement on that
plane can. That is what this harness is for.

## Method

The probe never touches the live primary.

1. **Clone.** APFS `clonefile` (`cp -cR`) of `~/.lastdb`, skipping subtrees a
   plane compaction never reads — chiefly the ~126k-file published `apps/`
   asset tree. What is skipped is logged, never silent. Planes are cloned
   concurrently because clonefile is metadata-bound.
2. **Isolate.** `cloud_sync.json` and the inherited sockets are removed from
   the clone, so the probe can never join the real account's cloud sync or
   bind over the primary's socket. The external `lastdbd-memory-guard`
   LaunchAgent watches `LASTDBD_PRIMARY_HOME` only, so a probe node is never a
   guard target and may exceed the limit safely.
3. **Boot.** The candidate daemon runs against the clone on the clone's own
   socket, with the primary's `LASTDB_*` tuning mirrored out of its LaunchAgent
   (the 4 GiB hash-group warm budget alone changes the number). Home-shaped
   keys are excluded so the probe can only ever see its own `--data-dir`.
   Unattended tips compaction is disabled for the settle window, so the
   baseline is not read against a plane something already rewrote.
4. **Measure.** Footprint comes from the node's own `/api/status`
   (`phys_footprint_bytes`, and the kernel lifetime maximum
   `phys_footprint_peak_bytes`) — the same fields the external guard reads.
   `ps -o rss=` is not a substitute: it excludes compressed anonymous pages and
   has measured 6x under the footprint on this host. The lifetime peak is the
   authoritative number because a sampling gap cannot hide it; the 2-second
   samples exist only to show the *shape* of the spike.
5. **Compact.** `POST /api/db/compact` with `dry_run:false` on the owner
   socket. Operator-invoked compaction is deliberately not headroom-gated
   (PR #1848), so this measures the rewrite itself rather than the deferral.
6. **Read back.** Rows are queried through the compacted plane. A rewrite that
   loses records is not a pass.

## Reading the result against the guard

The probe boots cold, so its absolute footprint is nowhere near the primary's.
Simulating a 14 GiB baseline with ballast on a 36 GiB host, next to a live
14 GiB primary, would risk the primary itself — so the equivalent-headroom form
is used instead, exactly as the card allows:

```
spike_over_baseline = peak footprint during the rewrite − settled baseline
projected_primary_peak = primary steady baseline + spike_over_baseline
headroom = LASTDBD_RSS_LIMIT_MB − projected_primary_peak
```

The bar is that `headroom` is real headroom, not the 70 MiB of the 07:16:44Z
near-miss.

## First measurement — 2026-08-31, build 0.23.3-1402-g3b8db0be5

**The bar in the card is NOT met, and the run did not clear the fix.** This
section records what was measured and, just as importantly, what the numbers
cannot yet settle.

Candidate: `lastdbd 0.23.3-1402-g3b8db0be5`, built from a commit that contains
PR #1849's merge `bdc88b7636` (`git merge-base --is-ancestor` verified). The
`-dirty` suffix is this document and the probe script; no daemon source
differs from the merge.

Clone: pristine copy of `~/.lastdb`, 41,232 files, tips plane 5,281,021,952
bytes allocated (4.92 GiB, 2048 shard files). Primary tuning mirrored,
including `LASTDB_HASH_GROUP_WARM_BYTES=4294967296`.

| | |
|---|---|
| settled baseline footprint | 10,068,795,456 B — **9,602 MiB** |
| max sampled footprint during rewrite | 16,085,908,504 B — **15,341 MiB** |
| growth over baseline | 6,017,113,048 B — **5,738 MiB** |
| samples | 301, at 2 s |
| rewrite duration before it was stopped | **966 s**, not finished |
| plane before → after | 5,281,021,952 → 5,268,934,656 B |
| reclaimed | 12,087,296 B — **12 MB of 5.28 GB** |
| guard limit reported by the node | 17,179,869,184 B — 16,384 MiB |

The footprint curve is a monotonic climb, not a bounded plateau: 9,603 MiB at
t=0, 11,572 at t=42 s, 12,282 at t=249 s, 13,020 at t=430 s, 13,740 at t=533 s,
13,909 at t≈570 s, topping out at 15,341 MiB. Sixteen minutes in it had
reclaimed 12 MB and was still climbing.

Read against the guard, from the *fresh* 9.6 GiB baseline this probe had, the
process came within 1,043 MiB of the 16,384 MiB kill line. Projected onto the
primary's ~14.4 GiB steady baseline it does not fit at all.

### Why this is not yet a verdict on PR #1849

Three confounds, none of them small, and the first two are the reason this run
cannot be called a refutation:

1. **The machine was oversubscribed.** The probe ran beside the live primary on
   a 36 GiB host. At the end of the window the guard logged the primary at
   `footprint_mb=18734 swap_mb=16895`. Under that pressure macOS compresses and
   swaps, and `phys_footprint` counts compressed pages — so both processes'
   footprints read high for reasons that are not the rewrite. Some unknown part
   of the 5,738 MiB is this.
2. **The warm cache is not separated from the rewrite.** The node carries the
   primary's 4 GiB hash-group warm budget, and the rewrite touches every live
   key, so the cache fills as the rewrite proceeds. The guard does not care
   which allocation crossed its line, but anyone fixing this does. The run does
   not tell them apart.
3. **The rewrite was interrupted, so 5,738 MiB is a lower bound.** The probe's
   own watchdog stopped it (below).

What the run does establish, and what a re-measurement should start from: on
the real 11 M-key tips plane the bounded rewrite showed multi-GiB monotonic
footprint growth and had reclaimed 12 MB after 966 s. That is not the flat
profile `compact_memory_bound.rs` shows on a generated plane. Either the
synthetic bar does not capture what the real plane costs, or the cost is
elsewhere in the process — but "PR #1849 lets the primary's tips plane compact
under the guard" is not yet true, and must not be recorded as true.

### The probe restarted the primary — and now cannot

Fifteen minutes into the rewrite the primary's own memory guard fired:

```
2026-08-31T11:48:43Z OVER_LIMIT pid=48281 metric=footprint enforced_mb=18734
  limit_mb=16384 rss_mb=6529 swap_mb=16895
2026-08-31T11:48:43Z RESTART primary lastdbd (SIGTERM then kickstart)
2026-08-31T11:48:56Z kickstart agent=com.tomtang.lastdbd-primary-506
```

The guard did its job and the primary came back healthy in about a minute
(`kanban ping` 355 ms, footprint 8,517 MiB). But a probe that exists to ask a
question *about* the memory guard must not be the thing that trips it. The
harness now watches the live primary and kills its own node first
(`PROBE_PRIMARY_CEILING_MB`, default 15,000 MiB).

That ceiling is also the honest limit on this host: the probe needs ~15 GiB and
the primary runs at 14–15 GiB on a 36 GiB machine, so the two do not fit. A
completed, unconfounded measurement needs one of:

- the same probe on a host with materially more RAM, or
- a probe run with a reduced `LASTDB_HASH_GROUP_WARM_BYTES`, which separates the
  rewrite's own transient from the warm cache at the cost of no longer
  mirroring the primary exactly, or
- the primary stopped for the duration, which is a decision for Tom, not for a
  routine.

### One invalid run, recorded so it is not repeated

A rerun against the *reused* clone reported `live_keys: 2`, `bytes_before:
66619`, compacted in 0 s and deleted the 5.3 GiB plane as dead — a spectacular
false pass. Tip liveness is decided by the rest of the store, and the previous
interrupted rewrite had already moved that. Refreshing only the plane under
test is not enough. The harness now refuses a clone that has ever booted a node
and flags any run whose `live_keys` collapses.


## Second measurement — 2026-08-31, build 0.23.3-1403-g69dfc137b

**The rewrite still did not finish, so the card's bar is still not met.** But
the footprint number changed shape, and two harness defects that blocked the
whole method were found and fixed. Read this section together with the first
measurement; it does not replace it.

This run took the second option the first measurement listed: a reduced
`LASTDB_HASH_GROUP_WARM_BYTES`. The probe mirrored the primary's LaunchAgent
tuning and then overrode the warm budget from 4 GiB to 1 GiB, through
`PROBE_ENV_OVERRIDE`. The run therefore no longer mirrors the primary exactly.
That is the point: it separates the rewrite's own transient from the warm
cache, which the first run could not do.

Candidate: `lastdbd 0.23.3-1403-g69dfc137b`, built clean from `main` at
`69dfc137b`. That commit contains PR #1849's merge `bdc88b7636`.

Clone: 41,387 files, planes cloned in 403 s, tips plane 5,327,220,736 bytes
before the rewrite.

| | 4 GiB warm (first run) | 1 GiB warm (this run) |
|---|---|---|
| settled baseline footprint | 9,602 MiB | **6,391 MiB** |
| peak footprint during rewrite | 15,341 MiB | **7,764 MiB** |
| spike over baseline | 5,738 MiB | **1,373 MiB** |
| shape | monotonic climb, still rising | **plateau after ~60 s** |
| rewrite window | 966 s, stopped | 860 s, stopped |
| reclaimed | 12 MB of 5.28 GB | **0** |

The curve is the finding. The peak, 8,141,314,696 B, is a kernel lifetime
maximum reached at about t=60 s. After that the process is flat: over the 228
samples from t=120 s to the end, footprint held between 6,841 and 7,399 MiB,
median 7,293 MiB. Sampled every 2 s: 6,917 MiB at t=121 s, 7,342 at t=240,
7,288 at t=360, 7,324 at t=482, 7,294 at t=721, 6,985 at t=840.

That is a bounded plateau. It is what `compact_memory_bound.rs` predicts and
what the first run did not show.

### What this changes, and what it does not

The first run measured 5,738 MiB of growth and could not say how much of it
was the rewrite. This run says: with the warm budget cut by 3 GiB, the growth
falls by 4,365 MiB. So most of the first number was the 4 GiB hash-group cache
filling as the rewrite touched every live key, plus the swap and compression
the oversubscribed host added. The bounded rewrite's own transient, measured
alone, is about 1,373 MiB.

Read against the guard:

```
projected_primary_peak = 14,400 (primary steady baseline) + 1,373 = 15,773 MiB
headroom = 16,384 - 15,773 = 611 MiB
```

Treat that as an estimate, not a pass. It assumes the rewrite's transient is
the same at a 4 GiB warm budget as at 1 GiB, and this run cannot show that. It
also assumes the primary's steady baseline already holds a full warm cache. A
611 MiB margin is better than the 70 MiB of the 07:16:44Z near-miss, and it is
not a margin to ship on.

Two things this run still does not establish:

1. **The rewrite did not finish, and reclaimed nothing.** In 860 s the plane
   went 5,327,220,736 → 5,591,666,688 bytes. It grew by 264 MB, because the
   rewrite writes new shards before it releases old ones. A flat footprint over
   a rewrite that never completes is weaker evidence than a flat footprint over
   one that does.
2. **The read-back did not run.** The probe was stopped before that step.

### Why this run was stopped

The host, not the probe, ran out. With the probe at ~7.3 GiB the live primary
climbed to 15,080 MiB and swap reached 12,582 of 13,312 MiB. The primary then
degraded for every other agent on the machine: `/api/status` went from 0.32 s
to over 30 s, and `kanban ping` took 47,696 ms against a normal 355 ms.

The probe was stopped for that reason. The primary answered in 0.32 s again
immediately afterwards. Stopping was the correct trade: the primary's own
memory guard kills at 16,384 MiB, and a probe that exists to ask a question
about the guard must not be what trips it.

The conclusion of the first measurement therefore stands, and is now narrower.
A completed, unconfounded measurement needs a host with materially more RAM, or
the primary stopped for the duration. Reducing the warm cache was enough to
show the *shape* of the rewrite beside a live primary. It is not enough to run
that rewrite to completion beside one.

### Two harness defects found by running it

Both were merged in PR #1851 and both are fixed here. Each one made a step of
the method unable to do its job while still reporting normally.

**1. The read-back could never return a row.** The probe built
`{"type":"query","schema":S,"fields":["*"]}` and counted a `data` key. Measured
against the primary:

```
{"type":"query","schema":S,…}                          -> missing_required_key: schema_name
{"schema_name":S,"fields":["*"]}                       -> full_schema_scan_not_allowed
{"schema_name":S,"fields":["*"],"filter":{"HashKey":K}} -> no field(s): *
{"schema_name":S,"fields":[],"filter":{"HashKey":K}}    -> ok, results:[…]
```

LastDB is keyed-access only, so the read-back must name a key, ask for
`schema_name`, use `fields: []`, and read `results`. The old form could not
return a row for any input. "A rewrite that loses records is not a pass" was
the stated bar, and no run could have failed it. The probe now takes
`PROBE_READBACK_KEY`, which the usage comment already named but the code never
read, and it now separates a rejected query from a lost row.

**2. The primary watchdog died on its first poll.** `PROBE_PRIMARY_CEILING_MB`
was added after the first run restarted the primary. It logged
`primary watchdog armed at 15000MiB` and was already dead.

The watchdog subshell inherited the script's `set -euo pipefail` and read the
primary through `curl … | sed … | head -1` inside a command substitution. When
the host is under memory pressure the primary answers `/api/status` slowly,
`curl --max-time 5` exits 28, `pipefail` promotes 28 to the pipeline, and
`set -e` ends the subshell. Reproduced on this host: the loop exits before it
writes one line.

So the guard was inert in exactly the condition it exists for. This run
confirmed it live — the harness watchdog process was gone while the probe ran
beside a 14.8 GiB primary, and an external replacement had to be armed by hand.

The fix disables `errexit` and `pipefail` inside the watchdog, keeps every step
failure-tolerant, and treats an unreadable primary as the pressure signal it
is: it logs the blackout, and after 30 consecutive missed polls it stands the
probe down rather than run unwatched. `armed` now also means live — the
function checks that the subshell survived, and refuses to run the probe beside
an unwatched primary if it did not.
