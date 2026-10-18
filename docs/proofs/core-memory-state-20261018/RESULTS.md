# Core memory state: fresh before/after results

The final build reuses typed core state for schemas, molecule controls, complete selected keys, and fetched values.
Repeated exact reads improve. Repeated bounded reads improve, with zero reported file reads when the selected keys and values are warm.
The write results vary. They do not establish a general write gain or memory-only write preparation.

Before is the branch immediately before this work, executable `e10f4593…`.
After is final v4, executable `bdb83b38…`. This is a fresh matched comparison, not a comparison with the older `aa64f61` original engine.
The complete identities are in [before-source.json](before-source.json), [current-source-v4.json](current-source-v4.json), and [final-identity-check.json](final-identity-check.json).

## Method

- Host: Apple M4 Max, 14 cores, 36 GiB, macOS 26.5.2. See [host.json](host.json).
- Eight fresh synthetic homes. Each has an independent Hash schema and HashRange schema, with 256 initial records each.
- Each record has four fields and a 1 KiB body. Timed reads request title and body.
- All bounded reads name one hash, a start range key, and an exclusive end range key. No standalone Range query exists in this workload.
- Off means no selected schema preload. On selects only the HashRange schema.
- Both rounds use both executables with preload off and on. The second round reverses all four configurations.
- Each read shape starts after a normal process reopen. The macOS file cache is uncontrolled.
- First and disjoint read medians each use two observations. Repeated read medians use ten observations.
- Hash widths 16 and 64 use a batch of independent queries. HashRange uses one query. Both one-key cases use `/api/query`.
- Exact HashRange timing uses `HashRangeKeys` with explicit key pairs. Separate operational proof covers singular `HashRangeKey`.
- All writes contain 64 records. New-key and no-prior-read write medians use two observations; read-primed update medians use ten.
- Ack time and through-flush time are separate. Through-flush time includes one native-status call between ack and explicit flush.
- No failed timing request is retried. No configuration or source changes occur during the eight cases.

## Reads

All times below are median milliseconds. Lower is better.

First read after process reopen:

| Read | Before off | After off | Before on | After on |
|---|---:|---:|---:|---:|
| Hash exact, 1 key | 4.615 | 4.048 | 3.970 | 4.778 |
| Hash exact, 16 keys | 31.871 | 13.353 | 33.298 | 12.840 |
| Hash exact, 64 keys | 112.225 | 31.982 | 116.472 | 32.623 |
| HashRange exact, 1 key | 4.543 | 4.706 | 88.551 | 89.680 |
| HashRange exact, 16 keys | 12.615 | 13.751 | 91.852 | 89.695 |
| HashRange exact, 64 keys | 36.828 | 38.357 | 116.823 | 113.851 |
| HashRange bounded, 16 keys | 13.775 | 11.540 | 92.263 | 89.707 |
| HashRange bounded, 64 keys | 32.443 | 34.652 | 117.568 | 112.854 |

Repeated read of the same keys and values:

| Read | Before off | After off | Before on | After on |
|---|---:|---:|---:|---:|
| Hash exact, 1 key | 2.297 | 0.279 | 2.080 | 0.304 |
| Hash exact, 16 keys | 22.149 | 1.508 | 21.877 | 1.364 |
| Hash exact, 64 keys | 87.249 | 4.754 | 88.027 | 4.436 |
| HashRange exact, 1 key | 2.138 | 0.321 | 1.987 | 0.340 |
| HashRange exact, 16 keys | 2.519 | 0.810 | 2.386 | 0.753 |
| HashRange exact, 64 keys | 3.649 | 2.108 | 3.693 | 2.082 |
| HashRange bounded, 16 keys | 4.011 | 1.544 | 3.356 | 1.502 |
| HashRange bounded, 64 keys | 5.936 | 4.319 | 6.398 | 4.806 |

Different keys in the same process, after the earlier reads:

| Read | Before off | After off | Before on | After on |
|---|---:|---:|---:|---:|
| HashRange exact, 1 key | 3.918 | 1.972 | 3.626 | 1.735 |
| HashRange exact, 16 keys | 11.431 | 10.624 | 11.339 | 9.348 |
| HashRange exact, 64 keys | 35.158 | 33.997 | 35.141 | 32.715 |
| HashRange bounded, 16 keys | 11.458 | 8.707 | 10.977 | 9.032 |
| HashRange bounded, 64 keys | 30.337 | 29.333 | 33.308 | 31.654 |

The on configuration still pays the complete selected-schema key load on its first HashRange request, including an exact-key request.
A complete key list does not preload all field values. A disjoint request still fetches missing values.
The first preload request does not show a general speed gain. For example, exact one-key on changes from 88.551 ms to 89.680 ms.
Bounded 64-key off first-read time changes from 32.443 ms to 34.652 ms. Each first-read median has only two observations.

## Reported file work for repeated reads

These are instrumented file-read calls, not guaranteed physical disk reads. The operating system can serve a file call from its cache.

| Read | Before off | After off | Before on | After on |
|---|---:|---:|---:|---:|
| Hash exact, 1 key | 14.5 | 0 | 14 | 0 |
| HashRange exact, 1 key | 14.5 | 0 | 14 | 0 |
| HashRange bounded, 16 keys | 40 | 20.5 | 19 | 0 |
| HashRange bounded, 64 keys | 49.5 | 34 | 19 | 0 |

A warm bounded read without preload still reads the key list from disk. The selected on configuration reuses the complete keys in memory.

## Writes

Ack time, median milliseconds per 64 records:

| Write | Before off | After off | Before on | After on |
|---|---:|---:|---:|---:|
| Hash: new keys after reopen | 170.248 | 161.305 | 163.539 | 165.916 |
| Hash: update without prior value read | 171.907 | 162.433 | 162.318 | 172.746 |
| Hash: update after a read | 164.055 | 84.984 | 168.877 | 81.269 |
| HashRange: new keys after reopen | 208.219 | 206.027 | 221.656 | 215.199 |
| HashRange: update without prior value read | 243.117 | 233.851 | 235.843 | 245.136 |
| HashRange: update after a read | 224.400 | 230.421 | 224.802 | 190.129 |

Through explicit flush, median milliseconds per 64 records:

| Write | Before off | After off | Before on | After on |
|---|---:|---:|---:|---:|
| Hash: new keys after reopen | 407.243 | 415.131 | 422.757 | 410.036 |
| Hash: update without prior value read | 441.857 | 448.817 | 445.992 | 450.202 |
| Hash: update after a read | 444.983 | 404.983 | 458.609 | 390.013 |
| HashRange: new keys after reopen | 455.108 | 460.874 | 466.709 | 463.942 |
| HashRange: update without prior value read | 554.995 | 571.581 | 571.953 | 565.887 |
| HashRange: update after a read | 531.146 | 530.969 | 516.207 | 505.594 |

The large read-primed Hash ack reduction is an observed result. Write preparation still fetches clean previous values through its existing storage route.
A faster prior read also changes the request timing relative to the 500 ms background flush. These results do not prove memory-only writes.
The gains through flush are smaller. Hash new-key off time increases by about 1.9%, while HashRange no-prior-read off increases by about 3.0%.
Those two conditions have two observations each. All returned write modes are queued. No maximum write-throughput claim follows from these samples.

## Correctness and memory

All eight timing cases pass: 696 keyed read checks and 112 measured write batches.
Every case verifies all four fields after normal process reopen. Each schema then contains 320 generated records.
The final operational proof passes 118 keyed checks and 25 strict zero-file-read gates.
It covers accepted writes, flush, reopen, absence followed by insert, delete, bounded membership, and singular exact HashRange reads.
It also changes and restores each schema’s molecule map, with different donor keys and values.
Three concurrent 64-record update cohorts return complete old or new revisions. Calls run concurrently without a deterministic pause at captured state.
Schema absence, create, drop, and reinstall also pass. The two expected HTTP 400 responses match the exact schema-absence text.
See [operational result](operational-current-v4-complete/result.json) and [exact response validation](schema-absence-validation.json).

The current build reports 28 core definition entries and a maximum charged size of 101,297 bytes, about 98.9 KiB.
The definition allowance is 16 MiB. It is separate from the complete key allowance of 128 MiB.
Selected key state reaches 180,718 charged bytes for 320 keys, about 176.5 KiB.
The peak key charge reaches 67,289,582 bytes, about 64.17 MiB, including the temporary preload reservation.
The logical warm set reaches 2,560 records within its 10,000-record budget. No core definition refusal or purge occurs in the sampled statuses.
These are reservation counters, not total process memory measurements. This workflow does not sample total process memory.
See [memory-summary.json](memory-summary.json). Missing before counters remain unavailable.

## Preserved gate history and limits

- V1 stops at five file reads on a fully warm Hash query. The core metadata integration needs additional receiver and conflict controls.
- V2 stops at two warm Hash file reads. The final integration also retains molecule key metadata and active generation controls.
- A v2 projection diagnostic initially rejects the API’s extra primary key field. The corrected diagnostic preserves that field and passes all projections.
- V3 then reports zero file reads for fully warm projections. An operational gate next treats the first read after unrelated writes as warm.
- Existing delete controls remove warm molecule authority. That first post-change read can need disk work; its receipt remains preserved.
- The method then records a first post-change read separately and checks its unchanged repeat with the same zero-read requirement.
- This exposes a real complete-key bug: a generation delete marker adds a deleted key back when no generation exists.
- The minimal v4 publication guard fixes that key-list bug. The normal operational proof clears all 23 primary zero-read gates.
- The optional absence check initially expects JSON. The API returns the correct schema-absence message as plain text.
- The proof decoder accepts only the named schema’s absence. The final full run clears all 25 gates; the original decoder failure remains preserved.
- An independent final receipt check verifies both exact absence messages. The future parser also requires the named schema immediately before the absence phrase.
- Existing broad tip checks after a write remain conservative. A first read after a write can need disk work even for an unchanged key.
- Homes with active conflict history still read its current event range. Zero file reads here do not establish zero file work for every query shape.
- No primary or archive home is opened or copied. Every node uses newly generated data. No tests run.

All timing samples, ranges, costs, write modes, and native status observations remain in [comparison.json](comparison.json) and the per-case receipts.
The cases use the same fixed workload. These are small samples on one host, not latency or capacity guarantees.
Build and lint records are in [validation.json](validation.json). Source and executable identities remain unchanged after all cases.

After review, supported exact reclaim removes all 19 owned synthetic roots. All 166 owned node sessions stop normally. All preserved receipts remain.
See [cleanup-final.json](cleanup-final.json) and [receipt-seal.json](receipt-seal.json).
