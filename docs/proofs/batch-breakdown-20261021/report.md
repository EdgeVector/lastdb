# Write call rates and time per call

The earlier 12.06 records per second is a pooled result across three batch sizes.
It counts logical record changes. It does not count database calls per second.
The earlier benchmark uses the pre-integration executable, four fields, a 1 KiB body, and one caller.
Other database jobs share the Mac. These results do not establish a hardware limit.

Each result below contains 64 calls: two runs, two write types, and 16 calls per cell.
Both versions wait for local disk completion before the reply.

| Key type | Records per call | After: calls per second | After: mean seconds per call | After: records per second |
|---|---:|---:|---:|---:|
| Hash | 1 | 0.993 | 1.007 | 0.993 |
| Hash | 32 | 0.333 | 2.999 | 10.669 |
| Hash | 128 | 0.109 | 9.164 | 13.968 |
| HashRange | 1 | 0.759 | 1.318 | 0.759 |
| HashRange | 32 | 0.327 | 3.058 | 10.466 |
| HashRange | 128 | 0.109 | 9.161 | 13.973 |

The call rate divides the call count by the sum of the timed cell durations.
Each cell starts at its first send and ends at its final reply.
Mean call time divides the sum of the individual HTTP durations by the call count.
Time between calls adds only 20.37 milliseconds across the 384 after-version calls.
That difference is 0.0012% of their timed cell duration. It does not explain the slow rate.

The [full comparison](existing_report.md) includes both versions, medians, and 95th-percentile call times.
The [exact comparison archive](portable-analysis/existing_result-json/manifest.json) retains all units, denominators, input hashes, and individual cell summaries.

The fresh diagnostic uses the final integrated executable and a smaller database.
It completes 36 calls and 1,932 record changes, then checks all values and notices after a normal restart.
Each result combines three create calls and three update calls.
The database ends with 966 records; each original benchmark database ends with 5,152 records.
The sampler refreshes each second. Status calls and read checks interrupt the write sequence.
This workload measures call latency and available phases. It does not supply a new continuous call rate or a before/after gain.

| Key type | Records per call | Current diagnostic: mean milliseconds per call | Node CPU: milliseconds per call | Node write MiB per call |
|---|---:|---:|---:|---:|
| Hash | 1 | 239.5 | 19.4 | 0.425 |
| Hash | 32 | 423.6 | 99.5 | 5.303 |
| Hash | 128 | 814.6 | 377.4 | 18.102 |
| HashRange | 1 | 302.1 | 28.0 | 0.680 |
| HashRange | 32 | 566.7 | 128.1 | 4.521 |
| HashRange | 128 | 565.5 | 385.9 | 12.750 |

The CPU counter covers all node threads and the sampler. Its bracket extends slightly beyond the HTTP interval.
CPU time can overlap the other measurements. It is not a separate part to add to the call time.
The saved Mach timebase converts the raw CPU counter to milliseconds.
Apple's [task accounting](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/task.c#L6391)
and [resource accounting](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/bsd_kern.c#L1196) define these raw counters.

The node write-byte counter rises by 250.69 MiB across the 1,932 changes.
Their supplied body-field data contains 1.89 MiB. Keys and other fields add more data.
The counter also includes metadata, logs, and other node work inside the request brackets.
This is not a pure payload ratio. It points to file and metadata output as another useful cost to investigate.
MiB means 1,048,576 bytes. [write-byte-observations.json](write-byte-observations.json) records the exact byte totals and scopes.

The fresh timings locate most time inside the core storage commit path and its waits.
For example, a 128-record Hash call averages 814.6 milliseconds.
The data preparation and acceptance gate holds for 83.0 milliseconds, before the durable wait completes.
Schema load averages 1.49 milliseconds. Molecule restore averages 21.87 milliseconds. Notice preparation averages 1.02 milliseconds.
Mean request parse and validation times each stay below 0.21 milliseconds in all six cases.
The node also records about 33 file-sync calls and 51 directory-sync calls per such call.
The CPU, gate, phase, and disk-counter scopes overlap. These numbers do not form separate parts of one call.

A 128-record call updates 512 field keys. Its create data batch contains 1,542 stored operations; its update batch contains 1,797.
Those operations include fields, tips, counters, and notices. They share one data commit.
Some calls also save a clock reservation before that data commit.
Normal durable writes have no deliberate one-second delay. The one-second clock constant reserves future clock values.
The current engine also creates a new mutation-log stream after each publication, with another receipt publication.

The remaining broad phase, `apply_us`, includes unnamed CPU work and waits.
It does not measure disk-sync time. The current clocks cannot divide it precisely among clock completion, file output, and disk waits.
The fresh profile therefore supports a narrower investigation of the native commit path.
It does not prove which inner operation causes the earlier nine-second calls.
Further timings should separate the clock wait, commit preparation, file output, sync calls, and work after the commit.
The disk-before-ack rule remains unchanged.

Evidence: [fresh profile](profile_report.md), [exact derived measurements](profile_result.json),
[independent operation review](profile-r2/independent-operation-review.json), and [exact archives](archives.json).
The first diagnostic remains failed because its verification read exceeds its requested page limit.
The corrected diagnostic bounds its read requests and passes. The original source and receipts remain unchanged.
