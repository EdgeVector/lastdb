The current release uses a small fresh database. The run completes 36 calls and 1,932 changes.
Each result combines three create calls and three update calls. Each record has four fields and a 1 KiB body.
The host also runs other work. These results do not show a before/after gain or a machine limit.

| Key | Records/call | Mean ms | p50 ms | p95 ms | Node CPU ms | Data gate ms |
|---|---:|---:|---:|---:|---:|---:|
| hash | 1 | 239.5 | 205.5 | 344.9 | 19.4 | 4.2 |
| hash | 32 | 423.6 | 374.0 | 573.4 | 99.5 | 20.7 |
| hash | 128 | 814.6 | 809.4 | 958.3 | 377.4 | 83.0 |
| hashrange | 1 | 302.1 | 260.3 | 440.8 | 28.0 | 6.9 |
| hashrange | 32 | 566.7 | 607.8 | 629.7 | 128.1 | 42.8 |
| hashrange | 128 | 565.5 | 552.0 | 728.2 | 385.9 | 149.2 |

The p95 is the largest observed call in each six-call result. The sample is small.
CPU time includes all node threads and the sampler. The CPU interval extends slightly beyond the HTTP interval.
The saved Mach timebase is 125/3 nanoseconds per raw tick.

| Key | Records/call | Apply residual ms | Restore molecules ms | Schema load ms | Notice prepare ms |
|---|---:|---:|---:|---:|---:|
| hash | 1 | 236.79 | 0.54 | 1.18 | 0.16 |
| hash | 32 | 410.70 | 5.02 | 1.45 | 0.41 |
| hash | 128 | 766.48 | 21.87 | 1.49 | 1.02 |
| hashrange | 1 | 297.52 | 1.40 | 2.00 | 0.25 |
| hashrange | 32 | 546.92 | 8.54 | 2.18 | 0.57 |
| hashrange | 128 | 500.67 | 29.26 | 2.45 | 1.25 |

Apply is the remaining pipeline time after the named measurements. It includes unmeasured work and waits.
Apply does not measure disk sync time. An absent phase stays unreported in the JSON result.
The data gate covers private preparation and acceptance. The durable wait follows the gate release.

| Key | Records/call | File barriers/call | Directory barriers/call | File barrier ms/call | Directory barrier ms/call |
|---|---:|---:|---:|---:|---:|
| hash | 1 | 15.8 | 31.5 | 69.5 | 149.0 |
| hash | 32 | 19.5 | 34.8 | 106.6 | 216.4 |
| hash | 128 | 32.7 | 51.2 | 168.8 | 268.5 |
| hashrange | 1 | 22.0 | 37.3 | 98.8 | 174.3 |
| hashrange | 32 | 35.0 | 53.7 | 171.2 | 269.9 |
| hashrange | 128 | 43.7 | 60.7 | 90.6 | 91.3 |

These disk counters cover the node process between two status calls. Their intervals exceed the HTTP interval.
Barrier attempts can overlap. Their summed times do not divide the request time into separate parts.
Each request matches one final data gate through the native sequence range and its field count.
A 128-record create carries 1,542 stored operations. A 128-record update carries 1,797 stored operations.
The records, fields, counters, and change notices share that data commit. Some calls also carry a separate clock commit.

The driver checks all values and notices after a normal reopen. Both node sessions stop without a forced kill.
This analysis checks the complete receipt matrix, exact source and binary hashes, helper hashes, and phase coverage.
The compact receipts retain the driver checks and response hashes. This analysis does not repeat the API calls.
Use the original ABBA results for calls per second. This diagnostic includes status and validation pauses.

The next profile should split the Apply residual into the clock wait, data commit, and post-commit work.
Keep the disk-before-ack rule. Do not choose a code change from the Apply residual alone.

Binary SHA-256: `26454b6eb19a555031903be8f85e8ab1ce121b331a29420728f3398fe587da55`.
Source receipt SHA-256: `88b1e3c87396a862f1b79588bfc46df325167ac2a6a074b1fb6a20b013025fff`.
Receipt SHA-256: `67dbc673a111a1d484e0c76c054a668c5b647a42e0d92609105668b7dab575b5`.
Full inputs, clock units, per-call results, and evidence hashes: [profile_result.json](profile_result.json).
