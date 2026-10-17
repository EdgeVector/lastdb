# Fresh physical before/after comparison

Before source: `7110c7f54ea005e33678b5c93e89353d6117b41f`. After source: `a1d03310e433b99c788657ef149193776bf5aff6`.

All timed calls use the frozen binaries. The root owns this quiet window. No owned node or build runs during these timings. Ordinary host activity continues. Operating-system cache state is unknown. Each CLI call opens a fresh process; this does not establish a cold-device read. File counters measure userspace reads, not device traffic or write barriers.

The ordinary matrices use one collection, fanout 1, 4096 keys, and an unset disk cap. The 1 KiB matrix uses 31 alternating rounds per case. The 128-byte and 16 KiB matrices use 11 alternating rounds. Values use SHA-256 digest bytes. The recipe includes empty bodies and tombstones. Every returned body matches the generated input. Full known-key reads after writes use a normal process reopen.

The many-map extension uses 256 collections with 16 keys each, fanout 1, and 1 KiB nominal bodies. It includes 64 keys of 217 bytes. The same recipe retains its empty-body and tombstone exceptions. The 256-write call updates one key per collection. It uses 11 alternating rounds. All 4096 keys match the current generated values after writes.

Times below use the product elapsed clock. The p95 uses nearest rank. Maintenance has one matched call per program and operation; it has no p95. The source hashes isolate the shared locator forest in the physical CLI path. The node queue and author clock do not run on this path.

## physical-1k

| Operation | Shape | Count | Median ms before → after | p95 ms before → after | Opens before → after | Read calls before → after |
|---|---|---:|---:|---:|---:|---:|
| read | contiguous | 1 | 0.442 → 0.441 | 0.544 → 0.533 | 11 → 11 | 12 → 12 |
| read | contiguous | 16 | 0.581 → 0.580 | 0.618 → 0.615 | 11 → 11 | 12 → 12 |
| read | contiguous | 64 | 1.001 → 1.005 | 1.029 → 1.026 | 11 → 11 | 14 → 14 |
| read | contiguous | 256 | 2.649 → 2.647 | 2.711 → 2.677 | 11 → 11 | 21 → 21 |
| read | scattered | 1 | 0.439 → 0.441 | 0.450 → 0.455 | 11 → 11 | 12 → 12 |
| read | scattered | 16 | 0.939 → 0.935 | 0.965 → 0.974 | 11 → 11 | 37 → 37 |
| read | scattered | 64 | 2.528 → 2.511 | 2.566 → 2.590 | 11 → 11 | 119 → 119 |
| read | scattered | 256 | 5.669 → 5.691 | 5.719 → 5.922 | 11 → 11 | 341 → 341 |
| read | duplicates | 1 | 0.440 → 0.440 | 0.473 → 0.454 | 11 → 11 | 12 → 12 |
| read | duplicates | 16 | 0.579 → 0.575 | 0.623 → 0.587 | 11 → 11 | 13 → 13 |
| read | duplicates | 64 | 0.898 → 0.892 | 1.070 → 1.064 | 11 → 11 | 13 → 13 |
| read | duplicates | 256 | 2.077 → 2.079 | 2.145 → 2.204 | 11 → 11 | 13 → 13 |
| range | bounded | 1 | 0.444 → 0.444 | 0.549 → 0.538 | 11 → 11 | 12 → 12 |
| range | bounded | 16 | 0.543 → 0.540 | 0.600 → 0.620 | 11 → 11 | 16 → 16 |
| range | bounded | 64 | 0.829 → 0.830 | 0.851 → 0.866 | 11 → 11 | 30 → 30 |
| range | bounded | 256 | 1.967 → 1.963 | 2.032 → 2.022 | 11 → 11 | 88 → 88 |
| write | distinct | 1 | 156.261 → 152.931 | 170.661 → 163.844 | 159 → 159 | 110 → 110 |
| write | distinct | 16 | 159.781 → 164.824 | 178.615 → 178.182 | 159 → 159 | 114 → 114 |
| write | distinct | 64 | 168.991 → 167.823 | 301.762 → 296.996 | 159 → 159 | 130 → 130 |
| write | distinct | 256 | 166.257 → 165.491 | 205.266 → 183.626 | 159 → 159 | 192 → 192 |
| write | repeated | 1 | 164.224 → 160.846 | 193.326 → 200.408 | 159 → 159 | 110 → 110 |
| write | repeated | 16 | 158.576 → 156.504 | 167.904 → 177.565 | 159 → 159 | 114 → 114 |
| write | repeated | 64 | 157.900 → 153.049 | 177.713 → 175.643 | 159 → 159 | 129 → 129 |
| write | repeated | 256 | 166.475 → 164.537 | 180.501 → 184.185 | 159 → 159 | 187 → 187 |

## physical-128

| Operation | Shape | Count | Median ms before → after | p95 ms before → after | Opens before → after | Read calls before → after |
|---|---|---:|---:|---:|---:|---:|
| read | contiguous | 1 | 0.484 → 0.485 | 0.729 → 0.709 | 11 → 11 | 12 → 12 |
| read | contiguous | 16 | 0.537 → 0.532 | 0.556 → 0.588 | 11 → 11 | 12 → 12 |
| read | contiguous | 64 | 0.835 → 0.834 | 0.861 → 0.849 | 11 → 11 | 13 → 13 |
| read | contiguous | 256 | 1.950 → 1.961 | 1.995 → 1.991 | 11 → 11 | 17 → 17 |
| read | scattered | 1 | 0.441 → 0.443 | 0.454 → 0.477 | 11 → 11 | 12 → 12 |
| read | scattered | 16 | 0.961 → 1.012 | 1.058 → 1.200 | 11 → 11 | 37 → 37 |
| read | scattered | 64 | 2.339 → 2.355 | 2.388 → 2.446 | 11 → 11 | 119 → 119 |
| read | scattered | 256 | 4.591 → 4.575 | 4.649 → 5.242 | 11 → 11 | 127 → 127 |
| read | duplicates | 1 | 0.437 → 0.439 | 0.455 → 0.453 | 11 → 11 | 12 → 12 |
| read | duplicates | 16 | 0.556 → 0.553 | 0.568 → 0.567 | 11 → 11 | 13 → 13 |
| read | duplicates | 64 | 0.779 → 0.778 | 0.820 → 0.800 | 11 → 11 | 13 → 13 |
| read | duplicates | 256 | 1.650 → 1.660 | 1.713 → 1.716 | 11 → 11 | 13 → 13 |
| range | bounded | 1 | 0.458 → 0.468 | 0.635 → 0.594 | 11 → 11 | 12 → 12 |
| range | bounded | 16 | 0.497 → 0.500 | 0.579 → 0.545 | 11 → 11 | 13 → 13 |
| range | bounded | 64 | 0.641 → 0.635 | 0.651 → 0.647 | 11 → 11 | 17 → 17 |
| range | bounded | 256 | 1.196 → 1.195 | 1.337 → 1.205 | 11 → 11 | 32 → 32 |
| write | distinct | 1 | 159.856 → 156.194 | 178.442 → 198.472 | 89 → 89 | 70 → 70 |
| write | distinct | 16 | 159.590 → 157.320 | 170.311 → 192.821 | 89 → 89 | 71 → 71 |
| write | distinct | 64 | 154.963 → 159.715 | 184.146 → 190.156 | 89 → 89 | 76 → 76 |
| write | distinct | 256 | 175.950 → 170.044 | 304.253 → 221.330 | 89 → 89 | 96 → 96 |
| write | repeated | 1 | 161.283 → 154.945 | 182.196 → 167.543 | 89 → 89 | 70 → 70 |
| write | repeated | 16 | 161.187 → 160.710 | 167.982 → 190.045 | 89 → 89 | 71 → 71 |
| write | repeated | 64 | 154.126 → 156.364 | 161.400 → 183.246 | 89 → 89 | 75 → 75 |
| write | repeated | 256 | 167.147 → 163.221 | 225.309 → 308.196 | 89 → 89 | 91 → 91 |

## physical-16k

| Operation | Shape | Count | Median ms before → after | p95 ms before → after | Opens before → after | Read calls before → after |
|---|---|---:|---:|---:|---:|---:|
| read | contiguous | 1 | 0.496 → 0.499 | 0.637 → 0.607 | 11 → 11 | 12 → 12 |
| read | contiguous | 16 | 1.311 → 1.315 | 1.460 → 1.358 | 11 → 11 | 16 → 16 |
| read | contiguous | 64 | 3.883 → 3.863 | 3.950 → 3.979 | 11 → 11 | 30 → 30 |
| read | contiguous | 256 | 14.380 → 14.334 | 14.857 → 14.824 | 11 → 11 | 86 → 86 |
| read | scattered | 1 | 0.497 → 0.487 | 0.561 → 0.511 | 11 → 11 | 12 → 12 |
| read | scattered | 16 | 1.764 → 1.747 | 1.785 → 1.775 | 12 → 12 | 41 → 41 |
| read | scattered | 64 | 5.883 → 5.851 | 5.931 → 5.916 | 15 → 15 | 137 → 137 |
| read | scattered | 256 | 17.964 → 17.960 | 18.148 → 18.358 | 19 → 19 | 372 → 372 |
| read | duplicates | 1 | 0.491 → 0.490 | 0.663 → 0.528 | 11 → 11 | 12 → 12 |
| read | duplicates | 16 | 1.045 → 1.048 | 1.085 → 1.073 | 11 → 11 | 13 → 13 |
| read | duplicates | 64 | 2.681 → 2.687 | 2.736 → 2.848 | 11 → 11 | 13 → 13 |
| read | duplicates | 256 | 9.286 → 9.319 | 9.416 → 10.290 | 11 → 11 | 13 → 13 |
| range | bounded | 1 | 0.505 → 0.510 | 0.625 → 0.583 | 11 → 11 | 16 → 16 |
| range | bounded | 16 | 1.338 → 1.335 | 1.375 → 1.451 | 11 → 11 | 77 → 77 |
| range | bounded | 64 | 4.023 → 4.000 | 4.091 → 4.048 | 11 → 11 | 271 → 271 |
| range | bounded | 256 | 14.902 → 14.919 | 15.185 → 16.763 | 11 → 11 | 1050 → 1050 |
| write | distinct | 1 | 152.789 → 152.376 | 164.502 → 177.428 | 97 → 97 | 82 → 82 |
| write | distinct | 16 | 157.141 → 157.368 | 165.345 → 177.065 | 97 → 97 | 142 → 142 |
| write | distinct | 64 | 178.934 → 176.823 | 195.392 → 197.661 | 97 → 97 | 338 → 338 |
| write | distinct | 256 | 243.279 → 240.193 | 279.308 → 282.719 | 97 → 97 | 1121 → 1121 |
| write | repeated | 1 | 163.624 → 157.109 | 195.629 → 177.639 | 97 → 97 | 82 → 82 |
| write | repeated | 16 | 164.341 → 164.333 | 216.372 → 199.740 | 97 → 97 | 142 → 142 |
| write | repeated | 64 | 182.772 → 173.519 | 199.238 → 195.353 | 97 → 97 | 337 → 337 |
| write | repeated | 256 | 253.539 → 269.446 | 296.549 → 312.694 | 97 → 97 | 1116 → 1116 |

## physical-many-maps

| Operation | Shape | Count | Median ms before → after | p95 ms before → after | Opens before → after | Read calls before → after |
|---|---|---:|---:|---:|---:|---:|
| read | contiguous | 256 | 3.583 → 3.475 | 3.762 → 3.545 | 26 → 11 | 35 → 35 |
| read | scattered | 256 | 13.712 → 11.179 | 15.451 → 11.499 | 220 → 11 | 485 → 485 |
| read | duplicates | 256 | 2.045 → 2.072 | 2.101 → 2.168 | 11 → 11 | 13 → 13 |
| write | distinct | 256 | 1835.126 → 221.883 | 2050.177 → 247.008 | 4233 → 663 | 2802 → 987 |

## Every measured latency increase

This list includes every case with a higher median, p95, or both. Small differences can reflect host variation. No increase is suppressed.

| Profile | Operation | Shape | Count | Median ms before → after | p95 ms before → after |
|---|---|---|---:|---:|---:|
| physical-1k | read | contiguous | 64 | 1.001 → 1.005 | 1.029 → 1.026 |
| physical-1k | read | scattered | 1 | 0.439 → 0.441 | 0.450 → 0.455 |
| physical-1k | read | scattered | 16 | 0.939 → 0.935 | 0.965 → 0.974 |
| physical-1k | read | scattered | 64 | 2.528 → 2.511 | 2.566 → 2.590 |
| physical-1k | read | scattered | 256 | 5.669 → 5.691 | 5.719 → 5.922 |
| physical-1k | read | duplicates | 1 | 0.440 → 0.440 | 0.473 → 0.454 |
| physical-1k | read | duplicates | 256 | 2.077 → 2.079 | 2.145 → 2.204 |
| physical-1k | range | bounded | 16 | 0.543 → 0.540 | 0.600 → 0.620 |
| physical-1k | range | bounded | 64 | 0.829 → 0.830 | 0.851 → 0.866 |
| physical-1k | write | distinct | 16 | 159.781 → 164.824 | 178.615 → 178.182 |
| physical-1k | write | repeated | 1 | 164.224 → 160.846 | 193.326 → 200.408 |
| physical-1k | write | repeated | 16 | 158.576 → 156.504 | 167.904 → 177.565 |
| physical-1k | write | repeated | 256 | 166.475 → 164.537 | 180.501 → 184.185 |
| physical-128 | read | contiguous | 1 | 0.484 → 0.485 | 0.729 → 0.709 |
| physical-128 | read | contiguous | 16 | 0.537 → 0.532 | 0.556 → 0.588 |
| physical-128 | read | contiguous | 256 | 1.950 → 1.961 | 1.995 → 1.991 |
| physical-128 | read | scattered | 1 | 0.441 → 0.443 | 0.454 → 0.477 |
| physical-128 | read | scattered | 16 | 0.961 → 1.012 | 1.058 → 1.200 |
| physical-128 | read | scattered | 64 | 2.339 → 2.355 | 2.388 → 2.446 |
| physical-128 | read | scattered | 256 | 4.591 → 4.575 | 4.649 → 5.242 |
| physical-128 | read | duplicates | 1 | 0.437 → 0.439 | 0.455 → 0.453 |
| physical-128 | read | duplicates | 256 | 1.650 → 1.660 | 1.713 → 1.716 |
| physical-128 | range | bounded | 1 | 0.458 → 0.468 | 0.635 → 0.594 |
| physical-128 | range | bounded | 16 | 0.497 → 0.500 | 0.579 → 0.545 |
| physical-128 | write | distinct | 1 | 159.856 → 156.194 | 178.442 → 198.472 |
| physical-128 | write | distinct | 16 | 159.590 → 157.320 | 170.311 → 192.821 |
| physical-128 | write | distinct | 64 | 154.963 → 159.715 | 184.146 → 190.156 |
| physical-128 | write | repeated | 16 | 161.187 → 160.710 | 167.982 → 190.045 |
| physical-128 | write | repeated | 64 | 154.126 → 156.364 | 161.400 → 183.246 |
| physical-128 | write | repeated | 256 | 167.147 → 163.221 | 225.309 → 308.196 |
| physical-16k | read | contiguous | 1 | 0.496 → 0.499 | 0.637 → 0.607 |
| physical-16k | read | contiguous | 16 | 1.311 → 1.315 | 1.460 → 1.358 |
| physical-16k | read | contiguous | 64 | 3.883 → 3.863 | 3.950 → 3.979 |
| physical-16k | read | scattered | 256 | 17.964 → 17.960 | 18.148 → 18.358 |
| physical-16k | read | duplicates | 16 | 1.045 → 1.048 | 1.085 → 1.073 |
| physical-16k | read | duplicates | 64 | 2.681 → 2.687 | 2.736 → 2.848 |
| physical-16k | read | duplicates | 256 | 9.286 → 9.319 | 9.416 → 10.290 |
| physical-16k | range | bounded | 1 | 0.505 → 0.510 | 0.625 → 0.583 |
| physical-16k | range | bounded | 16 | 1.338 → 1.335 | 1.375 → 1.451 |
| physical-16k | range | bounded | 256 | 14.902 → 14.919 | 15.185 → 16.763 |
| physical-16k | write | distinct | 1 | 152.789 → 152.376 | 164.502 → 177.428 |
| physical-16k | write | distinct | 16 | 157.141 → 157.368 | 165.345 → 177.065 |
| physical-16k | write | distinct | 64 | 178.934 → 176.823 | 195.392 → 197.661 |
| physical-16k | write | distinct | 256 | 243.279 → 240.193 | 279.308 → 282.719 |
| physical-16k | write | repeated | 256 | 253.539 → 269.446 | 296.549 → 312.694 |
| physical-many-maps | read | duplicates | 256 | 2.045 → 2.072 | 2.101 → 2.168 |

## Matched compact and reclaim

Each pair uses the fresh distinct-256 homes from its own matrix. A backup reference retains the pre-compaction generation. The first reclaim keeps its required files. Release permits the second reclaim. Every 4096-record ordered digest remains unchanged through compact and reclaim, and the before/after program digests match for each profile.

| Profile | Compact ms before → after | Held reclaim ms before → after | Released reclaim ms before → after | Final files before → after | Final logical bytes before → after | Final locator files before → after | Final locator bytes before → after |
|---|---:|---:|---:|---:|---:|---:|---:|
| physical-1k | 2405.099 → 2319.177 | 78.137 → 74.891 | 1108.779 → 1120.744 | 76 → 76 | 6191668 → 6191668 | 1 → 1 | 1310720 → 1310720 |
| physical-128 | 1056.819 → 1053.726 | 71.136 → 70.073 | 464.584 → 475.558 | 36 → 36 | 2205993 → 2205993 | 1 → 1 | 1310720 → 1310720 |
| physical-16k | 1519.124 → 1480.231 | 107.186 → 77.381 | 730.559 → 549.600 | 44 → 44 | 70465563 → 70465563 | 1 → 1 | 1310720 → 1310720 |
| physical-many-maps | 4788.887 → 1062.681 | 275.415 → 56.611 | 18071.448 → 474.562 | 291 → 36 | 14259035 → 10064731 | 256 → 1 | 9437184 → 5259264 |

The many-map write uses 262 new locator/directory pages in both programs. Its new part count decreases from 257 to 2 per complete write. The matched writes-home build contains 264 → 9 files and 14148308 → 9954004 logical bytes. That single build call takes 3123.274 → 191.466 ms. These counters establish fewer files; the CLI does not count write barriers. Its initial locator page count decreases because one shared empty page replaces 256 empty pages. The final forest still retains a separate leaf for each collection. No result establishes a 10000-record warm-set limit, power-loss recovery, cap behavior near capacity, or a universal small-write tail guarantee.

The single 1 KiB release/reclaim call increases from 1108.779 to 1120.744 ms. The 128-byte call increases from 464.584 to 475.558 ms. These are the only measured maintenance time increases. `physical-maintenance-regressions.json` preserves their unrounded values.

## Receipts

- `physical-1k/inputs.json`: binary hashes, driver hash, generated-input digest, time, and host load.
- `physical-1k/summary.json` and `receipts.jsonl`: complete matrix and every real product call.
- `physical-1k/maintenance-baseline/` and `maintenance-candidate/`: exact compact/reclaim commands, file inventory, and preserved record digests.
- `physical-128/inputs.json`: binary hashes, driver hash, generated-input digest, time, and host load.
- `physical-128/summary.json` and `receipts.jsonl`: complete matrix and every real product call.
- `physical-128/maintenance-baseline/` and `maintenance-candidate/`: exact compact/reclaim commands, file inventory, and preserved record digests.
- `physical-16k/inputs.json`: binary hashes, driver hash, generated-input digest, time, and host load.
- `physical-16k/summary.json` and `receipts.jsonl`: complete matrix and every real product call.
- `physical-16k/maintenance-baseline/` and `maintenance-candidate/`: exact compact/reclaim commands, file inventory, and preserved record digests.
- `physical-many-maps/inputs.json`: binary hashes, driver hash, generated-input digest, time, and host load.
- `physical-many-maps/summary.json` and `receipts.jsonl`: complete matrix and every real product call.
- `physical-many-maps/maintenance-baseline/` and `maintenance-candidate/`: exact compact/reclaim commands, file inventory, and preserved record digests.

`physical-cost-increases.json` lists every higher file/read/buffer/process-memory counter. Whole-process maximum memory is not the logical warm-set count. `physical-regressions.json` lists every latency increase with unrounded values.
