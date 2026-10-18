# Current and original Hash / HashRange operations

All six matched cases pass. They complete 522 keyed read checks and 84 measured write batches. No request retry or configuration change occurs.

| Program | Identity |
|---|---|
| Original | Clean commit `aa64f61cd3d2d01a3f8953d4a1b3cf21f9940bab`; executable `3f591bae95065fd31a507dcbab12aa6e40e30b24ff346a55fa5a2b0d126b13cb` |
| Current off/on | Frozen current source; executable `e10f45939488c9f6286fedaca76a32dfb4f0f899f05026b976fa723011ce3ee1` |

The original rebuild reproduces its earlier executable identity. These current results follow the receipt and copy fixes. They do not reuse candidate-5 times.

Two rounds use six fresh synthetic homes. Each home has two independent schemas with 256 records each. Each record has four fields and a 1 KiB body. The HashRange schema has one fixed hash. Each bounded request names that hash, an inclusive start, and an exclusive end. There is no range-only case.

The configurations use write mode, a 10,000-path allowance, queued mutation requests, a 500 ms flush interval, and a 128 MiB deferred-byte allowance. Current-on selects the HashRange schema with a separate 128 MiB key allowance. All Hash-only first and repeated reads report zero preload loads.

Read requests return title and body. All value checks after writes and final reopen return all four fields. Every first-read condition starts after a normal process reopen. The OS file cache remains uncontrolled.

Each cell below shows **first / repeated milliseconds**. First values are medians of two observations, one per home. Repeated values are medians of ten observations.

| Read | Keys | Original | Current off | Current on |
|---|---:|---:|---:|---:|
| Hash exact | 1 | 4.426 / 0.264 | 4.541 / 2.284 | 4.578 / 2.373 |
| Hash exact batch | 16 | 37.526 / 1.507 | 34.750 / 22.764 | 35.910 / 22.272 |
| Hash exact batch | 64 | 119.412 / 6.452 | 115.107 / 88.962 | 116.073 / 87.685 |
| HashRange exact | 1 | 5.556 / 0.300 | 4.934 / 2.314 | 89.234 / 2.049 |
| HashRange exact list | 16 | 17.684 / 0.872 | 12.823 / 2.594 | 95.981 / 2.397 |
| HashRange exact list | 64 | 49.790 / 2.522 | 40.327 / 3.882 | 121.309 / 3.677 |
| HashRange bounded | 16 | 14.579 / 10.434 | 11.680 / 3.694 | 96.257 / 3.466 |
| HashRange bounded | 64 | 45.463 / 30.301 | 32.815 / 6.174 | 115.227 / 6.437 |

Width-one calls use `/api/query` for both geometries. Wider Hash calls use `/api/queries/batch` with independent queries. HashRange exact lists use one `/api/query`. Thus, wider Hash and HashRange times do not isolate native lookup costs between geometries.

Current repeated exact reads cost more than the original. Current repeated bounded HashRange reads cost less. Preload adds a first-read cost. It does not consistently reduce elapsed time against current-off after the values are warm.

After the repeats, each HashRange case reads separate keys 128 through 128 plus width minus one. Those values do not appear in the earlier calls of that process. These are separate first-value observations in the same process.

| Other HashRange keys | Keys | Original ms | Current off ms | Current on ms |
|---|---:|---:|---:|---:|
| Exact | 1 | 5.152 | 4.263 | 3.702 |
| Exact list | 16 | 15.783 | 12.044 | 11.385 |
| Exact list | 64 | 44.963 | 36.352 | 34.889 |
| Bounded | 16 | 13.030 | 11.067 | 11.063 |
| Bounded | 64 | 38.178 | 31.865 | 33.184 |

Current-on keeps two completed loads, two Ready domains, and 256 candidate keys in both rounds. Loading, DiskOnly, and failed counts stay zero. Bounded requests increase memory selections from 12 to 14. The width-64 calls fetch 249,514 decrypted bytes. Thus, complete keys do not imply warm values.

Every measured write batch has 64 records. Each cell below shows **ack / through-flush milliseconds**. Through-flush time includes one ordinary native-status call after the ack. All responses return queued mode; that mode alone does not prove memory-only preparation.

| Write condition | Original | Current off | Current on |
|---|---:|---:|---:|
| Hash new keys after reopen | 175.138 / 425.637 | 178.722 / 430.911 | 164.926 / 401.783 |
| Hash update, no prior value read | 169.947 / 449.243 | 168.719 / 471.818 | 158.743 / 440.805 |
| Hash update after a value read | 168.896 / 465.752 | 172.027 / 469.220 | 157.057 / 462.756 |
| HashRange new keys after reopen | 208.234 / 480.438 | 325.461 / 654.192 | 206.680 / 479.140 |
| HashRange update, no prior value read | 242.702 / 574.269 | 250.630 / 598.925 | 238.741 / 567.993 |
| HashRange update after a value read | 221.784 / 530.684 | 235.667 / 549.927 | 175.362 / 491.934 |

New-key and no-prior-read values each use two observations. Read-primed updates each use ten observations. The labels describe caller operations. Write preparation can fetch state in either condition. These small samples do not establish a general write-latency gain.

All six cases verify 320 final records per schema after normal reopen. The full responses, source identities, helper identities, modes, native state, read costs, errors, and per-round samples remain in each case directory. `comparison.json` supplies the aggregate and observed p95 values. `run-order.json` records the fixed order.

The original native API lacks the newer warm, key, and raw-record counters. Missing fields remain unavailable, not zero. Product read-operation counts are not physical device command counts. Observed p95 values do not establish a latency guarantee.

All 96 owned node sessions stop normally with exit code zero. `cleanup-review.json` preserves the six exact-root previews. `cleanup-execution.json` records six successful supported reclaims and confirms that every exact path is absent.

The root agent separately removes its clean baseline code worktree through normal Git worktree removal. `baseline-cleanup.json` records exit code zero and the exact path absent. `final-source-check.json` confirms that all 1,656 frozen current source inputs and the current executable remain unchanged.
