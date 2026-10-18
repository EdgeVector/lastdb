All eight paired cases pass. Each case writes 512 generated records in batches of 64. The schema has four fields and a 1 KiB body.

Each program uses the same write-receipt prerequisite fix. Exactly eight copy-optimization source files differ between the programs. Both use queued acks, a 500 ms flush interval, and a 128 MiB deferred-byte allowance.

The host is an Apple M4 Max with 36 GiB of memory. Each case uses a fresh synthetic home. The wall time includes the final explicit flush.

| Case | Program | Total wall, two rounds (s) | Records/s | Field payload MB/s | Ack p50 (ms) | Ack p95 (ms) | Copy-path coverage |
|---|---|---:|---:|---:|---:|---:|---|
| Serial | Before | 4.8621 | 210.61 | 0.24295 | 263.98 | 572.41 | No |
| Serial | After | 4.6693 | 219.30 | 0.25298 | 265.53 | 389.53 | No |
| Four clients | Before | 3.6593 | 279.84 | 0.32281 | 659.75 | 1247.84 | Yes |
| Four clients | After | 3.5958 | 284.78 | 0.32851 | 616.61 | 1226.15 | Yes |

Each entry combines two fresh homes, 1,024 records, and 16 request acks. The p95 uses the nearest rank. Field payload MB counts serialized field values and uses 1,000,000 bytes per MB. Body-only rates are 0.21566/0.22457 MB/s for serial and 0.28655/0.29161 MB/s for four clients, before/after.

Both four-client rounds show two accepted requests at once in both programs. Their throughput changes are −2.95% and +6.63%. The combined change is +1.77%. This small mixed result does not establish a reliable speed gain. The serial cases show one accepted request and do not qualify for copy attribution.

Every case returns all 512 keys with the correct four field values after normal reopen. Exact-key batches and fixed-hash ranges both pass. All cases report zero mutation errors and zero refusals. No case retries a write or changes its configuration.

The sparse memory samples during an accepted two-request prefix are 119.797/123.750 MiB before and 119.609/122.266 MiB after, by round. The four-client samples after reopen are 107.156/107.641 MiB before and 105.922/105.375 MiB after. Serial has no sample inside the timed interval. The full phase samples are in `memory-observations.json`. These observations do not bound the peak or establish a process cap.

The public status samples report cached deferred bytes of zero and a 128 MiB allowance. Each receipt includes `sampled_at`. These cached values do not establish the instantaneous raw-byte charge.

All 20 final node sessions stop normally with exit code zero. The 10 final case homes and two program captures are removed through the reviewed exact cleanup path. The execution aggregate is `../cleanup-execution.json`.

The full paired results are in `comparison.json`. The two pilot cases and the earlier uncovered cases remain separate. They do not enter this aggregate.
