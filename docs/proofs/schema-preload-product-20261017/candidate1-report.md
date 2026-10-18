# First candidate: diagnostic observations

This is the first candidate smoke run, not the final performance comparison.
The executable SHA256 is `e4e1e9080944de0d08f8032e82d9fd191475ea1f4fe182d449d0179ad6d0a284`.
`candidate1-source.json` records the source hashes before the next code changes.
Both runs use fresh generated data and the ordinary 10,000-path cap.

## Selected-schema 256-record run

`after-on-main-256-candidate1/` preserves the full requests, responses, costs, and driver sources.
All 54 reads return the expected values and membership.
The update, insert, delete, key move, flush, and reopen phases pass.
All five owned nodes stop normally with exit code zero.

The first exact read produces two Ready domains with 256 keys.
The complete keys use 150,064 charged bytes.
Four concurrent first range calls produce two loads in total.
The key budget records a 67,258,928-byte peak charge, which includes a temporary load reservation.
This charge is not the measured process memory size.

| Request | Keys | Median ms | Observed p95 ms | Median disk read operations |
|---|---:|---:|---:|---:|
| First exact after reopen | 16 | 29.643 | — | 114 |
| Repeated exact | 16 | 4.752 | 4.791 | 45 |
| Repeated exact | 64 | 10.972 | 11.432 | 79 |
| First range after reopen | 16 | 25.692 | — | 72 |
| Repeated range | 16 | 3.466 | 3.558 | 32 |
| Repeated range | 64 | 6.513 | 6.702 | 44 |
| Capped preview | 10 | 6.108 | — | 88 |

Repeated exact and range requests use the warm values.
Each 16-key request adds 80 core hits with zero misses.
Each 64-key request adds 320 core hits with zero misses.
Residual disk work still occurs, despite those hits.
The repeated exact requests take more time than the committed reference requests.
These findings need the final comparison; the first candidate does not establish the final result.

The ordinary delete and key move raise the load count from two to three, then four.
The status gives the `membership_control` reason.
The next code change replaces that conservative reload for ordinary deletes.

## Separate 4,096-byte key-budget run

`after-on-small-key-budget-256-candidate1/` preserves this separate full workload.
All 54 reads and the write, flush, and reopen phases pass through disk fallback.
The native status reports two DiskOnly domains, zero Ready domains, and zero complete keys.
The charged size and peak charge are both 2,155 bytes.
The reason reports that the key allowance refused the preload work.
The load count stays at zero; repeated query deltas also report zero new loads.

This run overlaps a source-check window.
Its times do not enter performance comparisons.
All five owned nodes stop normally with exit code zero.

## Cleanup

Both roots pass the exact ownership, process, open-file, and socket checks.
Both supported cleanup previews succeed.
The parent reviews the previews and authorizes the exact reclaim commands.
Both commands return zero, and both temporary roots are absent.
Each run keeps the command and outcome in `cleanup.json`.
