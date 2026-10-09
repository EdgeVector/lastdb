# Lazy restore UUID inversion

The plain segment installer derives a sequence number from a chunk UUID.
The old resolver computes all 50,001 candidate UUIDs on the first access to each handle.
A sequence-zero request therefore pays for every allowed sequence.

The resolver now checks its existing map, then computes candidates only until a match appears.
It starts with space for eight entries and retains only the current handle.
The maximum sequence remains 50,000.
UUID derivation, segment paths, restore order, and manifest validation do not change.

The deterministic tests check sequence zero, later sequences, cached revisits, handle replacement, the maximum, and an unknown UUID.
The sequence-zero assertion fails against the old resolver: 50,001 entries appear instead of one.
The fixed resolver passes all three focused tests in debug and release builds.

A four-group fixture compares the old full-map loop with the lazy resolver for sequence zero:

| Build | Eager resolver | Lazy resolver |
| --- | ---: | ---: |
| Debug | 861,420 microseconds | 60 microseconds |
| Release | 26,245 microseconds | 5 microseconds |

These measurements are informational and have no timing threshold.
They measure the resolver only; they do not predict complete cloud restore time.
An unknown UUID still requires the full bounded search.

Run the tests with:

```sh
cargo test -p laststore --lib plain_restore_uuid_cache -- --nocapture
cargo test -p laststore --release --lib plain_restore_uuid_cache -- --nocapture
```
