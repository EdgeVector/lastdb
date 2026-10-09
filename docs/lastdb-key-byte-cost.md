# Where the key bytes go, and what each key fix returns

| Field | Value |
|-------|-------|
| **Status** | Measured — primary home, 2026-08-23 |
| **Tool** | `scripts/agent/lastdb-key-cost-scan.py` (read-only, no daemon) |
| **Brain** | `lastdb-every-stored-byte-attributed-the-key-is-the-largest-structural-term-2026-08-23` |
| **Method SOP** | `sop-lastdb-attribute-every-stored-byte-by-parsing-the-segments` |

Run it against any home:

```bash
scripts/agent/lastdb-key-cost-scan.py --home ~/.lastdb --stride 8
```

It parses the segment files directly, so it needs no daemon verb, no decryption
key and about a minute. It requires `packaging=Plain` (`lastdb status`, line
`Layout:`). Validate a run against `lastdb db compact --all --json` (dry run)
before publishing any figure from it.

## The measurement

Primary home, 7,692 MiB on disk (7,031 MiB `.seg` + 661 MiB `.idx`),
11,709,320 live rows.

| term | MiB | share of the store |
|---|---:|---:|
| live body bytes | 4,371.7 | 56.8% |
| live key bytes | 2,010.4 | 26.1% |
| dead superseded versions | 570.9 | 7.4% |
| `.idx` sidecars | 661.0 | 8.6% |
| live record framing (7 B/row) | 78.2 | 1.0% |

**89.7% of live key bytes sit inside long hex runs.** A 256-bit hash written as
64 ASCII hex characters costs 8x the raw 32 bytes and 1.5x base64url.

## Two corrections to earlier readings

1. **The `.idx` sidecar does not store keys.** Sampling `tips`, `atoms` and
   `field_update_order_log`, one live key in 300 appears verbatim in the group's
   `.idx` bytes, and `.idx` per row ranges from 21% to 63% of key bytes across
   planes. So a key-encoding fix cannot claim any part of the 661 MiB. Every
   ceiling below is segment bytes only.

2. **Repetition is the smaller half, not the larger.** Measured on an unbiased
   run stratum (mean run 74.8 B against 78.2 B store-wide): **86.7% of distinct
   runs appear exactly once, and 97.4% appear in exactly one key class.** The
   caller record key is genuinely repeated across `mk:`, `atom:mk:`, `mord:`,
   `moc:` and `mhr:`, but that mechanism accounts for 2.6% of distinct runs.
   What repetition exists is dominated by a few very hot schema hashes.

## What each fix returns

| fix | returns | what it costs to build |
|---|---:|---|
| **A — hex runs to base64url** | **591 MiB** | key construction only; no new structure, no indirection |
| **A+B — also intern runs behind a 5 B reference** | **989 MiB** | a store-wide intern table, an indirection on every key, and table GC |

Interning adds about 398 MiB over re-encoding alone. It buys 1.67x the bytes for
a global mutable structure on the hot path.

## Row count

`mord:` and `moc:` are 4,624,650 live rows — **39.5% of every row in the
store** — carrying 1,102 MiB, which is 14.3% of the store.

**95% of those bytes live in the `tips` plane**, not in `field_update_order_log`
or `field_update_order_count`. Compacting the order-log planes reaches 67 MiB of
the 912 MiB of `mord:` rows. Any reclaim of the update order log has to act on
`tips`.

## Against the < 1 GiB target

| lever | MiB |
|---|---:|
| dead versions (`lastdb db compact`) | 570.9 |
| atom body base64 removal (#1711 / #1713 / #1714 / #1716) | 580.6 |
| key fix A+B | 989.0 |
| drop the update order log | 1,102.2 |
| **all four** | **3,242.7** |

That takes the store to about 4,449 MiB. The target is 1,024 MiB.

**No combination of the measured structural levers reaches it.** What remains is
dominated by atom bodies, where the envelope is roughly 97% of the content on a
median row. Closing the gap is a decision about what the store retains, not
about how it encodes what it holds.
