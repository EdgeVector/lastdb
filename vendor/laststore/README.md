# Last Store

**Last Store** is a multi-collection **local document store** for embedded and
personal-cloud apps. It is the exportable engine behind [LastDB](https://thelastdb.com)
**Storage v2** (schemas · atoms · tips · CAS blobs), but **any** app can use it
as a plain Rust crate — no LastDB runtime required.

| | |
|--|--|
| **Crate** | `laststore` |
| **Product name** | Last Store |
| **Remote** | `https://github.com/EdgeVector/laststore` |
| **License** | MIT OR Apache-2.0 |

## Why not sled?

Under a true head-to-head harness (same N, value size, flush rules), Last Store:

- **matches** sled write throughput at 64 B payloads  
- is **~3× faster** on writes at 1 KiB  
- is **~1.6× faster** on hot gets  
- is **~8–22× smaller** on disk after **compact** (real reclaim vs freelist hollow)

See the decision brief:  
`docs/DECISION_BRIEF_VS_SLED.md`  
(also mirrored as “Last Store” naming in brain  
`reference-storage-v2-decision-brief-segstore-vs-sled`).

## Install

```toml
[dependencies]
laststore = { git = "https://github.com/EdgeVector/laststore" }
# or path / crates.io when published:
# laststore = "0.1"
```

## Quick start

```rust
use laststore::{collections, LastStore};

fn main() -> laststore::Result<()> {
    let store = LastStore::open("./data")?;
    store.put(collections::ATOMS, "a1", br#"{"hello":"world"}"#)?;
    store.put(collections::TIPS, "t1", b"a1")?;
    store.flush()?; // durability barrier

    let tip = store.get(collections::TIPS, "t1")?.expect("tip");
    let atom = store.get(collections::ATOMS, std::str::from_utf8(&tip).unwrap())?
        .expect("atom");
    println!("{}", String::from_utf8_lossy(&atom));
    Ok(())
}
```

```bash
cargo run --example basic
cargo test
```

## API surface

| Method | Role |
|--------|------|
| `LastStore::open` / `open_with` | Open home directory |
| `put` / `get` / `delete` / `exists` | Point documents in a **collection** |
| `list_prefix` / `list_prefix_keys` | Prefix walk (bodies / ids only) |
| `list_prefix_paged` / `list_prefix_keys_paged` | Keyset pagination (`after` + `limit`) |
| `list_range` / `list_range_keys_paged` | Half-open `[start, end)` walks |
| `transaction` | Multi-doc apply + flush |
| `flush` | Group-commit durability barrier |
| `compact` / `compact_collection` | Rewrite live docs; delete old segments |
| `compact_collection_dead_groups` | Same, but only for hash groups that hold dead bytes; a clean group stays byte-identical |

Id walks are **engine primitives for efficient navigation**. Product meaning
(schema keys, hash ranges, B-trees) still lives **above** Last Store as
document layouts — see Abstraction North Star.

**Collections** are arbitrary UTF-8 names. LastDB Storage v2 uses
`schemas` / `atoms` / `tips` (constants in `laststore::collections`).

## Durability model

Writes append to an in-memory buffer and **group-commit** (spill + `sync_data`)
on dirty thresholds or when you call `flush` / `transaction` / drop.

This matches how LastDB uses sled today: many puts, flush at mutation commit —
**not** fsync-every-put.

## Layout on disk

```text
<home>/
  data/
    <collection>/
      <shard>/
        0000000001.seg
        0000000002.seg
```

Hash-group layout is available for identity-addressed document collections:

```text
<home>/
  data/
    <collection>/
      <shard>/
        g/
          <group>/
            0000000001.seg
```

A **new** home (`LastStore::open`) uses hash-group addressing by default.
Placement is a pure function of the document id: `group = hash(id) % G`.
Defaults are `shard_bits = 0` and `hash_group_bits = 10` (`G = 1024`), so
point `get` opens the chosen group instead of depending on one global
id-location map. Existing journal homes keep their recorded layout.
`LastStoreOptions::segment_log()` opts a new home into the legacy map.

`du` on a collection answers “why big?”. `compact` makes space return to the OS.

## Options

```rust
use laststore::{LastStore, LastStoreOptions};

let opts = LastStoreOptions {
    layout_mode: laststore::LayoutMode::HashGroup, // default for new homes
    shard_bits: 0,              // 0 = single shard (default; best sequential)
    hash_group_bits: 10,        // G=1024 for hash-group mode
    hash_algo: laststore::HashAlgo::Fnv1a64,
    layout_epoch: 0,
    max_segment_bytes: 8 << 20, // 8 MiB segment roll
    max_dirty_ops: 16_384,      // group-commit
    max_dirty_bytes: 16 << 20,
    data_key: None,             // Some([u8; 32]) encrypts group-commit frames
    ..LastStoreOptions::default()
};
let store = LastStore::open_with("./data", opts)?;
```

Use `LastStoreOptions::concurrent(4)` when you have many parallel writers.
Use `LastStoreOptions::hash_group()` (also the default) when point lookups
should route by id to `data/<collection>/<shard>/g/<group>/`.
Use `LastStoreOptions::segment_log()` only to write the legacy journal layout.

## Abstraction North Star

> **Last Store** = multi-collection document store.  
> **LastDB** = conventions (schemas / atoms / tips / blobs, tip hops, indexes as documents) **on top of it**.

Product features like “list keys for a schema” or hash-range navigation are
**upper-layer** layouts of document ids/bodies — not innate engine types.
Gaps such as Mini’s paged/range scans or the sled `main` bag are **Mini →
engine packaging** work, not “LastDB the product is missing a concept.”

See `docs/DECISION_BRIEF_VS_SLED.md` and Nano `docs/storage-v2.md`.

## What this is not (yet)

- Full SQL or multi-master replication  
- Mini-grade paged/range id walks (prefix list exists; more if Mini needs it)  
- Built-in AES (codec lives in LastDB Nano / `LastStoreEngine` today)  
- Drop-in for every sled tree Mini opens today  

Last Store is the **engine core** others can depend on while Storage v2
packages the product surface.

## Development

```bash
cargo test
cargo run --example basic
```

Venue: GitHub (`https://github.com/EdgeVector/laststore`). LastGit is retired.
