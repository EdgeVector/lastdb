# Run proofs for incremental compaction, steps S0, S1 and S2

This directory holds the scripts and the numbers for three steps of the plan to bound the parts and the
write gate (the plan itself is not in this repository; its steps are named here as they are used):

- S0: the measurement scripts, kept as run evidence.
- S1, cheap pack: the full pack keeps the facts it has proved (block-map pages, part headers, a few decoded
  blocks, open part files) for the whole pack and finishes the record digest once. No byte on disk changes.
- S2, one locator object: a new group takes a fresh part in an existing locator object. A batch makes 4 parts
  on a home that was never packed.

There are no tests here. Every proof is a run with the built binaries on synthetic homes.

## Rules for every script

- A home is a fresh `mktemp`-style directory under `$TMPDIR`, named `s1s2.*`. `ldc.refuse_primary` refuses
  any path inside a default home of the product (a dot directory directly under the user home that starts
  with `.lastdb`, `.folddb` or `.primary`). The scripts never copy, open or list such a home.
- Every node runs with `EXEMEM_ENV=dev`, and `Node.start` checks that `lastdb status` names the temporary
  home before any write.
- A node that a node proof starts is a child of the script. A kill is a SIGKILL of that child.
- `ldc.remove_owned` deletes only an `s1s2.*` entry directly under `$TMPDIR`.
- Clones are `cp -c` (copy-on-write) clones of my own synthetic homes. A snapshot is never changed.
  A node needs a short path (a Unix socket path is limited to about 100 bytes), so node clones use
  `ldc.clone_path`.

## Binaries

`LDBIN` names a directory with `lastdb`, `lastdbd`, `lastdb-pack-maintain`, `lastdb-disk-home`,
`lastdb-journal` and `lastdb-disk-recovery`. Two directories are used: `main` (a build of the commit that this
change branches from) and `new` (a build of this change). Both builds use the `dev-release` profile:

```
cargo build --profile dev-release --locked -p lastdb_node --bin lastdb --bin lastdbd \
  -p laststore --bin lastdb-pack-maintain --bin lastdb-disk-home --bin lastdb-journal --bin lastdb-disk-recovery
```

The oracle always reads with the `main` binaries, so the reader does not share the code under test.

## Scripts

| Script | What it does |
|---|---|
| `ldc.py` | Library: start and kill a node, write and read through its HTTP routes, decode the receipt of a stopped home. |
| `ingest.py` | Build a never-packed home with real writes (500 operations a request, 8 clients, 256 groups). Snapshots at stages. |
| `oracle.py` | Record oracle of a stopped home: count, count for each collection, order-free hashes of (collection, key, body SHA-256, batch, operation). |
| `oracle_pack.py` | Oracle before and after a pack, for any number of pack binaries and readers. `--twice` packs the packed home again. |
| `crafted_home.py`, `crafted_verify.py` | A crafted home (overwrites, tombstones, edge sizes, 1 MiB bodies across a part boundary, long keys) and a readback of every key. |
| `readback.py` | Pack a clone, start a node on it, read every acknowledged key. |
| `pack_speed.py` | Time `compact --grace-ms 0` of a binary on a clone: wall, CPU, peak memory. |
| `profile_pack.py` | `/usr/bin/sample` profile of the pack. |
| `parts_per_request.py` | Parts a request adds on a never-packed home (S2). |
| `kill_chain.py` | SIGKILL chain with the oracle on each killed home and a readback of every key. |
| `backup_restore.py` | Backup and restore of a home with one locator object. |
| `listing_cost.py` | Cost of many part files in one directory. |
| `node_pack.py` | Gate hold of the automatic pack in a node with writers. |

## Commands

`$MAIN` and `$NEW` are the two binary directories. `$HOME_DIR` names a synthetic home that a script made.
Run the scripts from this directory. They need only Python 3.9 and the standard library.

```
# S0: the homes. Never packed, auto maintenance off, 500 operations a request, 8 clients, 256 groups.
LDBIN=$MAIN python3 ingest.py --stages 14000,140000,200000 --tag big
#   14,000 operations = 98,553 records; 140,000 = 980,553 records; 200,000 = 1,400,553 records.
#   The script prints the path of each stage snapshot.

# S1, proof 1: the record oracle at about 100 thousand and about 1 million records.
python3 oracle_pack.py --home $SNAP14K  --oracle main=$MAIN --pack main=$MAIN,new=$NEW --reader new=$NEW --twice
python3 oracle_pack.py --home $SNAP140K --oracle main=$MAIN --pack main=$MAIN,new=$NEW --reader new=$NEW
#   The oracle reads the home before, and each packed clone after, with the binaries of main and of this change.

# S1, proof 1: the crafted homes (one made by the binaries of main, one by the binaries of this change).
LDBIN=$MAIN python3 crafted_home.py ; LDBIN=$NEW python3 crafted_home.py      # each prints a home path
python3 oracle_pack.py --home $CRAFTED --expected $CRAFTED.expected.json --oracle main=$MAIN \
  --pack main=$MAIN,new=$NEW --reader new=$NEW --twice

# S1, proof 1: every acknowledged key reads back from a node on the packed clone.
LDBIN=$NEW python3 readback.py --home $SNAP14K  --ops 14000  --pack-bin $NEW
LDBIN=$NEW python3 readback.py --home $SNAP140K --ops 140000 --pack-bin $NEW

# S1, proof 2 and 4: speed, CPU and peak memory, on a clone of the 1.4 million record home.
python3 pack_speed.py --home $SNAP200K --bins main=$MAIN,new=$NEW --runs 2
python3 profile_pack.py --home $SNAP200K --bin $NEW  --out profile-new.txt  --after 15 --seconds 20
python3 profile_pack.py --home $SNAP200K --bin $MAIN --out profile-main.txt --after 15 --seconds 20

# S1, proof 5: the gate hold of the automatic pack in a node, with 4 writers.
LDBIN=$MAIN python3 node_pack.py --home $SNAP200K
LDBIN=$NEW  python3 node_pack.py --home $SNAP200K

# S2, proof 3: parts for each request, before and after.
LDBIN=$MAIN python3 parts_per_request.py --label main
LDBIN=$NEW  python3 parts_per_request.py --label new
# S2: the same home shape at scale (the part count of the 1.4 million record home).
LDBIN=$NEW python3 ingest.py --stages 14000,200000 --tag s2
# S2: 20-round SIGKILL chains, without maintenance and with maintenance on all the time.
LDBIN=$NEW python3 kill_chain.py --rounds 20 --maintenance off      --oracle-bin $MAIN
LDBIN=$NEW python3 kill_chain.py --rounds 20 --maintenance frequent --oracle-bin $MAIN
# S2: backup and restore of a home with one locator object.
LDBIN=$NEW python3 backup_restore.py --oracle-bin $MAIN
# S2: cost of many part files in one directory.
python3 listing_cost.py
```

## Results

Host: one shared Mac (Apple silicon, APFS system disk, other work running), build profile `dev-release`.
`main` is the commit that this change branches from. All times are seconds. The repository ignores generated `*.jsonl` benchmark lines, so the raw lines of the runs stay
in the local worktree; the tables below are the compact report, and `results/` holds the small JSON reports and the two
profile tables.
A "cold clone" is a fresh `cp -c` clone: the file cache holds none of its files. That is the case that favours
`main` the least, because the I/O wait is the same for both builds.

### S1, proof 1: the record oracle (the multiset of records does not change)

The oracle reads (collection, key, body SHA-256, batch, operation) of every record, and also the hash group and
the body length, with the binaries of `main` (and again with the binaries of this change). The hashes are order free.

| Home | Records | Pack by | Records after | Digest equal | Reader |
|---|---|---|---|---|---|
| 14,000 operations | 98,553 | main, new (and a second pack of each) | 98,553 | yes (8 of 8 reads) | main, new |
| 140,000 operations | 980,553 | main, new | 980,553 | yes (4 of 4 reads) | main, new |
| crafted, written by main | 4,077 | main, new (and a second pack of each) | 4,077 | yes (8 of 8 reads), all 4,027 keys read back after each pack | main, new |
| crafted, written by this change | 4,077 | main, new (and a second pack of each) | 4,077 | yes (8 of 8 reads), all 4,027 keys read back after each pack | main, new |

The crafted home holds overwrites, 466 tombstones, a key written again after its
tombstone, empty bodies, records of 4,095 to 12,289 bytes around the block size, 24 bodies of 1 MiB that cross the
8 MiB part boundary, keys of 1,000 to 60,000 bytes (overflow pages), and a long shared key prefix.
The oracle sees a tombstone as a record without a body, so `crafted_verify.py` also reads every key and compares
the body hash, which tells a tombstone from an empty body.

Every acknowledged key reads back from a node on the packed clone (`readback.py`, the node is the build of this change):

| Home | Keys read | Missing or wrong | Pack time |
|---|---|---|---|
| 14,000 operations | 14,000 | 0 | 5.6 |
| 140,000 operations | 140,000 | 0 | 102 (other work ran at the same time) |

Limits of the oracle: `lastdb-pack-maintain records` refuses more than 1,000,000 records, so the 1.4 million record
home has no oracle run. The 980,553 record home is the largest one. Conflict records (the result of a sync merge of
two devices) cannot be made on a single node, so the crafted home has no such record. The pack treats every
collection alike, and the 466 tombstones and the molecule collections of the node homes are the nearest shape.

### S1, proof 2 and 4: speed, CPU and memory of `compact --grace-ms 0` (cold clones, wall time includes reclaim)

| Home | Records | Parts | main wall (pack published) | new wall (pack published) | Wall ratio | CPU main → new | Peak RSS main → new |
|---|---|---|---|---|---|---|---|
| 14,000 operations | 98,553 | 867 | 10.0 (9.8), 3 runs | 2.6 (2.4), 3 runs | 3.8x | 8.6 → 1.4 | 4.6 → 7.2 MB |
| same, warm file cache | 98,553 | 867 | 9.2 (8.9), 3 runs | 2.4 (2.1), 3 runs | 3.9x | 8.1 → 1.4 | 4.6 → 7.3 MB |
| 140,000 operations | 980,553 | 9,533 | 108.1 (103.4) | 40.0 (36.5) | 2.7x | 89.9 → 23.7 | 12.0 → 22.6 MB |
| 200,000 operations | 1,400,553 | 13,739 | 171.2 (161.5), runs 177.4 and 164.9 | 59.2 (53.5), runs 59.3 and 59.2 | 2.9x | 133.6 → 35.7 | 14.0 → 30.9 MB |
| 200,000 operations, packed once | 1,400,553 | 113 | 106.5, runs 106.2 and 106.7 | 9.8, runs 10.3 and 9.2 | 10.9x | 104.3 → 8.0 | 3.9 → 9.4 MB |

The same 200,000 operations written by this change (7,360 parts, 1 locator object):
`main` packs it in 154.1 s (148.4 until CURRENT names the packed root), this change in 56.4 s (52.7): 2.7 times.

The last row is a second pack: the records are in packs, in key order, so a decoded block serves about 16 records.
CPU is user plus system. The target of the plan was at least 2.5 times at 1.4 million records: it is met
(2.9 times for the wall time, 3.0 times until CURRENT names the packed root, 3.7 times for the CPU).

Where the time goes now (`results/profile-*-1400k-records.txt`, `/usr/bin/sample`, 20 s in the middle of the pack,
samples of one thread, the top of stack):

| Frame | main | new |
|---|---|---|
| `read` (the system call that reads one 4 KiB frame for each record) | 8,868 | 7,418 |
| software SHA-256 (`sha2::sha256::soft::compress`) | 3,410 | 0 |
| hardware SHA-256 (`sha2::sha256::aarch64::compress`) | 0 | 620 |
| `open` (a part file for each record) | 1,650 | 3,051 |

The map-page checksum, the part-header read and the stat are gone from the top of the list. What is left is the read
of one frame for each record (the records of a never-packed home come in key order from a different batch each time)
and a file open, which the 64 cached descriptors rarely save (a home has about 1,200 batches). User plus system CPU is
36 s of the 59 s wall time. The rest is the write barriers of the output (`F_FULLFSYNC`) and the reclaim of 16,148 files.

### S1, proof 5: the gate hold of the automatic pack in a node (4 writers, 100 operations a request, 1.4 million records)

| Build | Pack time that the node logs (the gate is held) | Whole run (pack and first reclaim slice) | Longest time between two completed writes | Requests that failed |
|---|---|---|---|---|
| main | 148.6 | 159.8 | 150.4 | 4 (the 90 s handler deadline) |
| this change | 48.5 | 55.4 | 48.0 | 0 |

(`results/node-pack-gate-hold-*.json`.) The gate is still held for the whole pack. S3 of the plan removes that hold.
This change only makes it about three times shorter.

### S2: one locator object

Parts for each request on a never-packed home (the number of batches for each
request depends on how many requests meet in one publication, so it varies with the load):

| Scenario | Parts a request, main | Parts a request, new | Parts a batch, main | Parts a batch, new | Locator objects at the end, main / new |
|---|---|---|---|---|---|
| durable, 1 operation | 12.0 | 9.9 | 5.33 | 4.00 | 90 / 1 |
| default ack, 1 operation | 7.59 | 5.04 | 6.72 | 4.00 | 110 / 1 |
| default ack, 10 operations, 10 groups | 12.13 | 5.40 | 10.40 | 4.00 | 70 / 1 |
| default ack, 100 operations, 1 group | 31.53 | 10.13 | 14.78 | 4.00 | 61 / 1 |
| default ack, 100 operations, 64 groups | 31.53 | 11.13 | 14.55 | 4.07 | 60 / 1 |
| default ack, 10 operations, 8 clients | 26.46 | 9.69 | 11.88 | 4.00 | 410 / 1 |
| durable, 10 operations, 8 clients | 24.44 | 8.93 | 8.83 | 4.00 | 304 / 1 |

A batch adds exactly 4 parts (journal data, journal map, directory, locator). A locator part holds 512 pages. A batch that
edits very many trees writes more than 512 pages and adds a second part. The ingest of the large home is such a case
(500 operations a request over 256 groups, about 7 pages for each tree):

| Home after | main | new |
|---|---|---|
| 14,000 operations | 867 parts (locator 578 in 30 objects) | 445 parts (locator 171 in 1 object) |
| 200,000 operations | 13,739 parts (locator 10,132 in 30 objects) | 7,360 parts (locator 3,903 in 1 object) |

The plan expected about 4,800 parts for the large home. The measured count is 7,360, because those batches need
3.4 locator parts each, not one.

20-round SIGKILL chains (`kill_chain.py`: three clients write durable requests, the node is killed with SIGKILL in the
middle of the writes, the record oracle reads the killed home, the node restarts, and every key is read back):

| Chain | Rounds | Keys | Mismatches | Request errors | After the chain |
|---|---|---|---|---|---|
| automatic maintenance off | 20 | 2,100 | 0 | 0 | 6,341 parts: directory 1,586, locator 1,585, map 1,585, journal 1,585 (exactly 4 for each batch), 1 locator object. A pack of a clone keeps the record set (oracle). Reclaim ends with 0 pending files. |
| maintenance on all the time (trigger 250 parts) | 20 | 1,090 | 0 | 0 | 77 parts, 1 locator object. A pack of a clone keeps the record set. Reclaim ends with 0 pending files. Packs ran in the chain: for example round 2 restarted a home of 248 parts and the first run of the worker made 5. |

Backup and restore of a never-packed home with one locator object (`backup_restore.py`: 29,871 records, 211 parts):
the restored root has the same 29,871 records (same count for each collection, same digests) and the same list of
files and sizes as the live home.

Many part files in one directory (`listing_cost.py`, empty files named like parts, APFS):

| Files in the directory | List, one directory | List, the same files in 32 directories | Create one more file |
|---|---|---|---|
| 10,000 | 4.3 ms | 4.6 ms | 0.05 ms |
| 65,536 (the limit of a receipt) | 28.8 ms | 29.4 ms | 0.05 ms |
| 100,000 | 50.5 ms | 47.9 ms | 0.06 ms |

### Bounds

- Memory of the pack cache: at most 4,096 block-map pages of 16 KiB (64 MiB, reached only by a home with that many
  batches), 256 decoded blocks of 4 KiB (1 MiB), 32,768 verified part records (about 6 MiB) and 64 open part files.
  The pages of trees are dropped every 256 records. The peak resident memory that was measured is above: 31 MB at
  1.4 million records, 7 MB at 98,000 records. `main` used 14 MB and 5 MB.
- Open files: the pack keeps up to 64 part files open between records. Before, a read call kept at most 32 and
  the pack made a new call for each record, so it kept about one.
- The sweep and the reclaim list the object directories. One locator directory can hold as many files as a receipt
  names locator parts (65,536 at most). That listing takes 29 ms.

### Notes and deviations

- The record oracle cannot read more than 1,000,000 records, so the largest oracle run is 980,553 records.
  The 1.4 million record home has the count of copied records (1,400,553) from the tool, the part counts, and a
  readback of the 140,000 keys of the 980,553 record home instead.
- `lastdb-disk-home build` no longer makes a home that this build opens (it makes a home without the atom-record
  feature), so the crafted home starts as a tiny home that a node makes, and `lastdb-disk-home write` appends the
  crafted batches to the stopped home. The node never opens the crafted home again.
- The SHA-256 change is a Cargo feature on `sha2` for `aarch64` only. It adds one crate (`sha2-asm`, built with `cc`)
  to `Cargo.lock`. The x86-64 build of `sha2` uses the SHA-NI instructions by itself when the processor has them, so
  the Linux x86-64 build is unchanged. The `sha2` feature is not set for other targets.
- The speed ratio for a cold clone is lower than the CPU ratio, because the writes of the output are the same for
  both builds (about 22 s of the 59 s at 1.4 million records are not CPU).
- A pre-read of the data parts of each journal batch (to warm the file cache) was tried and gave no gain
  (55.0 and 59.3 s against 59.3 and 59.2 s), so it is not in this change.
