# Two-device proof on one machine (no cloud)

This page shows how to run two LastDB homes of one owner on one machine, and
how to move their mutation-log segments between them through a directory. You
can then make a real two-device conflict, and read what each node did with it.

The tooling is for developers. It is guarded so it cannot run on a real home.

## What it does and does not prove

It runs the real code for these steps:

- capture of each write into the durable mutation log;
- the order, the seal (encryption) and the frontier of each segment;
- the peer-apply path on the other home, including the `MutationIntent` replay
  and the tip decision.

It replaces only the HTTP transport. A cloud is not involved, so it does not
prove anything about cloud auth, upload, download, listing, backup or restore.

## The two verbs

Both are hidden from `lastdb db --help` and exist only on the owner socket.

```text
lastdb db export-mutation-log --to <dir>      # seal pending records, write segments
lastdb db replay-mutation-log --from <dir>    # apply the segments of other writers
```

`export` runs the same scan, seal and confirm steps as the cloud upload cycle.
After it, the exported records count as published, as they do after a cloud
upload. A second `export` writes only newer records.

`replay` hands the segments to the same function that the live peer-apply cycle
uses. It skips the segments of its own device. It persists the applied frontier,
so a second `replay` of one directory applies nothing.

Both homes may use one directory.

## The guard

The daemon refuses unless every condition holds:

1. The daemon environment has `LASTDB_PROOF_REPLAY=1`.
2. The home holds a regular file named `.synthetic-home`. You create it.
3. The home, its `data` directory and its `identity.key` are not symlinks, and
   no entry directly inside `data` is a symlink.
4. By file identity (device and inode), the home and its `data` directory are
   not a primary home, do not sit inside one, and do not hold one. A primary
   home is `.lastdb` or `.folddb` under `$HOME` and under the home that the
   operating system account database names (`getpwuid`), each as written and
   after symlinks. The check compares identities, not path strings, so an alias
   of the same directory (a symlink, or a macOS firmlink spelling) is refused.
   A changed `$HOME` does not hide the real home. With no known user home the
   guard refuses.
5. The home has no cloud configuration (`cloud_sync.json` or its paused form).
6. The daemon booted with the local proof engine. That engine is built only
   when conditions 1 to 5 hold at boot. It is a sync engine that captures
   writes and replays segments, with an unresolvable `.invalid` address and an
   HTTPS-only client, so no call can leave the machine.

If the variable is set but the guard fails at boot, the daemon boots as a
normal local-only node and logs the refusal. It never fails boot.

Each verb checks conditions 1 to 5 again on every call. Every use logs a WARN
line. The boot logs `PROOF TOOLING ARMED`.

The directory argument of a verb must be an absolute path with no `..`
component, and its last component must not be a symlink. A directory that is,
sits inside, or holds a primary home is refused. An export makes this check
before it creates anything. The export writes only below the directory it is
given and refuses a symlink on any component below it. The replay refuses a
symlinked `log` entry or segment file, a segment file larger than a cloud
download accepts (the engine's `max_download_entry_bytes`, plus 1 MiB), more
than 10,000 segment files, and more than 512 MiB in one call.

### What the guard does not cover

CAUTION: The guard knows only the primary candidates above. Any other home
that holds real data (a home that was moved or saved aside, a copy, a home at a custom path) is
protected by the marker alone. Never create `.synthetic-home` in a home that
holds real data, and never set `LASTDB_PROOF_REPLAY=1` for a daemon that
serves one.

The guard also does not stop an operator who has the owner socket from
arming a home on purpose. The owner socket already gives full control of the
home.

The daemon with the proof engine shuts down in the local-only mode (no final
cloud sync), because it has no cloud.

## Make two homes of one owner

Two devices of one owner share one identity seed (`identity.key`, 32 bytes) and
have two different device ids (`data/.device_id`).

Use a synthetic seed only. Never copy the seed of a real home.

```bash
HA="$(mktemp -d "$TMPDIR/proofA.XXXXXX")"
HB="$(mktemp -d "$TMPDIR/proofB.XXXXXX")"
XD="$(mktemp -d "$TMPDIR/proofX.XXXXXX")"     # the exchange directory
touch "$HA/.synthetic-home" "$HB/.synthetic-home"
```

Keep the home path short: a Unix socket path is limited to about 100 bytes.

Start home A. It creates a fresh identity and its own device id.

```bash
env LASTDB_HOME="$HA" FOLDDB_HOME="$HA" LASTDB_PROOF_REPLAY=1 EXEMEM_ENV=dev \
    LASTDB_SUPPRESS_RECOVERY_PHRASE=1 RUST_MIN_STACK=67108864 \
    lastdbd --data-dir "$HA" &
```

Copy the seed of A to B before B starts for the first time. B then has the
same owner. B creates its own device id on its first start.

```bash
cp "$HA/identity.key" "$HB/identity.key" && chmod 600 "$HB/identity.key"
env LASTDB_HOME="$HB" FOLDDB_HOME="$HB" LASTDB_PROOF_REPLAY=1 EXEMEM_ENV=dev \
    LASTDB_SUPPRESS_RECOVERY_PHRASE=1 RUST_MIN_STACK=67108864 \
    lastdbd --data-dir "$HB" &
```

Check that the two homes are two devices of one owner:

```bash
for h in "$HA" "$HB"; do
  curl -s --unix-socket "$h/data/folddb.sock" http://x/api/schemas | jq -r .user_hash
done                                                      # same owner hash twice
cat "$HA/data/.device_id" "$HB/data/.device_id"           # two different ids
lastdb --data-dir "$HB" status | grep '^Home:'            # the home is your temp dir
```

Notes:

- A debug build overflows the 8 MiB worker stack on a write. Use a release or
  `dev-release` build, or set `RUST_MIN_STACK=67108864`.
- `EXEMEM_ENV=dev` makes a schema declaration use the DEV Schema Service. A node
  without it uses the production service. Do not declare proof schemas there.
- Set `FOLD_SYNC_DEVICE_ID` to choose a device id instead of a random one.

## Make a conflict and read it

Declare the same schema on both homes (`POST /api/schemas/declare` on the owner
socket). Use a `Hash` schema when you want to read the conflict state, because
`POST /api/conflicts/keys` supports only `Hash` schemas.

Write the same key on both homes at nearly the same time, with two values.
Then exchange in both directions:

```bash
lastdb --data-dir "$HA" db export-mutation-log --to "$XD"
lastdb --data-dir "$HB" db export-mutation-log --to "$XD"
lastdb --data-dir "$HB" db replay-mutation-log --from "$XD"
lastdb --data-dir "$HA" db replay-mutation-log --from "$XD"
```

Reads that work on a synthetic home:

| Read | What it shows |
|---|---|
| `POST /api/query` with a key | The tip on each node. |
| `POST /api/conflicts/keys` | `clean` or `conflict` for a `Hash` field molecule. |
| `GET /api/history/<molecule>` | The recorded mutation events of one molecule. |
| `GET /api/atom/<atom_uuid>` | Whether an atom body is still on disk. |
| `lastdb db inventory --json` | Atoms per schema, and the `history:` and `conflict:` key classes. |
| `lastdb status`, line `Atom counters` | Bodies created and deleted, and the counter faults, since the daemon started. The line lags by up to 60 s. |
| `lastdb db inventory --json`, block `atom_counters` | Counters, total references, bodies without a counter, counters without a body, zero counters and `suspect` counters. |
| `POST /api/storage/liveness/explain` with `{"class":"atom","ids":[...]}` | The counter, the `suspect` flag and the body state of each atom. |

An atom body leaves the store in the same batch as the last tip or conflict
record that names it. There is no grace window, no garbage-collection lap and
no peer-apply interlock, so there is nothing to age or to lift. To see a body
go, purge or delete the last key that holds it and read `GET /api/atom/<atom_uuid>`.

CAUTION: The conflict recorder fires only when the atom store knows the device
id of its sync engine. A node without an attached sync engine records no
conflict, and it records no conflict row to hold a loser atom. A review of the
atom counter change could not make the recorder fire on a synthetic home, on
this build or on main. If `GET /api/history/<molecule>` and `POST
/api/conflicts/keys` show no conflict after the exchange, the recorder did not
fire, and the steps above do not prove conflict handling. A recorded conflict
holds counted references to its loser atoms until its key is purged, deleted or
swept, or its retention window ends (`LASTDB_CONFLICT_RETENTION_SECS`, 30 days);
`lastdb status` shows the queue of expiring records, and `lastdb db verify-refs`
checks the references (`docs/lastdb-atom-counter.md`).

## Stop and clean up

Send `SIGTERM` to each daemon you started. Remove only the temporary homes you
made. Keep the receipts you need first.
