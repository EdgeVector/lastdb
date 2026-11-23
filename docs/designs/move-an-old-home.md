# Runbook: move a home written by the old build

Date: 2026-11-22. Author: Claude, for Tom. For the owner's home, which runs build `d06054159` and must move once to a build that holds `docs/designs/signed-log-on-local-machinery.md` section 11. The design and the reasons are there. This page is the steps.

What moves: the identity, every record, every file and every installed app stay where they are. The home gets the id of the personal database that the identity key derives, a new device id, and a first snapshot on the relay. What stays behind, in a directory beside the home, is the old upload state: the old spool and the old `log-sync/<UUID>/` directory. The old cloud log is not read again.

## Before

- You have a build with step 5 (`lastdb --version`) and a relay: `LASTDB_RELAY_URL` for the daemon, or a `cloud-credentials.json` that parses (the script reports which).
- You have the recovery phrase. The restore in step 7 needs it.
- No file that a record holds by path is larger than 32 MiB. A snapshot seals each such file as one value, and a run holds values up to 32 MiB, so the first snapshot of a home with a larger one cannot be made, and the node would not start. The script reads `data/local-file-refs.json` and refuses when it finds one. Import the file into the store or drop the reference first.
- Time for the first snapshot: the files of the home go up once. The home is about as large as its `data/disk-run` and `data/disk-cas` directories together.

## Steps

1. **Stop the node** the way you normally do, and check that nothing answers: `lastdb status` must say that the node is not reachable. The script also refuses to run while a daemon holds the home.

2. **Keep a local copy of the home** until the move is proven. A clone copy on APFS takes no extra space until the files change:

   ```bash
   cp -cR ~/.lastdb ~/.lastdb-before-move
   ```

3. **Look at what the script would do**, then run it. It prints each change.

   ```bash
   python3 scripts/operations/adopt_old_home.py --home ~/.lastdb --lastdb <the new lastdb> --check
   python3 scripts/operations/adopt_old_home.py --home ~/.lastdb --lastdb <the new lastdb>
   ```

   It writes the derived id into `databases/registry.json`, replaces `log-sync/device-id` with a fresh 64-hex id, and moves the old `log-sync/<UUID>/` directory and `data/disk-run/accepted-v1/` whole into `~/.lastdb.old-build-<date>`. It refuses when the home has owned databases with their own ids, when the owner id is another 64-hex id, when a daemon runs, or when the layout is not the old one. A second run says "nothing to do". It never touches the store, the identity key or the credentials.

4. **Check the relay.** The script prints which relay the new build will use. If it says that `cloud-credentials.json` does not parse, run `lastdb backup connect`, or set `LASTDB_RELAY_URL` in the daemon's environment. The api key is not used by the signed log.

5. **Install the new build and start the node.** On this start the home has data and no spool, so the node keeps a copy of the files (hard links, so it takes little time or space), writes the spool state with `snapshot_required`, and starts. If the node cannot keep the copy, it does not start and says so.

6. **Wait for the first snapshot.** The node uploads the files and then names them. Check that:

   - the node log says `first snapshot published`;
   - `data/disk-run/accepted-v1/state.json` says `"snapshot_required": false`;
   - `~/.lastdb/.signed-initial-snapshot` is gone;
   - `lastdb status` shows the cloud sync as connected and without an error.

   Writes made while the snapshot goes up are in the spool and go up as entries after it. Nothing needs to stop.

7. **Prove it with a restore into a scratch directory.** On another path, from the phrase, against the same relay:

   ```bash
   lastdb backup restore --recovery-phrase-stdin --relay-url <relay> --into /tmp/scratch-home --json < phrase.txt
   ```

   The report must name a `snapshot`, and `replayed` must be about the writes since the snapshot, not the history of the home. Start a node on the scratch home (its own socket) and compare each schema with the real home: the number of keys (key listing) and a sample by key (`lastdb get <schema> --key-hash <id>`), the alternate keys you use, and one file. Stop the scratch node and delete the directory. The scratch node is a second device: it signs nothing unless you write to it.

8. **Check what CI could not.** The apps you have installed start and read their data. The brain and the board read as before. A write you make now arrives on the relay as one entry. If the old store held markers of writes that the old cloud never acknowledged, the node cleared them when it kept the copy, and the first later snapshot (`lastdb backup snapshot --wait`) must work.

9. **When you are satisfied**, remove `~/.lastdb.old-build-<date>` and `~/.lastdb-before-move`.

## What each check proves

A restore that names a snapshot and replays only the tail proves that the first snapshot is on the relay and covers the history. Equal key counts and samples prove that the snapshot holds what the home holds. A working `lastdb backup snapshot --wait` proves that nothing left over from the old upload path holds snapshots back.

## Going back

Until step 9 the old home is whole. To return to the old build:

1. Stop the new node.
2. Move the moved home away and put the copy back:

   ```bash
   mv ~/.lastdb ~/.lastdb-moved
   mv ~/.lastdb-before-move ~/.lastdb
   ```

3. Install the old build (`d06054159`) and start it.

The relay keeps the birth, the first snapshot and the entries written since. The old build does not read them, and they do no harm. Writes made on the new build after the move are not in the copy. They are on the relay, and in `~/.lastdb-moved`. If you need them, read them from there before you remove it.

Do not put the set-aside directory back into a moved home: its spool belongs to the old cloud log. To go back, use the whole copy.

## If a step fails

- The script refused: it changed nothing, and the message says why.
- The script stopped half way (a crash, a full disk): run it again. The registry is changed last, so a half run leaves a home that the new build still refuses, never half of one that starts.
- The node does not start and names the first snapshot: the copy of the files could not be kept (for example, the disk is full). Free the space and start again. No write was captured.
- The first snapshot does not come: `lastdb status` and the node log name the error of the last turn (the relay URL, the relay refusing, a full disk). The copy and the spool are kept, so the next turn tries again, and no write is lost in the meantime.
