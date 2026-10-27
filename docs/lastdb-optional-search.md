# Optional Search delivery

Search delivery is off by default. LastDB needs no LastSeek installation for normal reads or writes.
The default path creates no Search batches and does not copy mutation values into a Search queue.
This option controls external Search delivery. It does not change keyed database reads or storage search pages.

Start the daemon with `lastdbd --enable-search` to enable delivery to LastSeek.
For service starts, set `LASTDB_SEARCH_ENABLED=1` in that service's environment before its next start.
The values `1`, `true`, and `on` enable it. An absent variable leaves it off.
The flag applies to that process. It does not change a service configuration or start LastSeek.

Enable Search only with a current LastSeek that honors `.rebuild-required` in its inbox.
A first enable needs a full LastSeek rebuild from current LastDB records. Future batches contain only subsequent changes.
The prior Search consumer must not serve its old corpus after an interval with delivery off.
Disable delivery through the next daemon start without the flag or enabled environment value.
If an inbox already exists, that start records the rebuild requirement before further database writes.
Existing queue files stay in place. This change does not remove installed data or upgrade a daemon.

## Batch contents

A batch file is plain JSON outside the encrypted run files. A Search reader can keep the file after it reads it.
An upsert carries only the searchable fields of its schema.
A searchable field has the `word` classification and no `secret`, `no_index`, or `no-index` classification.
A schema with no field classifications has no allowlist, so its batches carry every field.
A batch never carries a file key. LastDB removes each `dek` from a `$lastdb_file` pointer and from its thumbnail.
The live writes and `lastdb search-rebuild` use the same rules (`search_index/inbox_fields.rs`).

CAUTION: Batch files that an earlier LastDB wrote can still hold every field and file keys.

## Queue limit

The live producer permits at most 64 MiB of batch file lengths and 4,096 files.
`LASTDB_SEARCH_QUEUE_MAX_BYTES` changes the byte cap between 64 KiB and 1 GiB.
An invalid value uses the default. Zero selects the minimum; it does not remove the cap.
File allocation can exceed file lengths. CI reports both values.

Producers serialize the queue decision and the atomic file publication.
The file count and a one-second bound limit the directory inspection.
At capacity, LastDB durably records `.rebuild-required` and stops further delivery.
An oversized single batch follows the same rule. Database writes continue.
The marker remains across idle periods and daemon restarts. A consumer drain does not clear it.
Existing batch files stay intact; the next changes require a complete Search rebuild.
An inbox that already exceeds the cap receives no new batches. No automatic cleanup deletes its older files.
Storage failures still return errors; the queue cap does not make an unavailable disk writable.

`GET /api/status` exposes `status.search`. `GET /api/db/native-status` exposes the same `search` object.
It reports the enable state, both limits, and the rebuild requirement.
An unavailable marker check reports an unknown rebuild state and a status error.
The status path reads one marker; it does not enumerate queued batches.

## Recovery after a gap

CAUTION: Do not clear the marker or replay old queue files into a rebuilt store before a full rebuild finishes.
Unseen deletes can leave old Search records. A count match cannot prove a complete corpus.

Stop database writes and the daemon through the normal owner process.
Keep the stopped database unchanged until recovery completes.
Use `LASTDB_SEARCH_INBOX=<separate-export-directory> lastdb --data-dir <home> search-rebuild` to export all current records.
That explicit offline export uses a separate directory. It does not resume or enlarge the live delivery queue.
Build a fresh LastSeek store from the complete export, with the normal schema and embedder configuration.
Drain all export batches in bounded passes and require no incomplete or unreadable batch.
Do not reuse the old LastSeek store: it can contain records deleted during the gap.
After the full rebuild, the owner clears the old queue batches and removes `.rebuild-required` last.
Bind the rebuilt store to the empty live inbox before database writes resume:
`lastseek drain --home <rebuilt-store> --inbox <home>/apps/search/inbox --consume`.
This saves the live source path after the separate export source.
Then select the rebuilt LastSeek store and start LastDB with Search enabled.
No automatic task performs these recovery steps or changes the primary.

## CI size figures

The 1.5× advisory applies to database files under `data/` and `blobs/`, divided by logical atom bytes.
CI separately reports the Search inbox, other home files, and the whole home.
Each checkpoint retains file lengths and allocated blocks. The raw categories add to the whole home.
Database or queue growth of at least 10% and 64 KiB requires a second comparison before a warning.
The default-off and enabled queue growth runs also verify durable writes and current values after saturation and reopen.
PRs use short runs. Nightly runs use more records and update rounds. Size findings stay advisory.
