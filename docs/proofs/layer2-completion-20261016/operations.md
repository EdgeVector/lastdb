# Local native storage: operational commands

CAUTION: Use a fresh generated home. Do not name the primary, its copies, or the archive.

These commands use the product routes and the recovery command.
They do not contact the schema service or the cloud.
The final report records the frozen candidate and the observed results.

## Command paths and node scope

Set `candidate` and `recovery` to the frozen candidate binaries.
Use the original generated node home after each storage change.
The local data bundle excludes the node identity and encryption keys.
Keep those files in that original node home. Do not print them.

```bash
candidate=/absolute/path/to/lastdbd
recovery=/absolute/path/to/lastdb-disk-recovery
receipt_root="$(mktemp -d "${TMPDIR:?}/l2.XXXXXX")"
receipt_home="$receipt_root/node"
receipt_socket="$receipt_root/q.sock"
receipt_full_socket="$receipt_root/f.sock"
mkdir -p "$receipt_home"
env -u SENTRY_DSN -u FOLD_SENTRY_DSN \
  LASTDB_ISOLATED_COPY=1 LASTDB_SUPPRESS_RECOVERY_PHRASE=1 \
  LASTDB_DISK_PACK=1 LASTDB_RESIDENT_MODE=off \
  RUST_MIN_STACK=16777216 \
  "$candidate" --data-dir "$receipt_home" \
  --socket-path "$receipt_socket" --full-socket-path "$receipt_full_socket" \
  >"$receipt_root/node.log" 2>&1 </dev/null &
receipt_pid=$!
```

Wait for `/health` before each node start sequence continues.
Record the candidate hashes and the complete request and response files.

## Two keys for shared fields

Seed two schemas with `POST /api/schemas/seed-system`.
Each request sets `isolated_copy:true` and `schema.source:"system_seed"`.
Use the same fields and field descriptions in both schemas.
Use different schema names and different keys.

| Schema | Hash | Range | Fields |
|---|---|---|---|
| `Layer2ById` | `bucket` | `id` | `bucket`, `id`, `alias`, `title`, `body` |
| `Layer2ByAlias` | `bucket` | `alias` | `bucket`, `id`, `alias`, `title`, `body` |

The schema load binds the shared fields through proteins.
The app sends each record to one schema only.
Read the same record through both keys after that write.

Example schema body:

```json
{"isolated_copy":true,"schema":{"source":"system_seed","name":"Layer2ById","descriptive_name":"Layer 2 by ID","schema_type":"HashRange","key":{"hash_field":"bucket","range_field":"id"},"fields":["bucket","id","alias","title","body"],"field_descriptions":{"bucket":"generated local recovery hash","id":"generated local recovery identifier","alias":"generated local recovery alternate identifier","title":"generated local recovery title","body":"generated local recovery body"}}}
```

The second body changes the name, descriptive name, and range field only.
Declare both schemas before the first mutation batch.
Create generated records through `Layer2ById` with all five field values.
Each record uses one explicit `id` and one distinct explicit `alias`.
Send the records in one `POST /api/mutations/batch` call.
Update one record through `Layer2ByAlias`, then read both keys again.
Use `POST /api/queries/batch` for the two exact queries and the two bounded ranges.

## Local backup and source-free restore

`GET /api/db/native-status` reports the current generation and last durable batch.
It also reports an accepted batch and a failed writer.
This read does not request a flush.
Use it for the normal 500 ms completion observation.

First request the local durability barrier.
Then create a data bundle at a new absolute destination.

```bash
curl --fail-with-body --silent --show-error --max-time 90 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  --data-binary '{}' http://localhost/api/db/flush \
  >"$receipt_root/flush.json"
jq -nc --arg destination "$receipt_root/bundle" '{destination:$destination}' \
  >"$receipt_root/backup-request.json"
curl --fail-with-body --silent --show-error --max-time 1800 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  --data-binary "@$receipt_root/backup-request.json" \
  http://localhost/api/db/native-backup >"$receipt_root/backup.json"
kill -TERM "$receipt_pid"
wait "$receipt_pid"
mv "$receipt_home/data/disk-pack" "$receipt_root/source-unavailable"
"$recovery" restore --home "$receipt_home/data/disk-pack" \
  --backup "$receipt_root/bundle" >"$receipt_root/restore.json"
```

The restore command receives no source-home path.
Restart the original generated node home with the same candidate and sockets.
Read both exact keys and both bounded ranges.
Confirm the update and the absent keys from the saved request batch.
The restored data uses the original generated node identity and encryption keys.

## Local compaction after restore

Read a preview before the file change.
The native route uses the complete native home.
Do not supply a collection name.

```bash
curl --fail-with-body --silent --show-error --max-time 1800 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  --data-binary '{"dry_run":true}' http://localhost/api/db/compact \
  >"$receipt_root/compact-preview.json"
curl --fail-with-body --silent --show-error --max-time 1800 \
  --unix-socket "$receipt_socket" -H 'Content-Type: application/json' \
  --data-binary '{"dry_run":false,"grace_ms":0}' http://localhost/api/db/compact \
  >"$receipt_root/compact.json"
```

The explicit zero grace applies only to this generated home.
Read both exact keys and bounded ranges again.
Record the file counts, byte counts, generation, and last durable batch.

## Forward and reverse conversion

Create a separate fresh generated node with `LASTDB_DISK_PACK` unset.
Seed the same two schemas and send the same batch shape.
Use that node's original home for both later starts.
Stop the owned node cleanly before each conversion command.

```bash
"$recovery" convert --home "$legacy_home/data" \
  --target "$receipt_root/converted-native" >"$receipt_root/convert.json"
```

Restart the original legacy node home.
The saved conversion record selects the new native files.
Read both schema keys and ranges.
Send another mutation batch through the alternate key.
Request `/api/db/flush`, then stop that owned node cleanly.

```bash
"$recovery" reverse --home "$receipt_root/converted-native" \
  --target "$receipt_root/converted-legacy" >"$receipt_root/reverse.json"
```

Restart the original node home again.
Its conversion records select the new legacy files.
Read the latest values through both schema keys and ranges.
Reverse conversion uses the latest native data, including the later batch.

## Local scope and cloud gate

These commands cover Layer 2 local storage only.
The native cloud upload and bootstrap integration remains absent.
The native startup gate still refuses a cloud configuration.
The local bundle does not establish cloud recovery or primary upgrade readiness.

After the final report, stop each owned node.
Preserve the receipts before the supported cleanup removes the owned scratch data.
