# Admin Kanban hourly deliver (last-day board cards (all columns))

## Status of the dependency chain

| Card | State | What it shipped |
|------|-------|-----------------|
| `query-two-pass-field-filter` | **done** (fold PR #538) | Query `where` / `FieldPredicate` two-pass scan (incl. `after` on timestamps) |
| `deliver-time-window-last-day` | **done** (fold PR #563) | Deliver stage accepts `since: "24h"`, `since_field`, `columns_include`/`exclude`, `order_by` |
| `deliver-scheduled-config-and-runner` | **this script** | Durable config + hourly runner that stages/approves last-day cards across **backlog/todo/doing/done** |

Snapshot and mailbox legs list each column through **BoardCards HashRangePrefix**
(`hash=board`, `prefix="{column}#"`). They do not query Card with `where` /
`in(column)`. Mailbox chunks point-get Card by slug after BoardCards returns
the membership list.

```json
{
  "schema_name": "fkanban/BoardCards",
  "board": "default",
  "fields": ["slug", "title", "column", "updated_at", "tags", "assignee"],
  "columns_include": ["backlog", "todo", "doing", "done"]
}
```

Hashes come from live kanban `schemaHashes.board_cards` / `schemaHashes.card`
(`KANBAN_CONFIG`, `FKANBAN_CONFIG`, `~/.kanban/config.json`, or
`~/.fkanban/config.json`). Do not pin a stale Card hash in the deliver JSON.
Override with `LASTDB_ADMIN_DELIVER_BOARD_CARDS_SCHEMA_HASH` or
`LASTDB_ADMIN_DELIVER_CARD_SCHEMA_HASH` only for a one-off. A second snapshot
fire skips while the previous process still holds the lock file next to the
config.

## One-time setup

```bash
# 1. Example config into Mini home
python3 scripts/admin-kanban-hourly-deliver/deliver.py \
  --write-example-config ~/.lastdb/admin-kanban-hourly-deliver.json

# 2. Point recipient at the enrolled admin consumer
#    Option A — AWS SM (prod/dev enroll script writes this):
#      "recipient": { "secret_id": "ExememKanbanConsumer-prod", "region": "us-east-1" }
#    Option B — env (never commit keys):
export LASTDB_ADMIN_DELIVER_RECIPIENT_JSON='{"recipient_pubkey":"...","messaging_public_key":"...","messaging_pseudonym":"..."}'

# 3. Dry-run (no network)
python3 scripts/admin-kanban-hourly-deliver/deliver.py --dry-run

# 4. Fire once for real
python3 scripts/admin-kanban-hourly-deliver/deliver.py
```

## Dogfood proof without sending to Exemem

For a repeatable Mini-side proof that does not use live recipient keys, does
not send to Exemem, and does not leave a pending delivery behind, use a config
with `auto_approve: false` and run:

```bash
LASTDB_ADMIN_DELIVER_RECIPIENT_JSON='{"recipient_pubkey":"dogfood-test","messaging_public_key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","messaging_pseudonym":"00000000-0000-0000-0000-000000000001"}' \
python3 scripts/admin-kanban-hourly-deliver/deliver.py \
  --config /tmp/admin-kanban-hourly-deliver-dogfood.json \
  --cleanup-staged
```

`--cleanup-staged` stages a real `fkanban/Card` slice through
`POST /api/sharing/deliver`, verifies the delivery appears in
`GET /api/sharing/deliveries`, then rejects it with
`POST /api/sharing/deliveries/{id}/reject`. It refuses to run unless
`auto_approve` is false.

## Hourly cadence (routines)

Register a disk routine (scheduler survives brain outages):

```toml
# ~/.routines/registry/admin-kanban-hourly-deliver.toml
harness        = "shell"
# If shell harness is unavailable, use a tiny prompt that runs the script via claude/codex
# with `cwd` set, or wrap with launchd. Preferred: shell adapter when present.
rrule          = "FREQ=HOURLY;INTERVAL=1"
status         = "active"
timeout_min    = 10
cwd            = "/Users/YOU/code/edgevector/fold"
prompt         = "Run exactly: python3 scripts/admin-kanban-hourly-deliver/deliver.py && echo RESULT:ok"
```

Or a launchd `StartInterval` = 3600 calling the same command.

## Factory profile (admin Kanban Factory theater, ~1 min)

For the animated admin Factory view you want a **slim full-board** slice more
often — not done-only, not 4 cards/column if doing is hot.

```bash
# Example factory config
python3 scripts/admin-kanban-hourly-deliver/deliver.py \
  --write-example-config ~/.lastdb/admin-kanban-factory-deliver.json

# Dry-run (shows legs with assignee fields + 30d window + per-column caps)
python3 scripts/admin-kanban-hourly-deliver/deliver.py \
  --config ~/.lastdb/admin-kanban-factory-deliver.json --dry-run
```

Or set `"profile": "factory"` in any config (explicit keys still override):

| Knob | Factory default |
|------|-----------------|
| `since` | `720h` (30d) so quiet backlog cards still appear |
| `fields` | hourly fields + `assignee`, `block_status`, `branch` |
| `max_records_by_column` | doing/todo **40**, backlog **30**, done **24** |
| bodies | **never** included (size budget) |

**Why a size budget at all?** Exemem messaging rejects `encrypted_blob` over
**87382 base64 chars (~64KB)**. Mini enforces the same at approve time
(`MAX_BLOB_B64_CHARS` in `lastdb_node` deliver). That is a hard transport limit
for the blind mailbox (not “crypto can only do 64KB”). Raising it means a
product change in messaging + Mini, not just the script. Until then, more cards
⇒ lower fields, not bigger blobs.

Cadence is **not** inside LastDB — use launchd every 60s:

```bash
# Edit paths if needed, then:
cp scripts/admin-kanban-hourly-deliver/launchd/com.edgevector.admin-kanban-factory-deliver.plist \
   ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) \
  ~/Library/LaunchAgents/com.edgevector.admin-kanban-factory-deliver.plist
```

If a send exceeds the sealed-message cap, lower `max_records_by_column` (done
first) — the runner does not invent a new transport.

## Multi-app admin tabs

This runner is Kanban-specific. Other apps (routines, brain, …) reuse the same
deliver path with their own publishers; schedule those similarly once each
publisher dogfood lands.
