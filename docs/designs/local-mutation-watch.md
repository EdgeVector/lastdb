# Local mutation watch (doorbell / short-TTL outbox)

**Status:** implemented on Mini (`lastdbd`)  
**Date:** 2026-07-19  
**Not:** cloud-synced · product truth · full-row CDC

## Problem

Apps (lastgit forge, later Discovery) idle-poll keyed tables even when nothing
changed. Polls are correctly **keyed**, but **chatty** (N repos × cycles).

## Product shape

```
mutation succeeds
  → append thin LocalWatchEvent to process-local ring
  → wake long-poll waiters on GET /api/local-watch

client sleeps on /api/local-watch?after_seq=&timeout_ms=
  → on events: keyed-read product tables (HashRange)
  → on gap:true: resync from product cursors/keys
```

| Property | Value |
|----------|--------|
| Storage | In-memory ring only (not sled, not uploaded) |
| TTL | ~10 minutes (also max 50k events) |
| Payload | `seq`, `ts_ms`, `schema`, `mutation_type`, `hash?`, `range?` |
| Truth | Still product schemas (RefEvent, BoardCards, …) |
| Socket | Same UDS as query/mutation (`/api/local-watch`) |

## Wire

```http
GET /api/local-watch?after_seq=0&timeout_ms=0
GET /api/local-watch?after_seq=12&timeout_ms=30000
GET /api/local-watch?after_seq=12&timeout_ms=30000&schema=BoardCards&hash=board-1
GET /api/local-watch?after_seq=12&timeout_ms=30000&schema=BoardCards&hash=board-1&start=todo%23&end=doing%23
```

Response (envelope-wrapped as usual):

```json
{
  "after_seq": 12,
  "tip_seq": 15,
  "gap": false,
  "events": [
    {
      "seq": 13,
      "ts_ms": 0,
      "schema": "…identity_hash…",
      "mutation_type": "create",
      "hash": "fkanban",
      "range": "mrr…:refs/heads/main:accepted"
    }
  ]
}
```

`timeout_ms` capped at 60s. Long-poll blocks a UDS worker thread.

`schema` accepts repeated or comma-separated values. `hash` accepts one exact
HashRange hash. `start` is inclusive and `end` is exclusive. The node wakes the
request only for matching events. The response still includes `tip_seq`, so the
client can advance its cursor across unrelated writes.

## Concurrency: watchers are capped, and the cap is visible

A blocking poll parks the UDS worker OS thread it was dispatched onto
(`LocalOutbox::poll_after` → `Condvar::wait_timeout`, inside `block_on`) and
takes **no QoS permit** — `acquire_op_permit` is only called on the
query/mutation/search paths. Measured against the live primary 2026-07-28, four
concurrent watchers moved `UDS pool in_flight` by exactly +4 while `QoS: total`
stayed `0/64`. Uncapped, `workers` concurrent watchers occupy the whole pool
with sleeping threads, real work queues behind `queue_cap`, and `lastdb status`
still reports the node idle.

So blocking watchers are admitted through `lastdb_node::watch_gate::WatchGate`:

- Cap = **half the UDS worker pool** (28 workers → 14), so watchers can never
  take the last worker. Override with `LASTDB_LOCAL_WATCH_MAX`.
- Over the cap the node **sheds explicitly** — `503`, immediately, naming the
  fallback — rather than silently consuming a worker.
- `timeout_ms=0` is **never gated**: it holds a worker only for the read, and it
  is the fallback a shed client drops to.
- `lastdb status` prints `Watchers: active/max peak=N sheds=N` next to `QoS` and
  `UDS pool`, so the occupancy QoS structurally cannot see has a number.
- `lastdb ops` reports this idle wait in its own section, never in "Top by total
  time" or "Slowest recent" — a sleeping watcher is not a consumer, and ranking
  it as one makes `lastdb ops` finger the wrong client during triage.

This bounds the failure mode; it does not remove the block. The complete fix is
to park the waiter off the worker and re-dispatch on wake, which needs an async
connection path end to end (`uds_http::serve_connection` is sync,
thread-per-request). Card:
`lastdb-long-poll-watchers-pin-uds-workers-invisible-to-qos`.

## Non-goals (v1)

- Cloud upload of the ring  
- WebSocket  
- Full row bodies on the wire  

## First consumer (follow-up)

lastgit forge: sleep on local-watch instead of per-repo RefEvent idle poll;
on wake, `listAcceptedRefEventsAfter` for affected repos still keyed.
