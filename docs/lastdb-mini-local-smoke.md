# LastDB Mini local smoke canary

The real-data Mini smoke canary boots a second `lastdbd` against a copy of the
live LastDB home. It validates the candidate binary without touching the
primary owner socket, but it temporarily runs two large Mini processes on the
same Mac.

Before starting the canary, run the repo-owned memory preflight:

```bash
scripts/lastdbd/smoke-memory-preflight.sh
```

The default floor is 4096 MiB available. That is intentionally conservative:
the primary brain has been observed near 2.4 GiB RSS and the COW smoke instance
can reach a similar footprint while loading real embeddings and schemas. Running
the canary below that floor risks macOS memory pressure and a Homebrew restart
of the primary `lastdbd`, which makes the smoke result noisy and can discard
in-flight sessions.

Use `--warn` only for diagnostics where the runner should continue and record
the memory state:

```bash
scripts/lastdbd/smoke-memory-preflight.sh --warn
```

The canary should record the primary PID before and after the run. A stable PID
is the expected result. If the PID changes, treat the smoke verdict as
inconclusive unless the log clearly proves an unrelated restart.

## Offline CoW baseline

The old one-shot cutover baseline helper has been retired with the rest of the
historical sled to LastStore tooling. For current validation, use the
`lastdb-safe-upgrade` flow: take a durable backup, run the candidate on a copy of
the real home, and only then consider a live upgrade. The historical helper
sources are in git history; reports are under `docs/history/mini-cutover/`.

## Known COW snapshot warning

The real-data smoke uses a copy of the live LastDB home rather than stopping the
primary node. That copy can catch the append-only HashRange order log between
the `moc:{molecule}` count update and every matching `mord:{molecule}:{seq}`
entry becoming visible in the copied database. In that case the smoke log may
contain:

```text
update_order append-log has fewer entries than its moc: count; returning scanned entries
```

For this smoke canary, that warning is expected when the run still reaches a
GREEN verdict and schema/search checks pass: the reader returns the scanned
entries it can prove exist. The same warning on a clean primary boot is not
expected and should be investigated as a possible lost order-log entry.
