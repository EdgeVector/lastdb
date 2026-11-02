# LastDB Mini local smoke canary

The Mini smoke canary boots a second `lastdbd` on a fresh empty home with
synthetic data. It validates the candidate binary without touching the primary
owner socket. It never copies the live LastDB home (Tom, 2026-10-12). It
temporarily runs a second Mini process on the same Mac.

Before starting the canary, run the repo-owned memory preflight:

```bash
scripts/lastdbd/smoke-memory-preflight.sh
```

The default floor is 4096 MiB available. That is intentionally conservative:
the primary brain has been observed near 2.4 GiB RSS and a second Mini
process adds load on top of it. Running
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

## Offline baseline

The old one-shot cutover baseline helper is retired. A current home is a format 8
home on disk format v2. For current validation, use the `lastdb-safe-upgrade`
flow: take a durable backup, run the candidate on synthetic data in a fresh
empty home, and only then consider a live upgrade.
