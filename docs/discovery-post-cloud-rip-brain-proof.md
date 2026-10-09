# Discovery Post-Cloud Brain Refresh Proof

Date: 2026-07-16

This note records the repository-side proof for the F-Kanban card
`brain-refresh-discovery-post-cloud-rip`.

## Brain Updates

Appended maturity corrections to:

- `discovery-app-standalone-local-fof-architecture`
- `active-programs`

The corrections mark the old "recording adapters" Discovery wording as stale
and point agents at the live LastDB Mini product path:

- Discovery is canonical at `https://github.com/EdgeVector/discovery`.
  The 2026-07-16 note named `lastdb:///discovery`. That LastGit remote is retired.
- Cloud Discovery ANN and `/api/discover/*` are retired product paths.
- Exemem remains transport-only for opaque encrypted slices.
- Live Mini adapters, authenticated relay provenance, three-node capstone,
  deliver request/grant, multi-handle slice, and one-command dogfood have
  landed.
- Remaining Discovery work should come from live PR-sized board cards, not from
  stale program prose.

## Fold Agent-Doc Scan

Checked fold agent-facing docs for current-product claims about the old cloud
Discovery service:

```bash
rg -n "DISCOVERY_SERVICE|ExememDiscovery|/api/discover|recording adapters|cloud Discovery|Discovery Lambda|live cloud|discovery.*lambda|lambda.*discovery" \
  CLAUDE.md AGENTS.md README* docs crates lastdb_node fold_db schema_service apps
```

Result: no fold agent guidance claimed a live cloud Discovery product path. The
only hit was an unrelated `schema_service` lambda test name, so no fold prose
needed editing.
