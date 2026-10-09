# LastDB App Scaffolds

This directory holds zero-UI app scaffolds that can be driven by agents before
they grow a packaged binary or MCP server. Each app owns a `folddb.toml`
manifest, app-owned schema declarations, and a machine-readable command surface.

`registry.json` is the local pickup index for agents and tests. It is not a
package manager; the canonical app registry still lives behind the `folddb`
publish flow.
