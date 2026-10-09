# LastDB Mini secondary launchd nodes

Secondary Mini nodes such as `bob` should run with their own LastDB home and
their own Sentry environment tag. Do not add raw Sentry DSNs to a LaunchAgent
plist, repo file, board card, or shell history. Store the public DSN in
LastSecrets and reference the locator.

The repo-owned installer writes a secret-free LaunchAgent plus a small wrapper
that resolves `OBS_SENTRY_DSN` at process start:

```bash
scripts/lastdbd/install-secondary-launch-agent.sh \
  --name bob \
  --home "$HOME/.lastdb-bob" \
  --dsn-locator lastsecrets://obs-sentry-dsn-rust
```

Defaults:

- LaunchAgent label: `com.edgevector.lastdbd-bob`
- `OBS_SENTRY_ENVIRONMENT`: `bob`
- `OBS_SENTRY_INSTALL_ID`: `bob`
- Log files: `~/Library/Logs/lastdb/lastdbd-bob.log` and `.err.log`
- Wrapper: `~/.config/lastdb/launchd/com.edgevector.lastdbd-bob-env.sh`

The plist contains only `HOME`, `PATH`, and the wrapper path. The DSN value is
read by the wrapper with `lastsecrets get obs-sentry-dsn-rust` and is never
written back to disk by the installer.

Verification:

```bash
plutil -p ~/Library/LaunchAgents/com.edgevector.lastdbd-bob.plist | grep -i SENTRY
# expected: no output; the secret is not persisted in the plist

launchctl print gui/$(id -u)/com.edgevector.lastdbd-bob | grep -E 'state|pid|path'

pid="$(pgrep -f 'lastdbd.*lastdb-bob' | head -1)"
ps eww -p "$pid" | tr ' ' '\n' | grep -E '^OBS_SENTRY_(ENVIRONMENT|INSTALL_ID)='
# expected: OBS_SENTRY_ENVIRONMENT=bob and OBS_SENTRY_INSTALL_ID=bob
```

To prove the Sentry route, use a controlled secondary-node error or crash probe
only against the secondary home. Do not restart or kill the primary Mini as part
of bob validation.
