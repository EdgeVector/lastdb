================================================================
PROOF: Version Constraint Checking in Install and Upgrade
================================================================

=== Setup: Create test app repository ===

V1 SHA: 4eacb340d2742c146cfb7572acab4bf050ab3891
V2 SHA: 968d38a873061b187ced06e816d1b90456fb7334
V3 SHA: a4c4ff5ea46a2431ff189613fb01b15d0dd42347

=== Test 1: Install v1 (compatible constraint) ===

Calling install_pinned with v1...
SUCCESS: install_pinned returned:
  app_id: demo-app
  app_version: 1.0.0
  sha: 4eacb340d2742c146cfb7572acab4bf050ab3891
  install_dir: /tmp/proof-install
  checkout_path: /tmp/proof-install/source

--- Files after v1 install ---

Install directory:
total 16
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:30 .
drwxrwxrwt. 24 root   root   4096 Oct 24 20:30 ..
-rw-r--r--   1 ubuntu ubuntu  379 Oct 24 20:30 lastdb-app-install.json
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:30 source

Receipt (lastdb-app-install.json):
{
  "app_id": "demo-app",
  "channel": "test",
  "index": "test",
  "installed_at": "2026-10-24T20:30:55.978558285+00:00",
  "lastdb_version": "0.23.4-pre.0-331-ga0a135e11",
  "proof_run": "proof-run-1",
  "proved_at": "2026-10-24T20:00:00Z",
  "sha": "4eacb340d2742c146cfb7572acab4bf050ab3891",
  "source": "file:///tmp/proof-app-repo",
  "trust": "test",
  "version": "1.0.0"
}
Manifest (source/lastdb-app.json):
{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.24.0",
  "version": "1.0.0",
  "metadata": {"name": "Demo App v1"}
}


=== Test 2: Upgrade to v2 (incompatible constraint) ===

Calling upgrade_pinned with v2...
EXPECTED ERROR: app 'demo-app' requires lastdb >=99.0.0, <100.0.0, but you have 0.23.4-pre.0-331-ga0a135e11 (core version 0.23.4)

--- Files after failed v2 upgrade (should still be v1) ---

Install directory:
total 16
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:30 .
drwxrwxrwt. 24 root   root   4096 Oct 24 20:30 ..
-rw-r--r--   1 ubuntu ubuntu  379 Oct 24 20:30 lastdb-app-install.json
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:30 source

Receipt (lastdb-app-install.json):
{
  "app_id": "demo-app",
  "channel": "test",
  "index": "test",
  "installed_at": "2026-10-24T20:30:55.978558285+00:00",
  "lastdb_version": "0.23.4-pre.0-331-ga0a135e11",
  "proof_run": "proof-run-1",
  "proved_at": "2026-10-24T20:00:00Z",
  "sha": "4eacb340d2742c146cfb7572acab4bf050ab3891",
  "source": "file:///tmp/proof-app-repo",
  "trust": "test",
  "version": "1.0.0"
}
Manifest (source/lastdb-app.json):
{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.24.0",
  "version": "1.0.0",
  "metadata": {"name": "Demo App v1"}
}


=== Test 3: Upgrade to v3 (compatible constraint) ===

Calling upgrade_pinned with v3...
SUCCESS: upgrade_pinned returned:
  app_id: demo-app
  upgraded: true
  installed_version: 1.0.0
  resolved_version: 3.0.0
  install.sha: a4c4ff5ea46a2431ff189613fb01b15d0dd42347
  install.install_dir: /tmp/proof-install
  install.checkout_path: /tmp/proof-install/source

--- Files after successful v3 upgrade ---

Install directory:
total 16
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:30 .
drwxrwxrwt. 24 root   root   4096 Oct 24 20:30 ..
-rw-r--r--   1 ubuntu ubuntu  379 Oct 24 20:30 lastdb-app-install.json
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:30 source

Receipt (lastdb-app-install.json):
{
  "app_id": "demo-app",
  "channel": "test",
  "index": "test",
  "installed_at": "2026-10-24T20:30:56.016760143+00:00",
  "lastdb_version": "0.23.4-pre.0-331-ga0a135e11",
  "proof_run": "proof-run-3",
  "proved_at": "2026-10-24T20:00:00Z",
  "sha": "a4c4ff5ea46a2431ff189613fb01b15d0dd42347",
  "source": "file:///tmp/proof-app-repo",
  "trust": "test",
  "version": "3.0.0"
}
Manifest (source/lastdb-app.json):
{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.25.0",
  "version": "3.0.0",
  "metadata": {"name": "Demo App v3 - wider range"}
}

Cleanup verification (no .upgrade-stage or .upgrade-backup):
ls: cannot access '/tmp/proof-install.*/': No such file or directory
None found (expected)


================================================================
Proof complete
================================================================
```

## Key Observations

1. **V1 install succeeded**: Receipt shows SHA `4eacb340...`, version `1.0.0`, manifest constraint `>=0.23.0, <0.24.0`
2. **V2 upgrade rejected**: Error message: `app 'demo-app' requires lastdb >=99.0.0, <100.0.0, but you have 0.23.4-pre.0-331-ga0a135e11 (core version 0.23.4)`
3. **V1 remains intact after rejection**: Receipt and manifest unchanged, still v1 with original SHA and timestamps
4. **V3 upgrade succeeded**: Receipt shows SHA `a4c4ff5e...`, version `3.0.0`, manifest constraint `>=0.23.0, <0.25.0`
5. **install_dir paths are final**: Returned `install.install_dir` and `install.checkout_path` point to `/tmp/proof-install` (not staging)
6. **Cleanup verified**: No `.upgrade-stage` or `.upgrade-backup` directories remain after operations
