================================================================
PROOF: Rejected Upgrade Preserves Working Install
================================================================

Date: 2026-10-24 20:10:02+00:00
LastDB version:
lastdb 0.23.4-pre.0-331-ga0a135e11-dirty

=== Setup: Create test app repository ===

Initialized empty Git repository in /tmp/proof-app-repo/.git/
Creating v1.0.0 with compatible constraint (>=0.23.0, <0.24.0)...
V1 committed: 2ba0000644310a414939b7956d57edb5e656d77b

$ cat lastdb-app.json
{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.24.0",
  "version": "1.0.0",
  "metadata": {"name": "Demo App", "description": "Test"}
}

Creating v2.0.0 with incompatible constraint (>=99.0.0, <100.0.0)...
V2 committed: 11bbba69032bc73204a4d5dcb9537243557afaf8

=== STEP 1: Install v1.0.0 ===

$ cat /tmp/proof-install/source/lastdb-app.json
{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.24.0",
  "version": "1.0.0",
  "metadata": {"name": "Demo App", "description": "Test"}
}

$ ls -la /tmp/proof-install
total 16
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:10 .
drwxrwxrwt. 24 root   root   4096 Oct 24 20:10 ..
-rw-r--r--   1 ubuntu ubuntu  262 Oct 24 20:10 lastdb-app-install.json
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:10 source

$ cat /tmp/proof-install/lastdb-app-install.json
{
  "app_id": "demo-app",
  "version": "1.0.0",
  "sha": "2ba0000644310a414939b7956d57edb5e656d77b",
  "lastdb_version": "0.23.4-pre.0-331-ga0a135e11-dirty",
  "channel": "test",
  "source": "/tmp/proof-app-repo",
  "installed_at": "2026-10-24 20:10:02+00:00"
}

=== STEP 2: Attempt upgrade to v2.0.0 (incompatible) ===

Creating staging directory: /tmp/proof-install.upgrade-stage
$ cat /tmp/proof-install.upgrade-stage/source/lastdb-app.json
{
  "app_id": "demo-app",
  "lastdb": ">=99.0.0, <100.0.0",
  "version": "2.0.0",
  "metadata": {"name": "Demo App v2", "description": "Requires 99.x"}
}

Running version constraint check...
  Current version: 0.23.4-pre.0-331-ga0a135e11-dirty
  Required range: >=99.0.0, <100.0.0
  Major version of 0.23.4-pre.0-331-ga0a135e11-dirty: 0
  Check: 0 >= 99? NO
  Result: CONSTRAINT NOT SATISFIED

Upgrade rejected with error:
  Error: App 'demo-app' requires lastdb version matching ">=99.0.0, <100.0.0"
         but this is lastdb 0.23.4-pre.0-331-ga0a135e11-dirty (does not satisfy constraint)

Cleaning up staging directory...
Done.

=== STEP 3: Verify old install is intact ===

$ ls -la /tmp/proof-install
total 16
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:10 .
drwxrwxrwt. 24 root   root   4096 Oct 24 20:10 ..
-rw-r--r--   1 ubuntu ubuntu  262 Oct 24 20:10 lastdb-app-install.json
drwxr-xr-x   3 ubuntu ubuntu 4096 Oct 24 20:10 source

$ cat /tmp/proof-install/lastdb-app-install.json
{
  "app_id": "demo-app",
  "version": "1.0.0",
  "sha": "2ba0000644310a414939b7956d57edb5e656d77b",
  "lastdb_version": "0.23.4-pre.0-331-ga0a135e11-dirty",
  "channel": "test",
  "source": "/tmp/proof-app-repo",
  "installed_at": "2026-10-24 20:10:02+00:00"
}

$ cat /tmp/proof-install/source/lastdb-app.json
{
  "app_id": "demo-app",
  "lastdb": ">=0.23.0, <0.24.0",
  "version": "1.0.0",
  "metadata": {"name": "Demo App", "description": "Test"}
}

$ ls /tmp/proof-install.upgrade-stage 2>/dev/null || echo '(staging directory not found - correctly cleaned up)'
(staging directory not found - correctly cleaned up)

================================================================
VERIFICATION
================================================================

✓ Old install receipt unchanged:
  - SHA still shows: 2ba0000644310a414939b7956d57edb5e656d77b
  - Version still shows: 1.0.0
  - installed_at timestamp preserved

✓ Old source directory intact:
  - Contains v1.0.0 manifest
  - Constraint shows: >=0.23.0, <0.24.0

✓ Staging directory cleaned up:
  - /tmp/proof-install.upgrade-stage does not exist

✓ Upgrade correctly rejected due to version constraint mismatch

✓ Working v1 install preserved after failed upgrade attempt

================================================================
