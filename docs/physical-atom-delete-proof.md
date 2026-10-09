# Physical atom delete proof

A point-read miss proves logical absence. It does not prove that old segment bytes no longer contain the payload.

Use `scripts/lastdbd/physical_atom_audit.py` on a stopped, owned test home. Supply exact deleted atom IDs and a retained atom ID. The audit reads only the named atoms collection with fixed path depth, file, byte, and time limits.

```sh
python3 scripts/lastdbd/physical_atom_audit.py \
  --atoms-root /absolute/owned/home/data/data/atoms \
  --deleted <64-lowercase-hex-atom-id> \
  --control <64-lowercase-hex-retained-atom-id>
```

Repeat `--deleted` and `--control` as required. The command returns 0 only when every deleted payload Put is absent and every retained payload Put is present. A Put followed by a Delete remains a physical failure. An empty match set cannot pass the retained control. Unsupported or malformed segments refuse the audit.

The scanner accepts native `atom\0<id>`, legacy `atom:<id>`, and partition-prefixed atom keys. It reports scoped legacy bodies too. Each segment has a digest in the report. Compare those digests when two audits must prove unchanged files. The caller must preserve the source release, snapshot receipt, atom IDs, and audit phase separately.

## Tiny DEV cloud proof, 2026-09-12

Release `b96c2b266a1bbfee39953773750f95db5fa19c59` used three synthetic records in a fresh DEV identity. Two durable Deletes removed the keys. Scoped orphan GC and atom compaction removed the local payload bodies.

The first fresh restore passed point reads and exact atom misses across two cold boots. Its physical audit failed. Snapshot counter 3 still referenced two old 696-byte Put prefixes. Local compaction retired the later 768-byte Put-plus-Delete digests. The restore downloaded the old Puts, then replay appended Delete markers.

An exact two-chunk cloud rewrite removed those old prefixes through the normal manifest receipt and CAS path. The next fresh restore passed a physical audit before any daemon boot. Two cold boots preserved both logical absence and physical absence. The retained control passed all checks.

This test shows why the small cloud test precedes a production restore. It also shows that local compaction alone does not prove cloud payload reclamation. The prior Python proof missed native NUL keys; a retained positive control exposed its false result.

Run the offline scanner tests with `python3 tests/test_physical_atom_audit.py`.
