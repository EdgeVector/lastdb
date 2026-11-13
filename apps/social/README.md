# Social

Social remembers verified contacts in the local personal database and manages
reader access to Social v1 databases. It uses the LastDB owner socket and an
explicit, isolated LastDB home. Use Python 3.10 or later and the LastDB client
and daemon from the same build. The publisher needs a connected ExeMem account.

Create an owned database with `social create-db NAME`. Use LastDB's normal
schema and record APIs to put content in it. `lastdb share contact --out PATH`
exports a contact file and its public node key. Check the key independently
with the person before granting access.

```sh
apps/social/social --data-dir "$publisher_home" --lastdb "$lastdb_bin" \
  add --database family --user Ana --node-key "$ana_key" --contact "$ana_contact"
```

`add` returns JSON with `databaseId`, `revision`, `publisher`, and `invitation`.
The caller hands the sealed invitation receipt to the recipient. Keep it out
of logs. The recipient passes that JSON to `accept` on stdin, with the
publisher's independently verified key.

```sh
apps/social/social --data-dir "$recipient_home" --lastdb "$lastdb_bin" \
  accept --publisher "$trusted_publisher_key" --name family < "$receipt"
```

`accept` returns LastDB's received database metadata, including its verified
spec. The local LastDB syncs its records from ExeMem. `grants --database NAME`
returns the applied revision and member list. `list` shows active grants with
known names. `remove --database NAME --user Ana` revokes access. Calling `add`
again restores a revoked reader. Calling it on an already active reader issues
a fresh invitation at the same revision. You can omit `--contact` when Social
already remembers the verified contact for that node key.
An active contributor grant must be revoked before using `add` for reader
access. A revoked contributor returns as a reader when you use `add`.
An old pending contributor regrant requires owner review before retry.
`list` and `grants` report a database with a pending or unavailable roster
using its own status row. Other databases' confirmed access stays visible.

Social reads and updates the whole LastDB share configuration, preserving
other members. It records the exact pending request in the personal database
until LastDB confirms the result or definitively rejects it. A later `add` or
`remove` can resume delivery. LastDB seals the invitation after the grant.
