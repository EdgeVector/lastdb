# Social

Social remembers verified contacts in the local personal database and manages
reader access to owned named LastDB databases set up for sharing. It uses the
LastDB owner socket for the selected home. Use Python 3.9 or later and the LastDB client
and daemon from the same build. The publisher needs a connected ExeMem account.

Create an owned database with `social create-db NAME`. This uses the existing
Social format for albums and photos. You can also share an existing named
database set up with LastDB's Social or Files format. Use the database name
with the Social command, and use LastDB's schema and record APIs for content.
`lastdb share contact --out PATH`
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

## Open the local app

Run `social --data-dir "$lastdb_home" open` on a running LastDB node. The
launcher opens a browser window for the local Social session. Add verified
contacts under People. Use Databases to create a share, see the applied roster,
revoke a reader, copy the sealed invitation, or accept one. The recipient's
cloud access and downloaded record readiness are shown separately. Use
`open --database ID` to go directly to a database's sharing settings.

TypeScript apps can use the [Social SDK helpers](../../lastdb_app_sdk/README.md#set-up-sharing-with-social)
for database creation, invitation, acceptance and revocation.

The picker filters by spec and intent. For example, run
`social --data-dir "$isolated_home" pick-database --spec social --version 1 --intent read`
to see choices. Add `--choose` to open the browser picker and wait for the
user's selection or cancellation. Add `--database ID --session` when an
owner has already made the choice. An approved selection includes a
short-lived `databaseSession`. A client uses the selected ID with the normal
LastDB data API and sends that value in `X-LastDB-Database-Session`. Keep the
session out of logs. The node limits a session to the selected database and
its approved operation. The existing owner socket still trusts programs that
run as its OS user. Run content apps outside that owner trust boundary before
using this as an app isolation guarantee.
