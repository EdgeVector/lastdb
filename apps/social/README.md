# Social

Social remembers verified contacts in the local personal database and manages
access to owned named LastDB databases set up for sharing. It uses the
LastDB owner socket for the selected home. Use Python 3.9 or later and the LastDB client
and daemon from the same build. The publisher needs a connected ExeMem account.

## Whole-database sharing

Use `social create-db --whole-database NAME` to create an empty share for
your app's own schemas. For the standard album schemas, use the shared-albums
setup below. After confirming a circle grant, use
`social seal-invitation --database ID --node-key VERIFIED_KEY` to get the
sealed receipt for the caller to deliver. This uses the same operation as
**Seal invitation** in the app.

The TypeScript [LastDB app SDK](../../lastdb_app_sdk/README.md#set-up-sharing-with-social)
exposes `socialClient` for creation, setup, invitations, acceptance, and
revocation. It calls the installed Social app. Use the returned stable
database ID with the regular LastDB client for schemas, records, and files.
Use the Circles screen to set reusable people and schema grants.

### Accept sharing declarations from apps

Social 0.5.0 lets a local app list reusable circle names and IDs with
`social list-circles`, then submit a sharing request as JSON on stdin to
`social request-sharing`. Reuse the same request UUID when retrying the
same declaration. The command saves the proposal with `awaitingApproval`
and returns the request ID. Use `social open --sharing-request UUID` to
put the proposal in front of the owner and `social sharing-status UUID`
to look up its outcome after the owner reviews it. In Social 0.6.0, choosing
**Decline request** saves a terminal `declined` status. Closing the review
leaves it awaiting approval. Reusing a declined request's UUID returns the
same outcome.

An app can request a circle binding, remove a binding, set a direct person
grant, revoke, or explicitly restore a person. Give a list of installed
canonical schema names or choose the `viewer`, `commenter`, or
`album-organizer` preset. A full write scope includes future schemas and
requires review. After approval, the binding follows changes to the circle.
Social applies each database independently and saves unfinished work for
retry when it opens again. An unfinished update only blocks new changes that
affect the same database. The owner can review a new app request for another
database while the earlier change is pending. Reopen any Social windows
after upgrading. Once Social 0.6.0 saves the circle directory, use 0.6.0
or later to edit circles and resume its sharing updates.

The owner sees the current and proposed grants when another publisher tool
changes a grant that Social manages. They can preserve the outside grant as
a direct grant or approve Social's proposed write scopes. Unrelated grants
and explicit revocations are preserved. Use `seal-invitation` after a
new recipient's grant is confirmed.

## Existing filtered shares

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
revoke access, copy the sealed invitation, or accept one. The recipient's
cloud access and downloaded record readiness are shown separately. Use
`open --database ID` to go directly to a database's sharing settings.

Choose **Whole database with circle write access** when creating a database
for circles. Install the database's schemas through LastDB, then open
**Circles** to create one membership list with links to several databases.
Use a LastDB build with named schema grants for this screen.
Set the write scope on each link and preview the changes before applying them.
A member can read each whole database. Write access combines across overlapping
circles, and an explicit revoke on a database stays in effect across later circle
edits. If one database update fails, review the unfinished changes and retry it.
Existing local circles remain editable under the database. Use **Seal invitation**
on each database to hand its new recipients their sealed receipts.

Write grants use the installed schema's canonical name, for example
`social/Comment`. Compatible upgrades keep the grant in place. The publisher
still checks the specific current definition and the record rules for each write.
The **All schemas** choice also covers future schemas. An older exact-ID circle
grant can be converted by reviewing and saving its circle settings.

On a whole-database share, LastDB lets a receiver with a `social/Comment`
write grant create comments and edit comments they created. The publisher can
edit or delete any comment. These rules apply under every circle and every app.

Choose **Shared albums (version 1)** to create a whole-database share with
the catalog definitions for Album, AlbumItem, Photo, and Comment already
installed. Social records the newly created database ID before installing
schemas and can resume setup by the same provisioning name. To reopen a
compatible owned or received share, select its stable ID. Reopening verifies
that the required fields and keys are present and reports the current definition
IDs. Additive schema updates can introduce more fields under the same name.
When replacing a record, read it first, retain the fields and keys you aren't
editing, and send the replacement against the record's current revision.

```sh
social --data-dir "$lastdb_home" setup-database --setup shared-albums --version 1 --name family
social --data-dir "$lastdb_home" setup-database --setup shared-albums --version 1 --database DATABASE_ID
```

For an incomplete owned setup, pass `--finish-existing` with its exact ID.
Social verifies the schemas already present before installing missing ones.
With retryable whole-database creation, setup by local name reserves its ID
before creating the database and can resume by the same name.

Apps use the sharing settings to show which changes the publisher allows.
Every active receiver can send an encrypted change to the pending mutation
log. A successful submission confirms that ExeMem queued it. Only the
publisher's LastDB can read pending changes. It checks each one against the
current schema permissions and record rules, discards refusals, and uploads
accepted changes to the separate published log. Receivers follow that log
and can read the resulting record after it syncs.
[How sharing works](../../docs/designs/shared-database-mutation-logs.md)

Social changes must preserve the two logs, the signed per-database roster and
schema grants, and the publisher's authority to approve each mutation.
[Requirements to preserve](../../docs/designs/shared-database-mutation-logs.md#requirements-to-preserve)

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
