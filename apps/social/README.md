# Social

Social remembers verified contacts in the local personal database and manages
access to owned named LastDB databases set up for sharing. It is a command-line
tool. It uses the LastDB owner socket of the selected home. Use Python 3.9 or
later and the LastDB client and daemon from the same build. The publisher needs
a connected ExeMem account.

```sh
social --data-dir "$lastdb_home" COMMAND ...
```

`LASTDB_HOME` can stand in for `--data-dir`. A command prints a plain table on
a terminal and JSON when its output is piped or when you pass `--json`. An
error prints one `social:` line on standard error and exits non-zero.

| Command | What it does |
| --- | --- |
| `person add NAME --node-key KEY --contact FILE` | Verify a contact file and save the contact |
| `person list`, `person show PERSON` | The address book. One person, with what you share and what they share with you |
| `person rename PERSON NAME`, `person remove PERSON` | Rename a contact. Remove one that has no active grant and is in no circle |
| `circle list`, `circle show CIRCLE` | Circles, their members, and their linked databases |
| `circle set CIRCLE [--members A,B] [--link DB=SCOPE] [--unlink DB] [--rename NAME]` | Create a circle, or change the named parts of one |
| `circle remove CIRCLE` | Remove a circle and the access it grants |
| `db create NAME [--setup shared-albums] [--filtered]` | Create a whole-database share, optionally from a named setup |
| `db verify ID --setup NAME [--finish]` | Verify a database against a named setup |
| `db list`, `db show DB` | Databases. One database with its roster, circles, and pending change |
| `db accept [FILE] --publisher KEY [--name NAME]` | Accept a sealed invitation |
| `db retry [DB]` | Resume unfinished sharing updates |
| `grant list [DB]`, `grant revoke DB PERSON` | Active grants. Revoke one person |
| `grant set DB PERSON --write SCOPE`, `grant restore DB PERSON` | Give one person their own grant on a whole-database share. Restore a revoked person |
| `invite DB PERSON [--out FILE]` | Seal an invitation for a person |
| `request submit`, `request status ID` | For apps: propose a sharing change, read its outcome |
| `request list`, `request show ID`, `request approve ID`, `request decline ID` | For the owner: review and decide app proposals |
| `pick-database --spec NAME --version N --intent read\|write` | List the databases an app can use, or select one |
| `check` | Check the running LastDB for Social compatibility |

`PERSON` is a saved contact's name, or a node key. `DB` is a database name or
ID. `CIRCLE` is a circle name or ID.

## Preview, then apply

`circle set`, `circle remove`, `grant set`, `grant restore`, and `grant revoke`
change who can reach a database. Without `--apply` they print what would change and change nothing.

```sh
social circle set Family --members Ana,Bea --link family=social/Comment
social circle set Family --members Ana,Bea --link family=social/Comment --apply
```

A preview includes a `token`. Pass `--apply --token TOKEN` to make the change
only if it still matches that preview.

When another publisher tool changed a grant that Social manages, the command
stops and names each one. Decide each with `--resolve DB:PERSON=keep` to keep
the outside grant as a direct grant, or `--resolve DB:PERSON=social` to apply
Social's grant. Unrelated grants and explicit revocations are preserved.
A circle edit can instead take `--apply --defer-conflicts`: the other databases
are updated, and the one with the outside grant stays pending for
`db retry DB --resolve ...`.

Social applies each database independently and saves unfinished work. An
unfinished update only blocks new changes to the same database. `db retry`
resumes it.

## Contacts

`lastdb share contact --out PATH` exports a contact file and its public node
key. Check the key with the person through another channel, then save them:

```sh
social person add Ana --node-key "$ana_key" --contact ana.contact
```

## Whole-database sharing

`social db create NAME` creates an empty share for your app's own schemas.
Install the schemas through LastDB, then link a circle to the database.

A circle is one list of people with links to any number of databases. Each
link carries a write scope: `read`, `all`, or a comma-separated list of
canonical schema names such as `social/Comment`. Every member can read each
whole database. Write access combines across overlapping circles. `all` also
covers future schemas. An explicit revoke on a database stays in effect
across later circle edits.

```sh
social circle set Family --members Ana,Bea --link family=social/Comment --link trips=read --apply
social invite family Ana --out ana-invitation.json
```

For one person outside any circle, use a direct grant. It combines with the
person's circle grants. A revoked person comes back only through an explicit
`grant restore`.

```sh
social grant set family Cal --write social/Comment --apply
social grant revoke family Cal --apply
social grant restore family Cal --write read --apply
```

`invite` produces JSON with `databaseId`, `revision`, `publisher`, and
`invitation`. With `--out` it writes that receipt to a file only you can read.
Without it the receipt goes to standard output. Hand the sealed receipt to
the recipient. Keep it out of logs.
The recipient accepts it with the publisher's independently verified key:

```sh
social db accept ana-invitation.json --publisher "$trusted_publisher_key" --name family
```

`db accept` returns LastDB's received database metadata. The local LastDB
syncs its records from ExeMem. `db show` reports the recipient's cloud access
and downloaded record readiness separately.

Compatible schema upgrades keep a grant in place. The publisher still checks
the specific current definition and the record rules for each write. On a
whole-database share, a receiver with a `social/Comment` write grant can
create comments and edit comments they created. The publisher can edit or
delete any comment.

### Named setups

`social db create family --setup shared-albums` creates a whole-database share
with the catalog definitions for Album, AlbumItem, Photo, and Comment already
installed. Social records the new database ID before installing schemas and
resumes setup by the same name. `social db verify ID --setup shared-albums`
checks an owned or received share by its stable ID and reports the current
definition IDs. Add `--finish` to install missing schemas in an owned
database. Additive schema updates can introduce more fields under the same
name. When replacing a record, read it first, retain the fields and keys you
aren't editing, and send the replacement against the record's current
revision.

## Sharing proposals from apps

A local app lists circles with `social circle list` and submits a proposal as
JSON on standard input to `social request submit`. Reuse the same request UUID
when retrying the same declaration. The proposal is saved with
`awaitingApproval`. `social request status UUID` reports its outcome.

An app can request a circle binding, remove a binding, set a direct person
grant, revoke, or explicitly restore a person. It gives a list of installed
canonical schema names or one of the `viewer`, `commenter`, or
`album-organizer` presets.

The owner decides:

```sh
social request list
social request show UUID
social request approve UUID
social request decline UUID
```

`request show` prints the access the proposal would produce. After approval a
circle binding follows later changes to the circle. A decline is final, and
reusing a declined request's UUID returns the same outcome.

The owner socket trusts every program that runs as its OS user, so an app
running as the owner could run `request approve` itself. Run content apps
outside that trust boundary before treating owner approval as app isolation.

## Filtered shares

`social db create NAME --filtered` creates a share in the earlier Social
format for albums and photos. You can also manage an existing named database
set up with LastDB's Social or Files format. On these shares `invite` grants
reader access and seals the invitation in one step:

```sh
social invite family "$ana_key" --name Ana --contact ana.contact
```

You can omit `--name` and `--contact` when Social already remembers the
contact. Calling `invite` again on an active reader issues a fresh invitation
at the same revision, and on a revoked reader restores access. An active
contributor grant must be revoked before `invite` grants reader access.
`grant list` reports a database with a pending or unavailable roster in its
own status row. Other databases' confirmed access stays visible.

Social reads and updates the whole LastDB share configuration, preserving
other members. It records the exact pending request in the personal database
until LastDB confirms the result or definitively rejects it.

## How sharing works

Every active receiver can send an encrypted change to the pending mutation
log. A successful submission confirms that ExeMem queued it. Only the
publisher's LastDB can read pending changes. It checks each one against the
current schema permissions and record rules, discards refusals, and uploads
accepted changes to the separate published log. Receivers follow that log and
can read the resulting record after it syncs.
[How sharing works](../../docs/designs/shared-database-mutation-logs.md)

Social changes must preserve the two logs, the signed per-database roster and
schema grants, and the publisher's authority to approve each mutation.
[Requirements to preserve](../../docs/designs/shared-database-mutation-logs.md#requirements-to-preserve)

## Choosing a database for an app

`social pick-database --spec social --version 1 --intent read` lists the
databases that match. Add `--database ID` to check one selection, and
`--session` to issue a short-lived `databaseSession` for it. A client uses the
selected ID with the normal LastDB data API and sends that value in
`X-LastDB-Database-Session`. Keep the session out of logs. The node limits a
session to the selected database and its approved operation.

The TypeScript [LastDB app SDK](../../lastdb_app_sdk/README.md#set-up-sharing-with-social)
exposes `socialClient`, which runs these commands.
