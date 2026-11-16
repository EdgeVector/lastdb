"""The `social NOUN VERB` command grammar."""

import argparse
import os
from pathlib import Path


def flag(*names, **options):
    return names, options


APPLY = (
    flag("--apply", action="store_true", help="Make the change. Without it the command only previews"),
    flag("--token", help="With --apply, refuse unless the change still matches this preview"),
)
RESOLVE = flag("--resolve", action="append", metavar="DATABASE:PERSON=keep|social",
               help="Keep a grant made outside Social as a direct grant, or apply Social's grant")
PERSON = flag("person", help="Saved contact name, or node key")
DEFER = flag("--defer-conflicts", action="store_true",
             help="With --apply, leave a database with an unresolved outside grant pending for `db retry`")
DATABASE = flag("database", help="Database name or ID")
WRITE = flag("--write", default="read", metavar="read|all|Schema,Schema",
             help="Write scope. The person always reads the whole database. Default: read")
SETUP_VERSION = flag("--version", type=int, default=1, help="Setup version")

COMMANDS = {
    "person": ("Saved contacts", {
        "add": ("Verify a contact file and save the contact", (
            flag("name"), flag("--node-key", required=True, help="Independently verified node key"),
            flag("--contact", required=True, type=Path, help="Contact file from `lastdb share contact`"))),
        "list": ("List saved contacts", ()),
        "show": ("Show what you share with a person and what they share with you", (PERSON,)),
        "rename": ("Change a contact's name", (PERSON, flag("name", help="New name"))),
        "remove": ("Remove a contact that has no active grant and no circle membership", (PERSON,)),
    }),
    "circle": ("Reusable lists of people linked to databases", {
        "list": ("List circles with their members and linked databases", ()),
        "show": ("Show one circle", (flag("circle", help="Circle name or ID"),)),
        "set": ("Create a circle or change the named parts of one", (
            flag("circle", help="Circle name or ID"),
            flag("--members", metavar="PERSON,PERSON", help="Replace the members. Use '' for none"),
            flag("--link", action="append", default=[], metavar="DATABASE=read|all|Schema,Schema",
                 help="Link a database with a write scope. Members always read the whole database"),
            flag("--unlink", action="append", default=[], metavar="DATABASE", help="Remove a database link"),
            flag("--rename", metavar="NAME"), RESOLVE, DEFER, *APPLY)),
        "remove": ("Remove a circle and the access it grants", (
            flag("circle", help="Circle name or ID"), RESOLVE, DEFER, *APPLY)),
    }),
    "db": ("Shared databases", {
        "create": ("Create a whole-database share", (
            flag("name"), flag("--setup", help="Install a named setup, for example shared-albums"),
            SETUP_VERSION,
            flag("--filtered", action="store_true", help="Use the filtered albums and photos format"))),
        "verify": ("Verify a database against a named setup", (
            flag("database", help="Exact database ID"), flag("--setup", required=True), SETUP_VERSION,
            flag("--finish", action="store_true", help="Install missing setup schemas in an owned database"))),
        "list": ("List owned and received databases", ()),
        "show": ("Show one database, its roster and any pending change", (DATABASE,)),
        "accept": ("Accept a sealed invitation", (
            flag("receipt", nargs="?", help="Invitation file. Default: read standard input"),
            flag("--publisher", required=True, help="Independently verified publisher node key"),
            flag("--name", help="Local database name"))),
        "retry": ("Resume unfinished sharing updates", (
            flag("database", nargs="?", help="Database name or ID. Default: all"), RESOLVE)),
    }),
    "grant": ("Access for one person on one database", {
        "list": ("List active grants", (flag("database", nargs="?", help="Database name or ID. Default: all"),)),
        "set": ("Give one person their own grant on a whole-database share", (
            DATABASE, PERSON, WRITE, RESOLVE, *APPLY)),
        "restore": ("Restore a revoked person on a whole-database share", (
            DATABASE, PERSON, WRITE, RESOLVE, *APPLY)),
        "revoke": ("Revoke a person's access", (DATABASE, PERSON, APPLY[0])),
    }),
    "invite": ("Seal an invitation for a person. A filtered share also grants reader access", (
        DATABASE, PERSON, flag("--name", help="Contact name, with --contact"),
        flag("--contact", type=Path, help="Contact file for a new or updated contact"),
        flag("--out", type=Path, help="Write the sealed receipt to this file, readable by you only"))),
    "request": ("Sharing changes proposed by apps", {
        "submit": ("Save a JSON declaration from standard input for owner review", ()),
        "status": ("Read the outcome of a proposal", (flag("request_id"),)),
        "list": ("List proposals awaiting a decision", (flag("--cursor", help="Continue from nextCursor"),)),
        "show": ("Preview what approving a proposal changes", (flag("request_id"), RESOLVE)),
        "approve": ("Approve a proposal and apply it", (flag("request_id"), RESOLVE, APPLY[1])),
        "decline": ("Decline a proposal", (flag("request_id"),)),
    }),
    "pick-database": ("List databases an app can use, or select one by ID", (
        flag("--spec", required=True), flag("--version", required=True, type=int),
        flag("--intent", required=True, choices=("read", "write")), flag("--database", help="Exact database ID"),
        flag("--session", action="store_true", help="Issue a data session for the selected database"))),
    "check": ("Check the running LastDB for Social compatibility", ()),
}


def parser():
    home = os.environ.get("LASTDB_HOME")
    command = argparse.ArgumentParser(
        prog="social", description="Remember people and share databases from one LastDB home.")
    command.add_argument("--data-dir", required=not home, default=home, type=Path)
    command.add_argument("--lastdb", type=Path, help="LastDB executable. Default: lastdb on PATH")
    command.add_argument("--json", action="store_true", help="Print JSON. This is the default when piped")
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    nouns = command.add_subparsers(dest="noun", required=True, metavar="COMMAND")
    for noun, (summary, body) in COMMANDS.items():
        if isinstance(body, dict):
            verbs = nouns.add_parser(noun, help=summary).add_subparsers(dest="verb", required=True, metavar="VERB")
            leaves = [(verbs.add_parser(verb, help=text, parents=[shared]), flags)
                      for verb, (text, flags) in body.items()]
        else:
            leaves = [(nouns.add_parser(noun, help=summary, parents=[shared]), body)]
        for leaf, flags in leaves:
            for names, options in flags:
                leaf.add_argument(*names, **options)
    return command
