"""Local LastDB Files operations on a generated named database."""

import hashlib
import json
import os
import subprocess

def require(value, message):
    if not value:
        raise RuntimeError(message)


def subcommands(result):
    require(result.returncode == 0, "Share help failed: " + result.stderr)
    return {line.split()[0] for line in result.stdout.splitlines() if line.strip()}


def check_addressing(command, cli, owner, publication_id, missing_id):
    catalog = "lastdb-files/FileCatalog"
    listed = command(cli, "--data-dir", owner.home, "db", "list", "--json")
    require(listed.returncode == 0, "database list failed: " + listed.stderr)
    databases = json.loads(listed.stdout)["databases"]
    require(any(db["id"] == publication_id and db["name"] == "files-proof"
                for db in databases), "the Files database did not expose its own ID")
    selected = ["--data-dir", owner.home, "--db", publication_id]
    keys = command(cli, *selected, "get-keys", catalog, "--json")
    require(keys.returncode == 0, "selected database key list failed: " + keys.stderr)
    require([key["hash"] for key in json.loads(keys.stdout)["keys"]] == ["files-v1"],
            "wrong database catalog key")
    read = command(cli, *selected, "get", catalog, "--key-hash", "files-v1", "--json")
    require(read.returncode == 0, "selected database read failed: " + read.stderr)
    require(len(json.loads(read.stdout)["results"]) == 1, "selected database lost the file catalog")
    request = owner.root / "ordinary-create.json"
    request.write_text(json.dumps({
        "type": "mutation", "schema": "lastdb-files/FileChunk",
        "key_value": {"hash": "local-operation", "range": "0000"},
        "alternates": {}, "record": {"data": "ordinary write"},
        "mutation_type": "create", "expected": {"type": "absent"},
    }))
    written = command(cli, *selected, "mutate", "--request-file", request)
    require(written.returncode == 0 and "record_revision" in json.loads(written.stdout),
            "ordinary publisher mutation failed: " + written.stderr)
    revision = json.loads(written.stdout)["record_revision"]
    print("Owner direct create committed:", revision, flush=True)
    read = command(cli, *selected, "get", "lastdb-files/FileChunk", "--key-hash",
                   "local-operation", "--key-range", "0000", "--json")
    require(read.returncode == 0 and json.loads(read.stdout)["results"][0]["fields"]["data"] == "ordinary write",
            "ordinary publisher read did not observe the committed record")
    request.write_text(json.dumps({
        "type": "mutation", "schema": "lastdb-files/FileChunk",
        "key_value": {"hash": "local-operation", "range": "0000"},
        "alternates": {}, "record": {"data": "ordinary rewrite"},
        "mutation_type": "replace",
        "expected": {"type": "revision", "revision": revision},
    }))
    written = command(cli, *selected, "mutate", "--request-file", request)
    require(written.returncode == 0, "ordinary publisher replace failed: " + written.stderr)
    revision = json.loads(written.stdout)["record_revision"]
    print("Owner direct replace committed:", revision, flush=True)
    read = command(cli, *selected, "get", "lastdb-files/FileChunk", "--key-hash",
                   "local-operation", "--key-range", "0000", "--json")
    require(read.returncode == 0 and json.loads(read.stdout)["results"][0]["fields"]["data"] == "ordinary rewrite",
            "ordinary publisher replace did not replace the record")
    request.write_text(json.dumps({
        "type": "mutation", "schema": "lastdb-files/FileChunk",
        "key_value": {"hash": "local-operation", "range": "0000"},
        "alternates": {}, "record": {},
        "mutation_type": "delete",
        "expected": {"type": "revision", "revision": revision},
    }))
    written = command(cli, *selected, "mutate", "--request-file", request)
    require(written.returncode == 0, "ordinary publisher delete failed: " + written.stderr)
    print("Owner direct delete committed:", json.loads(written.stdout)["record_revision"], flush=True)
    read = command(cli, *selected, "get", "lastdb-files/FileChunk", "--key-hash",
                   "local-operation", "--key-range", "0000", "--json")
    require(read.returncode == 0 and json.loads(read.stdout)["results"] == [],
            "ordinary publisher delete left the record in place: " + read.stdout)
    written = command(cli, *selected, "mutate", "--request-file", request)
    require(written.returncode != 0, "a repeated owner delete must conflict")
    print("Owner repeated delete conflicted:", written.stderr.strip(), flush=True)

    original = command(cli, "--db", "personal", "get", "RecordById", "--key-hash", "one", "--json",
                       node=owner)
    require(original.returncode == 0 and json.loads(original.stdout)["results"][0]["fields"]["value"] == "v3",
            "explicit personal selection did not read the original record: " + original.stderr)
    wrong = command(cli, "--db", missing_id, "get", "RecordById", "--key-hash", "one", "--json",
                    node=owner)
    require(wrong.returncode == 1 and "database is not registered" in wrong.stderr,
            "an unknown database selector must never read the node's personal records: " + wrong.stderr)
    wrong = command(cli, "--db", missing_id, "mutate", "--request-file", request, node=owner)
    require(wrong.returncode == 1 and "database is not registered" in wrong.stderr,
            "an unknown database selector must refuse the mutation before dispatch: " + wrong.stderr)


def check_files(command, cli, owner, publication_id, other_id, source, manifest):
    home = ["--data-dir", owner.home]
    selected = [*home, "--db", publication_id, "share", "files"]
    listed = command(cli, *selected, "list")
    require(listed.returncode == 0, "selected Files list failed: " + listed.stderr)
    listing = json.loads(listed.stdout)
    require(listing["database"] == publication_id and listing["files"] == [manifest],
            "selected Files list returned the wrong database or manifest")

    output = owner.root / "saved-file.txt"
    saved = command(cli, *selected, "save", "--file", manifest["id"], "--out", output)
    require(saved.returncode == 0, "selected Files save failed: " + saved.stderr)
    receipt = json.loads(saved.stdout)
    require(receipt["database"] == publication_id and receipt["path"] == str(output),
            "selected Files save returned the wrong database or path")
    require(output.read_bytes() == source.read_bytes(), "selected Files save changed the file bytes")

    refused = command(cli, *home, "--db", other_id, "share", "files", "list")
    require(refused.returncode == 1 and "database is not registered" in refused.stderr,
            "an unknown database selector must not fall back to the local Files catalog: "
            + refused.stderr)


def exercise(cli, owner):
    def command(binary, *arguments, node=None):
        environment = None if node is None else {
            **os.environ, "LASTDB_HOME": str(node.home), "LASTDB_SOCKET_PATH": str(node.socket),
        }
        return subprocess.run([binary, *arguments], capture_output=True, text=True, timeout=60,
                              env=environment)

    daemon_help = command(owner.binary, "--help")
    require(daemon_help.returncode == 0 and "--share-owner-home" not in daemon_help.stdout,
            "the standalone publication flag is still available")
    require({"accept", "contributors"} <= subcommands(command(cli, "share", "--help")),
            "the Share invitation and contributor commands are missing")
    files_commands = subcommands(command(cli, "share", "files", "--help"))
    require({"add", "receive", "list", "save"} <= files_commands and "follow" not in files_commands,
            "the Files CLI does not expose only the current commands")
    for retired in ["create", "get", "publish"]:
        result = command(cli, "share", retired, "--help")
        require(result.returncode == 2 and "unrecognized subcommand" in result.stderr,
                f"the retired share {retired} command should be refused")

    home = ["--data-dir", owner.home]
    made = command(cli, *home, "db", "create", "files-proof", "--spec-name", "lastdb-files",
                   "--spec-version", "1", "--json")
    require(made.returncode == 0, "Files database create failed: " + made.stderr)
    publication_id = json.loads(made.stdout)["id"]
    missing_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    payload = bytes(range(251)) * 2300 + b"LastDB Files product operation\n"
    path = owner.root / "file.txt"
    path.write_bytes(payload)
    added = command(cli, *home, "--db", "files-proof", "share", "files", "add", path)
    require(added.returncode == 0, "share files add failed: " + added.stderr)
    receipt = json.loads(added.stdout)
    require(receipt["name"] == path.name and receipt["size_bytes"] == len(payload)
            and receipt["chunks"] > 16
            and receipt["sha256"] == hashlib.sha256(payload).hexdigest(),
            "share files add returned different file metadata")
    check_addressing(command, cli, owner, publication_id, missing_id)
    check_files(command, cli, owner, publication_id, missing_id, path, receipt)
    print("LastDB Files: named database add, list, save, keyed addressing, and retired commands",
          flush=True)
