"""Local LastDB Share setup, write modes, and Files operations on generated CI homes."""

import hashlib
import json
import os
import subprocess

from node import Node


def require(value, message):
    if not value:
        raise RuntimeError(message)


def subcommands(result):
    require(result.returncode == 0, "Share help failed: " + result.stderr)
    return {line.split()[0] for line in result.stdout.splitlines() if line.strip()}


def check_addressing(command, cli, owner, publication, publication_id):
    catalog = "lastdb-files/FileCatalog"
    listed = command(cli, "--data-dir", publication.home, "db", "list", "--json")
    require(listed.returncode == 0, "database list failed: " + listed.stderr)
    databases = json.loads(listed.stdout)["databases"]
    require(len(databases) == 1 and databases[0]["id"] == publication_id,
            "the publication did not expose its own database ID")
    selected = ["--data-dir", publication.home, "--db", publication_id]
    keys = command(cli, *selected, "get-keys", catalog, "--json")
    require(keys.returncode == 0, "selected database key list failed: " + keys.stderr)
    require([key["hash"] for key in json.loads(keys.stdout)["keys"]] == ["files-v1"],
            "wrong database catalog key")
    read = command(cli, *selected, "get", catalog, "--key-hash", "files-v1", "--json")
    require(read.returncode == 0, "selected database read failed: " + read.stderr)
    require(len(json.loads(read.stdout)["results"]) == 1, "selected database lost the file catalog")
    request = publication.root / "ordinary-create.json"
    request.write_text(json.dumps({
        "type": "mutation", "schema": "lastdb-files/FileChunk",
        "key_value": {"hash": "local-operation", "range": "0000"},
        "alternates": {}, "record": {"data": "ordinary write"},
        "mutation_type": "create", "expected": {"type": "absent"},
    }))
    written = command(cli, *selected, "mutate", "--request-file", request)
    require(written.returncode == 0 and "record_revision" in json.loads(written.stdout),
            "ordinary publisher mutation failed: " + written.stderr)
    read = command(cli, *selected, "get", "lastdb-files/FileChunk", "--key-hash",
                   "local-operation", "--key-range", "0000", "--json")
    require(read.returncode == 0 and json.loads(read.stdout)["results"][0]["fields"]["data"] == "ordinary write",
            "ordinary publisher read did not observe the committed record")

    original = command(cli, "--db", "personal", "get", "RecordById", "--key-hash", "one", "--json",
                       node=owner)
    require(original.returncode == 0 and json.loads(original.stdout)["results"][0]["fields"]["value"] == "v3",
            "explicit personal selection did not read the original record: " + original.stderr)
    wrong = command(cli, "--db", publication_id, "get", "RecordById", "--key-hash", "one", "--json",
                    node=owner)
    require(wrong.returncode == 1 and "database is not registered" in wrong.stderr,
            "an unknown database selector must never read the node's personal records: " + wrong.stderr)
    wrong = command(cli, "--db", publication_id, "mutate", "--request-file", request, node=owner)
    require(wrong.returncode == 1 and "database is not registered" in wrong.stderr,
            "an unknown database selector must refuse the mutation before dispatch: " + wrong.stderr)


def check_files(command, cli, publication, publication_id, other_id, source, manifest):
    home = ["--data-dir", publication.home]
    selected = [*home, "--db", publication_id, "share", "files"]
    listed = command(cli, *selected, "list")
    require(listed.returncode == 0, "selected Files list failed: " + listed.stderr)
    listing = json.loads(listed.stdout)
    require(listing["database"] == publication_id and listing["files"] == [manifest],
            "selected Files list returned the wrong database or manifest")

    output = publication.root / "saved-file.txt"
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


def exercise(cli, owner, root):
    def command(binary, *arguments, node=None):
        environment = None if node is None else {
            **os.environ, "LASTDB_HOME": str(node.home), "LASTDB_SOCKET_PATH": str(node.socket),
        }
        return subprocess.run([binary, *arguments], capture_output=True, text=True, timeout=60,
                              env=environment)

    def create(home, *options):
        result = command(cli, "--data-dir", owner.home, "share", "create",
                         "--into", home, "--env", "dev", *options)
        require(result.returncode == 0, "share create failed: " + result.stderr)
        return json.loads(result.stdout)

    def revoke(home, publisher_key):
        return command(cli, "--data-dir", owner.home, "share", "contributors", "revoke",
                       "--home", home, "--recipient-key", publisher_key, "--expected-version", "1")

    daemon_help = command(owner.binary, "--help")
    require(daemon_help.returncode == 0 and "--share-owner-home" in daemon_help.stdout,
            "the automatic Share publisher flag is missing")
    require({"accept", "contributors"} <= subcommands(command(cli, "share", "--help")),
            "the Share invitation and contributor commands are missing")
    require({"grant", "revoke", "accept"} <= subcommands(command(cli, "share", "contributors", "--help")),
            "the Share contributor command group is incomplete")
    require({"add", "receive", "follow", "list", "save"} <= subcommands(command(cli, "share", "files", "--help")),
            "the Files CLI command group is incomplete")
    for retired in ["get", "publish"]:
        result = command(cli, "share", retired, "--help")
        require(result.returncode == 2 and "unrecognized subcommand" in result.stderr,
                f"the retired share {retired} command should be refused")

    evidence = owner.output / "share"
    evidence.mkdir(mode=0o700)
    publication = Node(owner.binary, owner.revision, evidence, temporary_directory=root,
                       environment=owner.environment)
    try:
        publication.home.rmdir()
        publication.socket = publication.home / "data" / "folddb.sock"
        files = create(publication.home)
        require(files["write_mode"] == "publisher_only", "Files must default to publisher-only")
        contributor_home = publication.root / "contributor-publication"
        contributors = create(contributor_home, "--write-mode", "publisher-validated")
        publisher_key = files["publisher_public_key"]
        require(contributors["write_mode"] == "publisher_validated"
                and contributors["publication_id"] != files["publication_id"]
                and contributors["publisher_public_key"] == publisher_key,
                "the explicit contributor publication has the wrong write mode or identity")
        refused = revoke(publication.home, publisher_key)
        require(refused.returncode == 1 and "share does not allow contributor submissions" in refused.stderr,
                "the publisher-only mode must refuse contributor grant changes")
        ungranted = revoke(contributor_home, publisher_key)
        require(ungranted.returncode == 1 and "contributor grant version conflict" in ungranted.stderr,
                "the contributor mode must open its local policy and refuse an absent grant")
        publication.start()
        # The pattern shifts at each 32 KiB chunk boundary and includes a short tail.
        payload = bytes(range(251)) * 2100 + b"LastDB Files product operation\n"
        path = publication.root / "file.txt"
        path.write_bytes(payload)
        added = command(cli, "--data-dir", publication.home, "share", "files", "add", path)
        require(added.returncode == 0, "share files add failed: " + added.stderr)
        receipt = json.loads(added.stdout)
        require(receipt["name"] == path.name and receipt["size_bytes"] == len(payload)
                and receipt["chunks"] > 16
                and receipt["sha256"] == hashlib.sha256(payload).hexdigest(),
                "share files add returned different file metadata")
        check_addressing(command, cli, owner, publication, files["publication_id"])
        check_files(command, cli, publication, files["publication_id"],
                    contributors["publication_id"], path, receipt)
    finally:
        publication.close()
    print("LastDB Share: write modes, multi-batch Files save, database addressing, and retired commands passed",
          flush=True)
