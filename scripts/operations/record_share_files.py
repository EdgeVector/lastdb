"""Local LastDB Share setup, write modes, and Files import on generated CI homes."""

import hashlib
import json
import subprocess

from node import Node


def require(value, message):
    if not value:
        raise RuntimeError(message)


def subcommands(result):
    require(result.returncode == 0, "Share help failed: " + result.stderr)
    return {line.split()[0] for line in result.stdout.splitlines() if line.strip()}


def exercise(cli, owner, root):
    def command(binary, *arguments):
        return subprocess.run([binary, *arguments], capture_output=True, text=True, timeout=60)

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
    require({"get", "contributors"} <= subcommands(command(cli, "share", "--help")),
            "the Share record and contributor commands are missing")
    require({"grant", "revoke", "submit"} <= subcommands(command(cli, "share", "contributors", "--help")),
            "the Share contributor command group is incomplete")
    require({"add", "receive", "follow", "list", "save"} <= subcommands(command(cli, "share", "files", "--help")),
            "the Files CLI command group is incomplete")
    stopped_publish = command(cli, "share", "publish", "--help")
    require(stopped_publish.returncode == 2 and "unrecognized subcommand" in stopped_publish.stderr,
            "the retired stopped Share command should be refused")

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
        payload = b"LastDB Files product operation\n"
        path = publication.root / "file.txt"
        path.write_bytes(payload)
        added = command(cli, "--data-dir", publication.home, "share", "files", "add", path)
        require(added.returncode == 0, "share files add failed: " + added.stderr)
        receipt = json.loads(added.stdout)
        require(receipt["name"] == path.name and receipt["size_bytes"] == len(payload)
                and receipt["sha256"] == hashlib.sha256(payload).hexdigest(),
                "share files add returned different file metadata")
    finally:
        publication.close()
    print("LastDB Share: local write modes, contributor policy refusals, and Files import passed", flush=True)
