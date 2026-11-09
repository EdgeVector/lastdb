"""Local LastDB Files setup and import on generated CI homes."""

import hashlib
import json
import subprocess

from node import Node


def require(value, message):
    if not value:
        raise RuntimeError(message)


def exercise(cli, owner, root):
    def command(binary, *arguments):
        return subprocess.run([binary, *arguments], capture_output=True, text=True, timeout=60)

    daemon_help = command(owner.binary, "--help")
    require(daemon_help.returncode == 0 and "--share-owner-home" in daemon_help.stdout,
            "the automatic Share publisher flag is missing")
    files_help = command(cli, "share", "files", "--help")
    commands = {line.split()[0] for line in files_help.stdout.splitlines() if line.strip()}
    require(files_help.returncode == 0 and {"add", "receive", "follow", "list", "save"} <= commands,
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
        created = command(cli, "--data-dir", owner.home, "share", "create",
                          "--into", publication.home, "--env", "dev")
        require(created.returncode == 0, "share create failed: " + created.stderr)
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
    print("LastDB Files: local publication and file import passed; stopped publish is unavailable",
          flush=True)
