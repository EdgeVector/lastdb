#!/usr/bin/env python3
"""Rehearse the direct primary-format conversion on owned generated CI homes."""

import base64
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "scripts" / group) for group in ("benchmarks", "release")]
from nightly_probe import unpack
from node import Node


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class PackagedNode(Node):
    def start(self):
        try:
            super().start()
        except RuntimeError as error:
            version = self.output / "version.json"
            if ("differs from the selected revision" not in str(error) or not version.exists()
                    or f".g{self.revision[:9]}" not in version.read_text()):
                raise


class Apps:
    def __init__(self, runner, scratch, side, node):
        self.node = node
        self.brain_source = runner / f"{side}-brain"
        self.secrets_source = runner / f"{side}-secrets"
        self.brain_config = scratch / f"{side}-brain.json"
        self.secrets_config = scratch / f"{side}-secrets.json"
        self.environment = dict(os.environ, BRAIN_CONFIG=str(self.brain_config),
                                BRAIN_FOLDDB_SOCKET=str(node.socket),
                                LASTSECRETS_CONFIG=str(self.secrets_config),
                                LASTDB_SUPPRESS_RECOVERY_PHRASE="1")

    def command(self, app, args, payload=None):
        directory = self.brain_source if app == "brain" else self.secrets_source
        completed = subprocess.run(["bun", "src/cli.ts", *args], cwd=directory, env=self.environment,
                                   input=payload or b"", capture_output=True, timeout=120)
        if completed.returncode != 0:
            if app == "brain" or args[0] == "init":
                print(completed.stderr.decode(errors="replace")[-2000:], file=sys.stderr)
            raise RuntimeError(f"Generated {app} {args[0]} failed with exit {completed.returncode}")
        return completed.stdout

    def brain(self, *args, payload=None):
        return self.command("brain", args, payload)

    def lastsecrets(self, *args, payload=None):
        return self.command("lastsecrets", args, payload)

    def initialize(self):
        self.brain("init", "--node-url", "http://127.0.0.1")
        self.lastsecrets("init", "--config", str(self.secrets_config), "--socket", str(self.node.socket))

    def put_secret(self, value):
        self.lastsecrets("put", "generated-format-move", "--label", "Migration proof",
                         "--provider", "generated", "--purpose", "migration rehearsal",
                         "--env", "dev", "--value-stdin", payload=value)


def migrate(runner, evidence, source, target, mode):
    completed = subprocess.run([sys.executable, str(runner / "migration-tool/bin/last-stack-lastdb-home-copy"),
        "--source-socket", str(source.node.socket), "--target-socket", str(target.node.socket),
        "--old-brain-config", str(source.brain_config), "--new-brain-config", str(target.brain_config),
        "--old-lastsecrets-config", str(source.secrets_config), "--new-lastsecrets-config", str(target.secrets_config),
        "--brain-release", str(target.brain_source), "--lastsecrets-release", str(target.secrets_source),
        "--allow-different-identity", "--json", f"--{mode}"], capture_output=True, timeout=240)
    if completed.stdout:
        # The converter redacts secret values, keys, and digests from its report.
        (evidence / f"{mode}.json").write_text(json.dumps(json.loads(completed.stdout), indent=2) + "\n")
    require(completed.returncode == 0, f"The format converter refused {mode}; inspect the redacted report")


def seed_source_attachment(source, attachment):
    # The installed format 9 Brain does not alias the legacy put-blob fields.
    # Seed the file through the source's native API, then let the real CLI
    # recognize that content hash and write the attachment index normally.
    config = json.loads(source.brain_config.read_text())
    schema = config["schemaHashes"]["__attachmentfile__"]
    aliases = config["schemaFieldAliases"][schema]
    data = attachment.read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    fields = {"content_hash": sha, "size": str(len(data)), "media_type": "application/octet-stream",
              "created_at": datetime.now(timezone.utc).isoformat()}
    response = source.node.call("/api/db/put-blob", {"schema": schema, "key": {"hash": sha},
        "bytes_b64": base64.b64encode(data).decode(), "name": attachment.name,
        "media_type": "application/octet-stream", "fields": {aliases[key]: value for key, value in fields.items()}})[0]
    require(response.get("file", {}).get("sha256") == sha, "The source did not confirm the attachment bytes")
    source.brain("attach", "generated-format-move", str(attachment), "--type", "concept")


def verify_apps(apps, attachment, destination, secret):
    content = apps.brain("get", "generated-format-move", "--type", "concept", "--field", "body")
    require(b"generated format migration body" in content, "Brain lost the seeded record")
    apps.brain("attachment", "get", "generated-format-move", attachment.name,
               "--type", "concept", "-o", str(destination))
    require(attachment.read_bytes() == destination.read_bytes(), "Brain attachment bytes differ")
    require(apps.lastsecrets("get", "generated-format-move").rstrip(b"\n") == secret,
            "LastSecrets returned a different generated value")
    listed = apps.lastsecrets("list")
    require(b"generated-format-move" in listed and b"value=<redacted>" in listed and secret not in listed,
            "LastSecrets did not return redacted metadata")


def main():
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "Hosted CI only")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve()
    evidence = runner / "primary-format-results"
    evidence.mkdir(mode=0o700)
    settings = {"LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "LASTDB_SELF_METRICS_TO_DB": "false"}
    with ExitStack() as cleanup:
        scratch = Path(cleanup.enter_context(tempfile.TemporaryDirectory(prefix="format-move-", dir=runner)))
        bundle_dir = runner / "format14-bundle"
        bundle = bundle_dir / "lastdb-x86_64-unknown-linux-gnu.tar.gz"
        manifest = json.loads((bundle_dir / "lastdb-x86_64-unknown-linux-gnu.manifest.json").read_text())
        programs = scratch / "candidate"
        programs.mkdir()
        unpack(bundle, manifest, programs, manifest["version"], os.environ["TARGET_DB"])
        nodes = []
        for side, node_type, binary, revision, disk, api in (
            ("source", Node, runner / "format9-build/release/lastdbd", "SOURCE_DB", 9, 23),
            ("target", PackagedNode, programs / "lastdbd", "TARGET_DB", 14, 30),
        ):
            output = evidence / side
            output.mkdir()
            node = node_type(binary, os.environ[revision], output, temporary_directory=runner, environment=settings)
            cleanup.callback(node.close)
            node.start()
            require(node.home_format() == disk and node.api_version == api, f"Unexpected {side} home format or API")
            nodes.append(Apps(runner, scratch, side, node))
        source, target = nodes
        source.initialize()
        target.initialize()
        attachment = scratch / "generated-attachment.bin"
        attachment.write_bytes(secrets.token_bytes(4096))
        secret = secrets.token_hex(48).encode()
        source.brain("put", "generated-format-move", "--type", "concept",
                     payload=b"generated format migration body\n")
        seed_source_attachment(source, attachment)
        source.put_secret(secret)
        verify_apps(source, attachment, scratch / "source-attachment", secret)
        migrate(runner, evidence, source, target, "apply")
        target.node.stop()
        target.node.start()
        migrate(runner, evidence, source, target, "verify")
        verify_apps(target, attachment, scratch / "target-attachment", secret)
        new_body = b"write after cutover\n" * 2400
        target.brain("put", "generated-after-move", "--type", "concept", payload=new_body)
        require(target.brain("get", "generated-after-move", "--type", "concept", "--field", "body").rstrip()
                == new_body.rstrip(), "Brain cannot write and reopen a long body on the target")
        target.put_secret(secret + b"-updated")
        require(target.lastsecrets("get", "generated-format-move").rstrip(b"\n") == secret + b"-updated",
                "LastSecrets cannot update on the target")
        target.lastsecrets("delete", "generated-format-move")
        require(b"generated-format-move" not in target.lastsecrets("list"), "LastSecrets cannot delete on the target")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write("Moved generated Brain and LastSecrets records directly from format 9 to 14. "
                      "The converter and both apps verified values and file bytes after reopen.\n")


if __name__ == "__main__":
    main()
