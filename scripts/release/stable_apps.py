"""Prove real app operations and prepare declarations without publisher secrets."""

import json
import os
from pathlib import Path
import subprocess

from nightly_package import write_json
from stable_plan import APPS


def command(argv, environment, directory, output, name, body=None, timeout=300, retain_output=True):
    with (output / (name + ".log")).open("w") as log:
        result = subprocess.run(argv, cwd=directory, env=environment, input=body, text=True,
                                stdout=subprocess.PIPE, stderr=log, timeout=timeout)
        if result.returncode or len(result.stdout) > 1024 ** 2:
            raise RuntimeError(f"App operation {name} failed; see its CI artifact")
        if retain_output:
            log.write(result.stdout)
    return result.stdout


def prove(node, plan, output, prepare_cli=None):
    environment = {**os.environ, **node.environment,
                   "PATH": str(node.binaries) + os.pathsep + os.environ["PATH"],
                   "FBRAIN_FOLDDB_SOCKET": str(node.socket),
                   "LASTSECRETS_SOCKET_PATH": str(node.socket),
                   "LASTDB_SOCKET_PATH": str(node.socket)}
    results = {}
    for app in APPS:
        source = output / app
        command(["git", "clone", "--no-checkout", "https://github.com/EdgeVector/" + app + ".git",
                 str(source)], environment, output, output, app + "-clone")
        command(["git", "checkout", "--detach", plan["apps"][app]], environment, source,
                output, app + "-checkout")
        manifest = json.loads((source / "lastdb-app.json").read_text())
        if manifest["app_id"] != app:
            raise ValueError("App source identity differs")
        # The reviewed manifest supplies bounded install commands, never an
        # unverified registry fallback or a fabricated compatibility version.
        for index, step in enumerate(manifest["install"]["prepare"]):
            command(step["argv"], environment, source, output, f"{app}-prepare-{index}",
                    timeout=min(step.get("timeout_s", 300), 600))
        executable = source / ("bin/search" if app == "search" else
                               "bin/brain" if app == "brain" else "bin/registry-lastsecrets")
        executable.chmod(executable.stat().st_mode | 0o100)
        run = lambda args, name, body=None, retain_output=True: command(
            [str(executable), *args], environment, source, output, app + "-" + name, body,
            retain_output=retain_output)
        if app == "brain":
            run(["init"], "init")
            value = "CI stable qualification record"
            run(["put", "ci-release-qualification", "--type", "concept"], "put", value)
            if value not in run(["get", "ci-release-qualification", "--type", "concept"], "get"):
                raise RuntimeError("Brain did not return the acknowledged record")
        elif app == "lastsecrets":
            run(["init"], "init")
            value = "generated-nonsecret-ci-value"
            run(["put", "ci-release-qualification", "--label", "CI qualification", "--provider", "ci",
                 "--purpose", "release", "--env", "dev", "--value-stdin"], "put", value)
            if run(["get", "ci-release-qualification"], "get", retain_output=False).strip() != value:
                raise RuntimeError("LastSecrets did not return the acknowledged value")
        else:
            # Search owns no schemas. Exercise its real CLI status and inbox
            # drain. Model downloads and the generated index stay on the runner.
            inbox = node.home / "apps/search/inbox"
            if not inbox.is_dir() or not any(path.suffix == ".json" for path in inbox.iterdir()):
                raise RuntimeError("Database writes produced no real Search delivery")
            run(["init", "--last-db-home", str(node.home)], "init")
            drained = json.loads(run(["drain", "--last-db-home", str(node.home)], "drain"))
            hits = json.loads(run(["query", "CI stable qualification record", "--json",
                                  "--last-db-home", str(node.home)], "query"))
            if drained.get("ok") is not True or not hits.get("hits"):
                raise RuntimeError("Search did not query the real database changes")
            run(["status", "--last-db-home", str(node.home)], "status")
        statement = output / (app + ".declaration.json")
        command([str(prepare_cli or node.binaries / "lastdb"), "--data-dir", str(node.home), "app", "publish",
                 "--manifest", str(source / "lastdb-app.json"), "--channel", "stable",
                 "--prepare", str(statement)], environment, source, output, app + "-declaration")
        declaration = json.loads(statement.read_text())
        if declaration["sha"] != plan["apps"][app]:
            raise ValueError("The validated app commit differs from the plan")
        results[app] = {"sha": declaration["sha"], "api_version": declaration["api_version"],
                        "declaration": declaration, "complete": True}
    write_json(output / "apps.json", results)
    return results
