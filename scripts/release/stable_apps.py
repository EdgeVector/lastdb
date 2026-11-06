"""Prove real app operations and prepare declarations without publisher secrets."""

import json
import os
from pathlib import Path
import subprocess

from nightly_package import write_json
from stable_plan import APPS


def command(argv, environment, directory, output, name, body=None, timeout=300, retain_output=True,
            accept_exit=(0,)):
    with (output / (name + ".log")).open("w") as log:
        result = subprocess.run(argv, cwd=directory, env=environment, input=body, text=True,
                                stdout=subprocess.PIPE, stderr=log, timeout=timeout)
        if result.returncode not in accept_exit or len(result.stdout) > 1024 ** 2:
            raise RuntimeError(f"App operation {name} failed; see its CI artifact")
        if retain_output:
            log.write(result.stdout)
    return result.stdout


def source_for(node, plan, app, output, environment, cli):
    prefix = [str(cli), "--data-dir", str(node.home), "app"]
    help_result = subprocess.run([*prefix, "list", "--help"], env=environment,
                                 capture_output=True, text=True, timeout=10)
    resolver = "list" if help_result.returncode == 0 and "[APP]" in help_result.stdout else "resolve"
    resolved = subprocess.run([*prefix, resolver, app, "--channel", "stable", "--json"],
                              env=environment, capture_output=True, text=True, timeout=60)
    if resolved.returncode == 0 and json.loads(resolved.stdout).get("sha") == plan["apps"][app]:
        command([*prefix, "install", app, "--channel", "stable", "--accept-install"],
                environment, output, output, app + "-registry-install", timeout=600)
        active = Path(environment["LASTDB_APPS_ROOT"]) / app / "current"
        receipt = json.loads((active / "lastdb-app-install.json").read_text())
        if receipt["sha"] != plan["apps"][app] or receipt.get("inactive") is not False:
            raise ValueError("The activated registry app differs from the frozen source")
        write_json(output / (app + ".install-receipt.json"), receipt)
        return active / "source", True
    # Missing declarations can produce bootstrap evidence, never a stable
    # qualification. A publishing run refuses this mode before the hour starts.
    source = output / app
    command(["git", "clone", "--no-checkout", "https://github.com/EdgeVector/" + app + ".git",
             str(source)], environment, output, output, app + "-clone")
    command(["git", "checkout", "--detach", plan["apps"][app]], environment, source,
            output, app + "-checkout")
    manifest = json.loads((source / "lastdb-app.json").read_text())
    for phase in ("prepare", "probe"):
        for index, step in enumerate(manifest["install"][phase]):
            command(step["argv"], environment, source, output, f"{app}-{phase}-{index}",
                    timeout=min(step.get("timeout_s", 300), 600),
                    accept_exit=tuple(step.get("accept_exit", [0])))
    return source, False


def prove(node, plan, output, prepare_cli=None):
    observed = node.call("/api/version")
    environment = {**os.environ, **node.environment,
                   "PATH": os.pathsep.join((str(node.binaries), str(node.root / "os-home/.local/bin"),
                                            os.environ["PATH"])),
                   "LASTDB_APPS_ROOT": str(node.root / "managed-apps"),
                   "FBRAIN_FOLDDB_SOCKET": str(node.socket),
                   "LASTSECRETS_SOCKET_PATH": str(node.socket),
                   "LASTDB_SOCKET_PATH": str(node.socket)}
    for name in ("GH_TOKEN", "GITHUB_TOKEN", "ACTIONS_ID_TOKEN_REQUEST_TOKEN", "ACTIONS_RUNTIME_TOKEN"):
        environment.pop(name, None)
    results = {}
    for app in APPS:
        source, installed = source_for(node, plan, app, output, environment,
                                       prepare_cli or node.binaries / "lastdb")
        manifest = json.loads((source / "lastdb-app.json").read_text())
        if manifest["app_id"] != app:
            raise ValueError("App source identity differs")
        link = next(item for item in manifest["install"]["links"] if item["command"] == app)
        executable = Path(environment["HOME"]) / ".local/bin" / app if installed else source / link["source"]
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
            if (drained.get("ok") is not True or not any(
                    hit.get("schema_name") == "brain/Concept" and hit.get("key_hash") == "ci-release-qualification"
                    and "CI stable qualification record" in hit.get("text", "") for hit in hits.get("hits", []))):
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
                        "outbox_format_version": observed["current_outbox_format_version"],
                        "installed_from_registry": installed,
                        "declaration": declaration, "complete": True}
    write_json(output / "apps.json", results)
    return results
