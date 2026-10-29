#!/usr/bin/env python3
"""Index bounded CI output, then publish artifact facts to the CI summary.

Run --stage index before upload-artifact and --stage summary after upload.
The helper reports the supplied product/job status. Its own success is not
product proof. It does not read credentials or request headers, or dump the environment.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from urllib.parse import quote, urlsplit


MAX_DEPTH = 5
MAX_FILES = 1000
MAX_ENTRIES = 2000
MAX_BYTES = 100 * 1024 * 1024
MAX_JSON_BYTES = 5 * 1024 * 1024
INDEX = "evidence-index.json"
SUMS = "SHA256SUMS"
STATUSES = {"success", "failure", "cancelled", "skipped", "unknown", "timed_out"}
CARGO_RESULT = re.compile(
    r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", re.MULTILINE
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def owned_directory(argument):
    require(os.environ.get("GITHUB_ACTIONS") == "true", "GitHub Actions is required")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    requested = Path(argument)
    require(requested.is_absolute(), "the evidence directory must be absolute")
    require(not requested.is_symlink(), "the evidence directory cannot be a symbolic link")
    resolved = requested.resolve(strict=False)
    require(resolved != runner and resolved.is_relative_to(runner),
            "the evidence directory must be a strict descendant of RUNNER_TEMP")
    resolved.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(resolved.stat().st_uid == os.geteuid(), "the evidence directory has another owner")
    return resolved


def inventory(root):
    """Walk only this explicit result directory, with finite depth and size."""
    pending, files, entries, total = [(root, 0)], [], 0, 0
    while pending:
        directory, depth = pending.pop()
        with os.scandir(directory) as children:
            for child in children:
                entries += 1
                require(entries <= MAX_ENTRIES, "evidence exceeds the entry-count bound")
                info = child.stat(follow_symlinks=False)
                require(info.st_uid == os.geteuid(), "an evidence object has another owner")
                if stat.S_ISDIR(info.st_mode):
                    require(depth < MAX_DEPTH, "evidence exceeds the directory-depth bound")
                    pending.append((Path(child.path), depth + 1))
                    continue
                require(stat.S_ISREG(info.st_mode), "evidence contains a link or a nonregular file")
                path = Path(child.path)
                relative = path.relative_to(root).as_posix()
                require(not any(character in relative for character in "\n\r\\"),
                        "an evidence filename cannot enter a checksum file safely")
                require(path.suffix.lower() != ".onnx" and path.name not in {
                    "tokenizer.json", "special_tokens_map.json", "tokenizer_config.json"
                }, "model files must remain in the separate model cache")
                if relative == SUMS:
                    continue
                total += info.st_size
                files.append((relative, path, info.st_size))
                require(len(files) <= MAX_FILES, "evidence exceeds the file-count bound")
                require(total <= MAX_BYTES, "evidence exceeds the 100 MiB total-size bound")
    return sorted(files), total


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(64 * 1024):
            digest.update(block)
    return digest.hexdigest()


def source_revision():
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2],
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15,
    )
    revision = result.stdout.strip()
    require(re.fullmatch(r"[0-9a-f]{40,64}", revision), "git returned an invalid source revision")
    return revision


def json_file(path):
    require(path.is_file() and not path.is_symlink(), "the requested evidence file is absent or unsafe")
    require(path.stat().st_size <= MAX_JSON_BYTES, "a result report exceeds the JSON read bound")
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    require(isinstance(value, dict), "a result report must contain a JSON object")
    return value


def write_owned(path, text):
    if path.exists() or path.is_symlink():
        require(not path.is_symlink() and path.stat().st_uid == os.geteuid(),
                "refuse to replace an unsafe evidence metadata file")
    with path.open("w", encoding="utf-8") as stream:
        stream.write(text)


def index_output(args, root):
    inventory(root)
    now = datetime.now(timezone.utc)
    metadata = {
        "format_version": 1, "label": args.label, "source_revision": source_revision(),
        "product_job_status": args.job_status, "product_exit_code": args.exit_code,
        "indexed_at": now.isoformat(), "retention_days": 7,
        "expected_expiry": (now + timedelta(days=7)).isoformat(),
        "limits": {"depth": MAX_DEPTH, "files": MAX_FILES, "total_bytes": MAX_BYTES},
        "meaning": "The supplied product/job status is independent of this evidence helper.",
    }
    write_owned(root / INDEX, json.dumps(metadata, indent=2) + "\n")
    files, total = inventory(root)
    write_owned(root / SUMS, "".join(f"{checksum(path)}  {name}\n" for name, path, _ in files))
    print(f"Indexed {len(files)} files, {total} bytes; product/job status: {args.job_status}")


def case_counts(report):
    cases = report.get("cases", [])
    require(isinstance(cases, list), "the result case collection must be a list")
    require(len(cases) <= 1000, "the result case collection exceeds its bound")
    passed = sum(isinstance(case, dict)
                 and (case.get("status") == "passed" or case.get("passed") is True)
                 for case in cases)
    return passed, len(cases)


def structured_counts(root, label):
    name = "node-result.json" if label == "node" else "semantic-report.json"
    report_path = root / name
    if not report_path.exists():
        report_path = root / "results" / name
    if label not in {"node", "model"} or not report_path.exists():
        return None
    report = json_file(report_path)
    passed, total = case_counts(report)
    lines = [f"Product cases: **{passed}/{total} passed**."]
    if label == "model":
        lines.append(f"Product report result: **{report.get('passed') is True}**.")
        for key, title in [("false_positives", "False positives"), ("false_negatives", "False negatives")]:
            values = report.get(key, [])
            require(isinstance(values, list), "semantic error counts must be lists")
            lines.append(f"{title}: {len(values)}.")
        failure = report.get("model_failure", {})
        lines.append(f"Missing-model checks: **{failure.get('passed') is True}**.")
    source = report.get("source", report.get("source_commit"))
    if source and source != source_revision():
        lines.append("CAUTION: The product report names a different source revision.")
    return lines


def cargo_counts(root):
    for name in ["results.log", "cargo.log", "protocol.log", "aws.log"]:
        path = root / name
        if not path.is_file() or path.is_symlink():
            continue
        require(path.stat().st_size <= MAX_BYTES, "a Cargo log exceeds its read bound")
        counts = []
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                if match := CARGO_RESULT.match(line):
                    counts.append(tuple(int(value) for value in match.groups()[1:]))
                    require(len(counts) <= 64, "a Cargo log contains too many result summaries")
        if counts:
            passed, failed, ignored = (sum(values) for values in zip(*counts))
            return [f"Completed Cargo cases: **{passed} passed; {failed} failed; {ignored} ignored**."]
    return ["No structured product result or completed Cargo case summary is available."]


def artifact_facts(args):
    url = args.artifact_url or ""
    digest = (args.artifact_digest or "").removeprefix("sha256:")
    if url:
        parsed = urlsplit(url)
        require(parsed.scheme == "https" and parsed.netloc and not parsed.username
                and not parsed.password and not parsed.query and not parsed.fragment
                and not any(character.isspace() for character in url), "the artifact URL is unsafe")
        url = quote(url, safe=":/%-._~")
    require(not digest or re.fullmatch(r"[0-9a-fA-F]{64}", digest), "the artifact digest is invalid")
    return url, digest.lower()


def summary_output(args, root):
    metadata = json_file(root / INDEX) if (root / INDEX).exists() else {}
    require(not metadata or metadata.get("label") == args.label, "the evidence index has another label")
    url, digest = artifact_facts(args)
    revision = metadata.get("source_revision", source_revision())
    job_status = metadata.get("product_job_status", args.job_status)
    exit_code = metadata.get("product_exit_code", args.exit_code)
    lines = [f"## Schema evidence: {args.label}", "", f"Source: `{revision}`.",
             f"Supplied product/job status: **{job_status}**."]
    if exit_code is not None:
        lines.append(f"Product command exit code: **{exit_code}**.")
    lines.extend(structured_counts(root, args.label) or cargo_counts(root))
    lines.extend(["", "The evidence helper result does not establish a product pass.",
                  f"Artifact: [Download]({url})." if url else "Artifact download: unavailable.",
                  f"Artifact SHA-256: `{digest}`." if digest else "Artifact SHA-256: unavailable.",
                  "Retention: **7 days**."])
    if metadata.get("expected_expiry"):
        lines.append(f"Expected expiry from the index time: `{metadata['expected_expiry']}`.")
    if (root / SUMS).is_file() and not (root / SUMS).is_symlink():
        lines.append(f"File checksum list: `SHA256SUMS`; its SHA-256 is `{checksum(root / SUMS)}`.")
    else:
        lines.append("CAUTION: The file checksum list is unavailable; evidence indexing did not complete.")
    text = "\n".join(lines) + "\n"
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    require(destination, "GITHUB_STEP_SUMMARY is required")
    with Path(destination).open("a", encoding="utf-8") as stream:
        stream.write(text)
    print(f"Published {args.label} evidence facts; product/job status: {job_status}")
    return 0 if metadata and url and digest and (root / SUMS).is_file() else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=["index", "summary"], required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--label", choices=["node", "model", "s3-protocol", "s3-aws"], required=True)
    parser.add_argument("--artifact-url", default=os.environ.get("ARTIFACT_URL"))
    parser.add_argument("--artifact-digest", default=os.environ.get("ARTIFACT_DIGEST"))
    parser.add_argument("--job-status", choices=sorted(STATUSES),
                        default=os.environ.get("PRODUCT_JOB_STATUS", os.environ.get("JOB_STATUS", "unknown")))
    parser.add_argument("--exit-code", type=int, default=os.environ.get("PRODUCT_EXIT_CODE"))
    args = parser.parse_args()
    require(args.job_status in STATUSES, "the supplied product/job status is invalid")
    root = owned_directory(args.directory)
    if args.stage == "index":
        index_output(args, root)
        return 0
    return summary_output(args, root)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Evidence helper failed: {error}", file=sys.stderr)
        sys.exit(1)
