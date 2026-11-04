#!/usr/bin/env python3
"""Read-only credential preflight for the fixed protected CI baseline writer.

Only GH_TOKEN supplies authority. Metadata GETs prove the observed read context,
not secret or variable write permission. No baseline phrase or value is read.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

from support import OperationError, decode_json, require

REPO = "EdgeVector/lastdb"
ENVIRONMENT = "cloud-backup-dev"
SECRET = "LASTDB_CLOUD_DEV_BASELINE"
VARIABLE = "LASTDB_CLOUD_DEV_BASELINE_SCOPE"
MAX_BYTES = 65536
MAX_SECONDS = 50
ENDPOINTS = (
    ("repository", "/repos/" + REPO),
    ("environment_secret", "/repos/" + REPO + "/environments/" + ENVIRONMENT + "/secrets/" + SECRET),
    ("repository_variable", "/repos/" + REPO + "/actions/variables/" + VARIABLE),
)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, _request, _response, _code, _message, _headers, _url):
        return None


def token_class(token):
    for prefix, kind in (("github_pat_", "fine_grained_pat"), ("ghp_", "classic_pat"),
                         ("gho_", "oauth"), ("ghu_", "app_user"), ("ghs_", "app_installation")):
        if token.startswith(prefix):
            return kind
    return "unknown"


def explicit_token():
    token = os.environ.get("GH_TOKEN", "")
    require(bool(token), "baseline_writer_gh_token_missing")
    require(len(token) <= 8192 and token.strip() == token
            and all(32 < ord(character) < 127 for character in token),
            "baseline_writer_gh_token_invalid")
    return token


def gh_environment():
    """Pin gh's target and prevent fallback or debug output in the writer."""
    explicit_token()
    value = os.environ.copy()
    value.pop("GITHUB_TOKEN", None)
    value.pop("GH_DEBUG", None)
    value["GH_HOST"] = "github.com"
    value["GH_PROMPT_DISABLED"] = "1"
    return value


def header_facts(headers):
    scopes = headers.get("X-OAuth-Scopes")
    return {
        "scope_header_present": scopes is not None,
        "repo_scope": ("repo" in {item.strip() for item in scopes.split(",")}) if scopes is not None else None,
        "sso_required": headers.get("X-GitHub-SSO", "").split(";", 1)[0].strip() == "required",
        "rate_limited": headers.get("X-RateLimit-Remaining") == "0",
    }


def metadata(opener, token, path, endpoint, deadline):
    observation = {"endpoint": endpoint, "http_status": None}
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        observation["failure"] = "deadline_exceeded"
        return None, observation
    request = urllib.request.Request("https://api.github.com" + path, method="GET", headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "lastdb-baseline-writer-preflight",
    })
    try:
        with opener.open(request, timeout=min(15, remaining)) as response:
            observation.update(http_status=response.status, **header_facts(response.headers))
            if response.status != 200:
                observation["failure"] = "http_refused"
                return None, observation
            raw = response.read(MAX_BYTES + 1)
            require(len(raw) <= MAX_BYTES and time.monotonic() <= deadline,
                    "baseline_writer_metadata_bound")
            value = decode_json(raw)
            require(isinstance(value, dict), "baseline_writer_metadata_shape")
            return value, observation
    except urllib.error.HTTPError as error:
        observation.update(http_status=error.code, **header_facts(error.headers))
        observation["failure"] = "http_refused"
        error.close()
    except OperationError:
        observation["failure"] = "invalid_or_oversized_metadata"
    except Exception:
        observation["failure"] = "request_failed"
    return None, observation


def validate_metadata(endpoint, value, observation):
    if endpoint == "repository":
        require(value.get("full_name") == REPO, "baseline_writer_repository_mismatch")
        permissions = value.get("permissions", {})
        if isinstance(permissions, dict):
            for key in ("admin", "push"):
                flag = permissions.get(key)
                if type(flag) is bool:
                    observation["repository_" + key] = flag
    else:
        expected = SECRET if endpoint == "environment_secret" else VARIABLE
        require(value.get("name") == expected, "baseline_writer_metadata_identity")


def preflight():
    result = {"result": "failed", "repo": REPO, "environment": ENVIRONMENT,
              "token_class": "missing", "read_only": True, "write_permission_proven": False,
              "baseline_secret_derived": False, "requests": []}
    try:
        token = explicit_token()
        kind = token_class(token)
        result["token_class"] = kind
        deadline = time.monotonic() + MAX_SECONDS
        opener = urllib.request.build_opener(NoRedirect())
        for endpoint, path in ENDPOINTS:
            value, observation = metadata(opener, token, path, endpoint, deadline)
            result["requests"].append(observation)
            require(value is not None, "baseline_writer_" + endpoint + "_read_refused")
            validate_metadata(endpoint, value, observation)
            require(not observation["sso_required"], "baseline_writer_sso_required")
            require(not observation["rate_limited"], "baseline_writer_rate_limited")
            # Scope headers describe classic/OAuth grants. Their absence does
            # not establish insufficient permissions for fine-grained tokens.
            if kind in ("classic_pat", "oauth") and observation["scope_header_present"]:
                require(observation["repo_scope"], "baseline_writer_repo_scope_missing")
        result["result"] = "passed"
    except OperationError as error:
        result["failure"] = str(error)
    except Exception:
        result["failure"] = "baseline_writer_preflight_failed"
    return result


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    try:
        result = preflight()
    except BaseException:
        result = {"result": "failed", "failure": "baseline_writer_preflight_interrupted",
                  "read_only": True, "write_permission_proven": False}
    print(json.dumps(result, sort_keys=True))
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        with open(destination, "a") as stream:
            stream.write("\n### Baseline writer credential preflight\n\n```json\n"
                         + json.dumps(result, sort_keys=True, indent=2) + "\n```\n")
    return 0 if result["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
