"""Private HTTPS exchange with the DEV fresh-account lease broker."""

import hashlib
import json
import os
import re
from urllib.error import URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from support import OperationError, decode_json, require


AUDIENCE = "lastdb-fresh-backup-dev"
RESPONSE_BYTES = 65536
POINT_FIELDS = ("account_id", "account_generation", "stream_id", "commit_id", "commit_sha256")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def exchange(opener, url, token, body, failure):
    require(isinstance(token, str) and re.fullmatch(r"[\x21-\x7e]{1,32768}", token), failure)
    headers = {"Authorization": "Bearer " + token, "Accept": "application/json"}
    payload = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        payload = json.dumps(body, separators=(",", ":")).encode()
    try:
        with opener.open(Request(url, data=payload, headers=headers), timeout=20) as response:
            require(response.status == 200, failure)
            raw = response.read(RESPONSE_BYTES + 1)
            require(len(raw) <= RESPONSE_BYTES, failure)
            value = decode_json(raw)
    except (OSError, URLError, ValueError, OperationError):
        raise OperationError(failure) from None
    require(isinstance(value, dict), failure)
    return value


class Broker:
    def __init__(self):
        self.endpoint = os.environ.get("LASTDB_FRESH_DEV_BROKER_URL", "")
        require(re.fullmatch(r"https://[a-z0-9]+\.execute-api\.us-west-2\.amazonaws\.com",
                             self.endpoint), "dev_broker_endpoint")
        oidc_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
        require(0 < len(oidc_url) <= 4096 and not any(ord(char) <= 32 for char in oidc_url),
                "github_oidc_endpoint")
        url = urlsplit(oidc_url)
        require(url.scheme == "https" and re.fullmatch(
            r"(?:[a-z0-9-]+\.)+actions\.githubusercontent\.com", url.netloc)
            and url.path.startswith("/") and not url.fragment, "github_oidc_endpoint")
        query = parse_qsl(url.query, max_num_fields=32)
        require(all(name != "audience" for name, _value in query), "github_oidc_audience")
        self.oidc_url = urlunsplit(url._replace(query=urlencode([*query, ("audience", AUDIENCE)])))
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def post(self, route, body):
        require(route in ("lease", "enrolled", "snapshot", "verified"), "dev_broker_route")
        oidc = exchange(self.opener, self.oidc_url, os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
                        None, "github_oidc_request")
        token = oidc.get("value")
        require(isinstance(token, str) and len(token) <= 32768
                and re.fullmatch(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2}", token), "github_oidc_token")
        return exchange(self.opener, self.endpoint + "/" + route, token, body, "dev_broker_" + route)

    def lease(self, account, operation, public_key):
        result = self.post("lease", {"account_id": account, "operation": operation, "public_key": public_key})
        invite = result.get("invite_code")
        require(set(result) == {"account_id", "invite_code"} and result["account_id"] == account
                and isinstance(invite, str) and re.fullmatch(r"[\x21-\x7e]{1,512}", invite),
                "dev_broker_lease_receipt")
        return invite.encode()

    def enrolled(self, account, api_key):
        result = self.post("enrolled", {"account_id": account, "api_key_sha256": hashlib.sha256(api_key).hexdigest()})
        require(result == {"account_id": account, "state": "enrolled"}, "dev_broker_enrolled_receipt")

    def point(self, route, point):
        require(route in ("snapshot", "verified") and set(point) == set(POINT_FIELDS), "dev_broker_point")
        require(self.post(route, point) == point, "dev_broker_" + route + "_receipt")
