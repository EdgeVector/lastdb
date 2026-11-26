"""The relay's contract revision: a node reads it from each relay answer and reports it.

Every answer of a relay names the contract revision it serves in `X-LastDB-Relay-Revision`, as `1.<n>`. A relay
from before revisions names none, which a node reads as revision 0. A node keeps the revision of its relay's
last answer in a sync turn and reports it with each `relay_protocol::Feature` and whether that relay serves it
(`relay_protocol::supports`): the object `cloud_sync.relay` of /api/status, and one line of `lastdb status`:

    Relay contract: 1.2, serves contract_probe
    Relay contract: 1.1, does not serve contract_probe

The proof asks the relay itself what it announces (an unsigned request, which a relay refuses with the header)
and requires the node to report the same revision, and each feature as served exactly when the revision reaches
the revision the feature arrived in. Against the reference relay it restarts the relay as a relay of revision
1.1 and as one from before revisions (`--announce-revision`), requires the node to follow each time, and then
restores it. Against a deployed relay it checks the revision that relay serves.
"""
import urllib.error
import urllib.request

from personal_proof_nodes import require, wait
from personal_proof_status import status_lines

HEADER = "X-LastDB-Relay-Revision"
PROTOCOL = "1"
# Any protocol route: an unsigned request is refused, and every answer of a route names the revision.
PROBE_ROUTE = "/relay/v1/log/streams"


def announced(url):
    """The revision the relay at `url` names on an answer: n of `1.<n>`, 0 when it names none."""
    request = urllib.request.Request(url + PROBE_ROUTE, data=b"{}", method="POST",
                                     headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            value = response.headers.get(HEADER)
    except urllib.error.HTTPError as error:
        value = error.headers.get(HEADER)
    if value is None:
        return 0
    protocol, _, revision = value.strip().partition(".")
    require(protocol == PROTOCOL and revision.isdigit(), f"the relay names revision {value!r}")
    return int(revision)


def reported(device, revision, label):
    """Wait until `device` reports `revision` for its relay, then check the features and the status line."""
    def probe():
        relay = device.sync_status().get("relay")
        return relay if isinstance(relay, dict) and relay.get("revision") == revision else None
    relay = wait(f"{device.name} to report relay contract {PROTOCOL}.{revision} ({label})", probe, seconds=120)
    features = relay.get("features")
    require(isinstance(features, list) and features, f"{device.name}: no relay features in {relay!r}")
    for feature in features:
        require(feature.get("supported") == (revision >= feature["since"]),
                f"{device.name}: relay contract {PROTOCOL}.{revision} reports {feature!r}")
    serves = [feature["name"] for feature in features if feature["supported"]]
    lacks = [feature["name"] for feature in features if not feature["supported"]]
    line = f"Relay contract: {PROTOCOL}.{revision}"
    if serves:
        line += f", serves {', '.join(serves)}"
    if lacks:
        line += f", does not serve {', '.join(lacks)}"
    lines = status_lines(device)
    require(line in lines, f"{device.name}: `lastdb status` has no {line!r}: {lines!r}")
    return {"revision": f"{PROTOCOL}.{revision}", "serves": serves, "does_not_serve": lacks}


def prove_revision(relay, device):
    """The reference relay as it is, as a relay of revision 1.1, and as one from before revisions."""
    current = announced(relay.url)
    require(current >= 2, f"the reference relay names revision {PROTOCOL}.{current}; this build serves 1.2 or later")
    report = {"current": reported(device, current, "this build")}
    saved = list(relay.extra)
    try:
        for name, option, revision in (("older", "1.1", 1), ("before_revisions", "none", 0)):
            relay.stop()
            relay.extra = [*saved, "--announce-revision", option]
            relay.start()
            require(announced(relay.url) == revision, f"the relay restarted with --announce-revision {option} names "
                                                       f"revision {announced(relay.url)}")
            report[name] = reported(device, revision, f"--announce-revision {option}")
    finally:
        relay.stop()
        relay.extra = saved
        relay.start()
    report["restored"] = reported(device, current, "restored")
    print(f"5r. {device.name} reports the relay contract it is given: {PROTOCOL}.{current} serves every feature, "
          "1.1 and a relay from before revisions serve none, and it follows each restart of the relay", flush=True)
    return report


def shows_revision(relay, device):
    """Against a deployed relay: the node reports the revision that relay names."""
    report = reported(device, announced(relay.url), "the deployed relay")
    print(f"the node reports relay contract {report['revision']} of the deployed relay", flush=True)
    return report
