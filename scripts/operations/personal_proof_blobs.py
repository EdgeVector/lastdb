"""`blob/list` and `blob/drop`, asked by the proof itself with requests signed as a device.

`list_blobs` reads every page of `blob/list`. `omitted_stream_refused` sends one `blob/drop` that a
correct node never sends: it names the head of the snapshot stream and every other stream of the
publisher but one. A relay must answer HTTP 409 `{"code": "heads", "field": "heads"}` and drop nothing,
because an entry on a stream the caller did not name may name a file of the list. A device that sends its
first entry after a node listed the streams is such a stream: the node's sweep is refused and lists again.
"""
from personal_proof_deployed_trim import node_hex
from personal_proof_trim import relay_post
from record_runs import require


def list_blobs(relay, device, node):
    """`blob/list`, every page: digest -> length. The pages ascend by digest and hold at most 1,000."""
    found, after = {}, None
    while True:
        body = {"database_id": device.database, **({"after": after} if after else {})}
        status, answer = relay_post(relay, device, device.database, node, "/relay/v1/blob/list", body)
        require(status == 200, f"blob/list: {status} {answer!r}")
        digests = [item["sha256"] for item in answer["blobs"]]
        require(len(digests) <= 1000 and digests == sorted(set(digests)) and (not after or not digests or digests[0] > after),
                "a page of blob/list is not at most 1,000 digests in ascending order after the last page")
        found.update({item["sha256"]: item["bytes"] for item in answer["blobs"]})
        after = answer.get("next")
        if after is None:
            return found


def omitted_stream_refused(relay, device, view):
    """A drop that leaves one stream of the publisher out of `heads` is 409 `heads`, and nothing goes.

    `view` is what the relay holds now. The digest the request names is a sealed file that an entry after
    the cut names, so a relay that took the request would lose a file the proof then misses.
    """
    database, node, snapshot = device.database, node_hex(device), view["snapshot"]
    status, answer = relay_post(relay, device, database, node, "/relay/v1/log/streams",
                                {"database_id": database, "writer_key": node})
    require(status == 200, f"log/streams: {status} {answer!r}")
    heads = [{"device_id": item["device_id"], "writer_seq": item["head_seq"]} for item in answer["streams"]
             if item["device_id"] != snapshot["device_id"]]
    sealed = sorted(view["named"] - set(view["files"]))
    require(len(heads) >= 2 and sealed, f"the proof needs two streams and a sealed file: {len(heads)} streams, {len(sealed)} files")
    body = {"database_id": database, "snapshot": {"device_id": snapshot["device_id"], "writer_seq": snapshot["writer_seq"]},
            "heads": heads[1:], "sha256": sealed[:1]}
    status, answer = relay_post(relay, device, database, node, "/relay/v1/blob/drop", body)
    require(status == 409 and answer.get("code") == "heads" and answer.get("field") == "heads",
            f"a blob/drop that omits the stream {heads[0]['device_id'][:8]} was answered {status} {answer!r}, expected 409 heads")
    require(list_blobs(relay, device, node) == view["blobs"], "the refused blob/drop changed what the relay holds")
    print(f"   a blob/drop that names {len(heads) - 1} of the publisher's {len(heads)} streams was answered 409 heads and "
          "dropped nothing", flush=True)
