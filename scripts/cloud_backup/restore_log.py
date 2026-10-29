"""Bind product-validated routing evidence to one completed immutable log chain."""

import re

from support import canonical_uuid, require


def integer(value):
    require(type(value) is int and 0 <= value <= 2**64 - 1, "log_proof_integer")
    return value


def position(value):
    require(isinstance(value, dict) and set(value) == {"epoch", "sequence", "sha256", "frontiers"},
            "log_proof_position")
    canonical_uuid(value["epoch"])
    integer(value["sequence"])
    require(isinstance(value["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", value["sha256"]),
            "log_proof_position_hash")
    require(isinstance(value["frontiers"], dict) and set(value["frontiers"]) == {"main", "cas"},
            "log_proof_frontiers")
    for number in value["frontiers"].values():
        integer(number)
    return value


def prefix(proof, minimum):
    before, after = position(proof["from"]), position(proof["through"])
    hashes = proof["entry_sha256s"]
    require(before["epoch"] == after["epoch"] == minimum["epoch"]
            and isinstance(hashes, list) and 1 <= len(hashes) <= 128
            and after["sequence"] - before["sequence"] == len(hashes)
            and all(isinstance(item, str) and re.fullmatch(r"[0-9a-f]{64}", item) for item in hashes)
            and hashes[-1] == after["sha256"]
            and all(after["frontiers"][name] >= before["frontiers"][name] for name in ("main", "cas")),
            "authenticated_log_position_range")
    number = minimum["sequence"]
    if not before["sequence"] <= number <= after["sequence"]:
        return False
    require(all(before["frontiers"][name] <= minimum["frontiers"][name] <= after["frontiers"][name]
                for name in ("main", "cas")), "minimum_log_frontiers_in_prefix")
    if number == before["sequence"]:
        require(minimum == before, "minimum_log_from_position")
    elif number == after["sequence"]:
        require(minimum == after, "minimum_log_through_position")
    else:
        # Each authenticated entry hash commits its sequence and before/after frontiers.
        require(hashes[number - before["sequence"] - 1] == minimum["sha256"],
                "minimum_log_hash_in_authenticated_prefix")
    return True


def totals(proof, accepted, index):
    require(integer(proof["version"]) == 1 and integer(proof["link_index"]) == index
            and proof["commit_sha256"] == accepted["commit_sha256"]
            and proof["root_receipt_id"] == accepted["root_receipt_id"]
            and proof["inventory_root"] == accepted["inventory_root"], "accepted_log_route_binding")
    values = {name: integer(proof[name]) for name in (
        "native_files", "native_bytes", "objects", "metadata_objects",
        "main_files", "cas_add_files", "cas_delete_files")}
    require(0 < values["native_files"] <= 4096 and 0 < values["objects"] <= 4096
            and values["native_bytes"] > 0 and 1 < values["metadata_objects"] <= values["objects"]
            and values["objects"] == int(accepted["inventory_root"]["object_count"])
            and sum(values[name] for name in ("main_files", "cas_add_files", "cas_delete_files"))
                == values["native_files"], "bounded_complete_log_route_proof")
    return values


def verified(read, chain_names, replay, accepted, observe):
    """Read bounded private receipts only after the product's full restore succeeds.

    The product authenticates every page, receipt, path, destination and extent.
    This binds those checks to every selected log link and the completed cursor.
    It does not claim an independent Python manifest or encryption check.
    """
    plan, cursor = read("chain/plan.json"), read("chain/cursor.json")
    links = integer(plan["links"])
    require(integer(plan["version"]) == integer(cursor["version"]) == 1 and 1 <= links <= 8
            and plan["head"] == cursor["head"] == accepted["commit_sha256"]
            and plan["base"] == replay["base"] and cursor["previous"] == accepted
            and integer(cursor["remaining"]) == integer(cursor["entry"]) == integer(cursor["part"]) == 0,
            "complete_selected_log_replay")
    expected = {"plan.json", "cursor.json"}
    expected.update(f"{kind}-{index:08}.json" for kind in ("link", "routes") for index in range(links))
    require(chain_names == expected and callable(observe), "complete_bounded_log_chain_evidence")
    minimum, final = position(replay["minimum"]), position(cursor["position"])
    sums = dict.fromkeys(("native_files", "native_bytes", "objects", "metadata_objects",
                          "main_files", "cas_add_files", "cas_delete_files"), 0)
    previous, through, covered = plan["base"], None, False
    observe(previous)
    for index in reversed(range(links)):
        point, proof = read(f"chain/link-{index:08}.json"), read(f"chain/routes-{index:08}.json")
        require(point["begin"]["job"]["stream"] == accepted["begin"]["job"]["stream"]
                and point["begin"]["predecessor"] == {"commit_id": previous["begin"]["job"]["commit_id"],
                    "ordinal": previous["ordinal"], "commit_sha256": previous["commit_sha256"]}
                and int(point["ordinal"]) > int(previous["ordinal"]), "exact_log_predecessor_chain")
        observe(point)
        counts = totals(proof, point, index)
        covered = prefix(proof, minimum) or covered
        require(through is None or proof["from"] == through, "continuous_authenticated_log_positions")
        through, previous = proof["through"], point
        for name, count in counts.items():
            sums[name] += count
    require(previous == accepted and through == final and covered
            and final["sequence"] >= minimum["sequence"]
            and all(final["frontiers"][name] >= minimum["frontiers"][name] for name in ("main", "cas")),
            "selected_log_covers_exact_written_prefix")
    require(0 < sums["native_files"] <= 4096 and 0 < sums["objects"] <= 4096
            and sums["main_files"] > 0 and sums["cas_add_files"] > 0,
            "real_main_and_cas_log_bytes")
    return {"main_r2": True, "cas_b2": True, "metadata_r2": True,
            "native_files": sums["native_files"], "native_bytes": sums["native_bytes"],
            "object_receipt_references": sums["objects"], "accepted_log_links": links,
            "product_validated_routes": True, "written_prefix_verified": True}
