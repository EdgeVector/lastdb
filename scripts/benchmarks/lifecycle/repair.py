"""Tom-approved one-time repair drill; this integration PR does not merge."""

FIELDS = ["bucket", "id", "body"] + [f"field-{number:02}" for number in range(24)]
SCOPE = "6e18c61c7416a50b7f221c4cf2f80b6e9a1f4459db974a30430a1c87e3df44ca"


def seed(node):
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": "LifecycleRepair", "descriptive_name": "LifecycleRepair",
        "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
        "fields": FIELDS, "field_descriptions": {field: "Generated repair drill " + field for field in FIELDS}}})
    mutations = []
    for number in range(8):
        fields = {field: f"value-{number}-{field}" for field in FIELDS}
        fields.update(bucket="repair", id=f"key-{number:02}", body=f"body-{number}")
        mutations.append({"type": "mutation", "schema": "LifecycleRepair",
            "key_value": {"hash": "repair", "range": fields["id"]}, "fields_and_values": fields,
            "mutation_type": "create", "durability": "durable"})
    result, _ = node.call("/api/mutations/batch", {"mutations": mutations})
    if result.get("count") != 8 or result.get("durability") != "durable":
        raise RuntimeError("The real wide-write batch lacks its durable acknowledgement")


def sample(node):
    value, _ = node.call("/api/query", {"schema_name": "LifecycleRepair",
        "fields": FIELDS, "limit": 8,
        "filter": {"HashRangeKeys": [["repair", f"key-{number:02}"] for number in range(8)]}})
    if len(value["results"]) != 8 or value.get("has_more"):
        raise RuntimeError("The complete generated repair read lost a key")
    return sorted(value["results"], key=lambda row: row["key"]["range"])


def measure_repair(node, args, command, recount):
    seed(node)
    expected = sample(node)
    node.stop()
    sequence = 0

    def helper(phase):
        nonlocal sequence
        sequence += 1
        return command(node, args.helper, [str(node.home), phase],
                       f"repair-{sequence:02}-{phase}")

    second_scope = helper("repair-seed-scope")
    repair_command = recount

    def recount(node, cli, label, **settings):
        return repair_command(node, cli, label, scope=SCOPE, **settings)

    damage = helper("repair-inject-repairable")
    if damage["original_digest"] == damage["damaged_digest"]:
        raise RuntimeError("The generated drill changed no protected data")
    plans = [recount(node, args.cli, f"repair-plan-{shards}", shards=shards)
             for shards in (1, 2, 4)]
    planned = plans[0]["recount"]["molecules"]
    if any(planned != plan["recount"]["molecules"] for plan in plans[1:]):
        raise RuntimeError("One-pass, two-pass, and four-pass molecule plans differ")
    if helper("repair-inventory")["write_surface_digest"] != damage["damaged_write_surface_digest"]:
        raise RuntimeError("The report-only recount changed protected data")
    written = recount(node, args.cli, "repair-damaged-execute", execute=True)
    repaired = written["recount"]["molecules"]
    for name, minimum in (("headers_rewritten", 2), ("bindings_rewritten", 1), ("bindings_deleted", 1)):
        if repaired[name] < minimum:
            raise RuntimeError("The repair did not exercise " + name)
    if repaired != planned:
        raise RuntimeError("The actual repairs differ from the report-only plan")
    if helper("repair-inventory")["digest"] != damage["original_digest"]:
        raise RuntimeError("Repair changed metadata, holders, values, or correct counts")
    repeat = recount(node, args.cli, "repair-damaged-repeat", execute=True, shards=1)
    names = ("headers_rewritten", "bindings_rewritten", "bindings_deleted", "drains_queued")
    if any(repeat["recount"]["molecules"][name] for name in names):
        raise RuntimeError("A repeated damaged-source repair is not a no-op")
    helper("repair-restore")
    refusals = {}
    for case in ("missing-header", "unbound-live", "unreadable-tip", "unreadable-conflict",
                 "unreadable-header", "unreadable-binding", "binding-identity"):
        damage = helper("repair-inject-" + case)
        label = "repair-refusal-" + case
        result = recount(node, args.cli, label, execute=True, success=False)
        text = ((node.output / (label + ".stdout")).read_text(errors="replace")
                + (node.output / (label + ".stderr")).read_text(errors="replace"))
        if "molecule recount:" not in text:
            raise RuntimeError("The damaged-source refusal did not come from molecule validation")
        after = helper("repair-inventory")
        if after["write_surface_digest"] != damage["damaged_write_surface_digest"]:
            raise RuntimeError("A refused repair wrote a partial change")
        helper("repair-restore")
        refusals[case] = {"exit_code": result["exit_code"], "protected_data_unchanged": True}
    node.start()
    if sample(node) != expected:
        raise RuntimeError("The generated wide records changed after the repair drill and reopen")
    return {"complete": True, "one_pass_equals_two_and_four_passes": True,
            "plan_writes_nothing": True, "repairs": repaired, "repeat_is_no_op": True,
            "restored_source_digest": damage["original_digest"], "refusals": refusals,
            "reopened_values_equal": True,
            "second_scope": second_scope,
            "scope": "Approved one-time generated damage; two populated storage scopes in both key modes"}
