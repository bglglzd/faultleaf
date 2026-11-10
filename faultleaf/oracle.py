"""Business assertions over observed stores and retained writes, independent of worker flags."""

from collections import defaultdict

from .scenario import SERVICES

IDENTITY_RULES = dict(zip(SERVICES, ("CRM_CONTACT_IDENTITY", "CALENDAR_BOOKING_IDENTITY", "CONFIRMATION_IDENTITY")))


def signature(violation):
    return {key: violation[key] for key in ("class", "rule", "service", "lead_id", "version")}


def evaluate(scenario, state, claims, timeline):
    violations = []

    def add(kind, rule, service, lead, version, expected, actual, witness):
        entry = {"class": kind, "rule": rule, "service": service, "lead_id": lead, "version": version,
                 "expected": expected, "actual": actual, "witness": witness}
        if not any(signature(v) == signature(entry) for v in violations):
            violations.append(entry)

    active = {s: [r for r in state[s]["items"] if r.get("active", True)] for s in SERVICES}
    expected = scenario["expected"]
    leads = {f["lead_id"] for f in scenario["facts"].values()}
    isolated = len(leads) > 1
    for service in SERVICES:
        groups = defaultdict(list)
        for effect in state[service]["effects"]:
            if effect.get("effect") in ("create", "update") and effect.get("operation") != "operator_edit":
                groups[(effect["lead_id"], effect["version"])].append(effect)
        for (lead, version), effects in sorted(groups.items()):
            if len(effects) > 1:
                add("duplicate_effect", IDENTITY_RULES[service], service, lead, version,
                    "one committed upsert per business version", len(effects), effects)
        wanted = expected["services"].get(service, [])
        for row in wanted:
            lead, version = row["lead_id"], row.get("version", 1)
            actual = [r for r in active[service] if r["lead_id"] == lead]
            matching = [r for r in actual if all(r.get(k) == v for k, v in row.items()
                                                if k not in ("kind", "remaining"))]
            if matching:
                continue
            rule = "LEAD_IDENTITY_ISOLATION" if isolated else "EVENTUAL_CURRENT_EFFECTS"
            if not actual:
                add("missing_effect", rule, service, lead, version, row, actual,
                    [a for a in state[service]["attempts"] if a["lead_id"] == lead])
            else:
                add("stale_overwrite", "LATEST_REQUEST_REVISION", service, lead, version, row, actual,
                    [e for e in state[service]["effects"] if e["lead_id"] == lead])
                if service == "crm" and any(k in row for k in ("operator_tags", "status")):
                    initial = [r for r in scenario["initial"]["crm"] if r["lead_id"] == lead]
                    if initial and any(r.get("status") != row.get("status") or
                                       r.get("operator_tags", []) != row.get("operator_tags", []) for r in actual):
                        add("stale_overwrite", "OPERATOR_FIELDS_PRESERVED", service, lead, version,
                            {k: row[k] for k in ("status", "operator_tags") if k in row}, actual, initial)
        for row in active[service]:
            if not any(r["lead_id"] == row["lead_id"] and
                       r.get("version", row["version"]) == row["version"] for r in wanted):
                facts = [f for f in scenario["facts"].values() if f["lead_id"] == row["lead_id"]]
                version = max((f["version"] for f in facts), default=row["version"])
                add("stale_overwrite", "LATEST_REQUEST_REVISION", service, row["lead_id"], version,
                    [r for r in wanted if r["lead_id"] == row["lead_id"]], row, state[service]["effects"])

    # An operator close is a barrier for future creations, even if later cleanup hid them.
    for edit in state["crm"]["effects"]:
        if edit.get("operation") != "operator_edit" or not edit.get("after"):
            continue
        if edit["after"][0]["status"] != "closed":
            continue
        lead, version = edit["lead_id"], edit["version"]
        committed_after = [e for s in ("calendar", "notifications") for e in state[s]["effects"]
                           if e["lead_id"] == lead and e["seq"] > edit["seq"]
                           and e.get("effect") in ("create", "update")]
        if committed_after:
            add("stale_overwrite", "OPERATOR_STATUS_GUARD", "crm", lead, version,
                "no new appointment effects after operator close", committed_after, edit)
        if any(r["lead_id"] == lead for r in active["calendar"]):
            add("stale_overwrite", "CLOSED_LEAD_NO_ACTIVE_EFFECT", "calendar", lead, version,
                [], [r for r in active["calendar"] if r["lead_id"] == lead], edit)

    # Queuing an obsolete confirmation after a newer request arrived is itself observable harm.
    for delivery in [e for e in timeline if e["kind"] == "fact_activated"]:
        for effect in state["notifications"]["effects"]:
            if (effect["lead_id"] == delivery["lead_id"] and effect["version"] < delivery["version"]
                    and effect["seq"] > delivery["seq"] and effect.get("effect") in ("create", "update")):
                add("stale_overwrite", "LATEST_REQUEST_REVISION", "notifications", delivery["lead_id"],
                    delivery["version"], "no obsolete confirmation after newer intent", effect, delivery)

    for claim in claims:
        if claim["state"] != "complete":
            continue
        lead, version = claim["lead_id"], claim["version"]
        for service in SERVICES:
            rows = [r for r in active[service] if r["lead_id"] == lead and r["version"] == version]
            if not rows:
                add("false_success", "COMPLETE_REQUIRES_CURRENT_EFFECTS", service, lead, version,
                    "current committed business state", rows, claim)
    for service, field, upstream in (("calendar", "crm_id", "crm"),
                                     ("notifications", "crm_id", "crm"),
                                     ("notifications", "booking_id", "calendar")):
        for row in active[service]:
            matches = [r for r in active[upstream] if r["id"] == row.get(field)
                       and r["lead_id"] == row["lead_id"] and r["version"] == row["version"]]
            if not matches:
                add("stale_overwrite", "CROSS_SERVICE_REFERENCE", service, row["lead_id"], row["version"],
                    f"current {upstream} record for this lead/version", row.get(field), row)
    for wanted in expected.get("claims", []):
        actual = [c for c in claims if c["lead_id"] == wanted["lead_id"]]
        match = [c for c in actual if all(c.get(k) == v for k, v in wanted.items())]
        if not match and not any(v["lead_id"] == wanted["lead_id"] for v in violations):
            # Missing work is checked above; this covers loss of durable completion/pending knowledge.
            add("missing_effect", "DURABLE_WORK_STATE", "worker", wanted["lead_id"], wanted.get("version", 1),
                wanted, actual, wanted)
    return violations
