"""Declared target detection, repaired controls and replay identity."""

from .oracle import signature
from .replay import replay
from .scenario import examples, load


def run_suite(*, http=False, repeat=False):
    cases = []
    for path in examples():
        scenario = load(path)
        candidate = replay(scenario, http=http)
        mutations = []
        for mutation in scenario["mutations"]:
            unsafe = replay(scenario, mutation["variant"], http=http)
            mutations.append({"variant": mutation["variant"], "target": mutation["target"],
                              "detected": unsafe["valid"] and mutation["target"] in [signature(v) for v in unsafe["violations"]],
                              "digest": unsafe["canonical_digest"], "violations": unsafe["violations"]})
        deterministic = not repeat or candidate["canonical_digest"] == replay(scenario, http=http)["canonical_digest"]
        reached_faults = {e["detail"]["id"] for e in candidate["timeline"] if e["kind"] == "fault_injected"}
        required_faults = {f["id"] for f in scenario["faults"]}
        complete = not candidate["unreached_tokens"] and required_faults <= reached_faults
        cases.append({"id": scenario["id"], "candidate_passed": candidate["passed"],
                      "deterministic": deterministic, "declared_schedule_reached": complete,
                      "candidate_digest": candidate["canonical_digest"], "violations": candidate["violations"],
                      "mutations": mutations})
    passed = bool(cases) and all(c["candidate_passed"] and c["deterministic"] and c["declared_schedule_reached"]
                                 and all(m["detected"] for m in c["mutations"]) for c in cases)
    return {"schema_version": 1, "passed": passed, "engine": "loopback-http" if http else "deterministic-memory",
            "repeated": repeat, "cases": cases}
