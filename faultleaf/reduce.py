"""Deletion reduction over the declared optional-token grammar."""

from copy import deepcopy

from .oracle import signature
from .replay import ReplayError, replay
from .scenario import validate


def reduce_failure(scenario, variant=None, target=None, max_pairs=128):
    original = validate(scenario)
    if not original.get("reduction", {}).get("eligible"):
        raise ValueError("Scenario is not eligible for token reduction")
    if not 1 <= max_pairs <= 128:
        raise ValueError("Replay pair budget must be in 1..128")
    mutations = original.get("mutations", [])
    if variant is None:
        if not mutations:
            raise ValueError("Reduction needs a declared unsafe variant")
        variant = mutations[0]["variant"]
    if target is None:
        matches = [m["target"] for m in mutations if m["variant"] == variant]
        if not matches:
            raise ValueError("Reduction needs an exact declared target signature")
        target = matches[0]
    required = set(original["reduction"].get("required_fault_ids", []))
    evidence, pairs = [], 0

    def predicate(candidate):
        nonlocal pairs
        if pairs >= max_pairs:
            return None
        pairs += 1
        try:
            unsafe, repaired = replay(candidate, variant), replay(candidate)
        except (ReplayError, ValueError) as exc:
            evidence.append({"pair": pairs, "accepted": False, "error": type(exc).__name__})
            return False
        reached = []
        for run in (unsafe, repaired):
            reached.append({t["detail"]["id"] for t in run["timeline"] if t["kind"] == "fault_injected"})
        pinned = {s["token_id"] for s in candidate["schedule"] if s.get("pinned")}
        okay = (target in [signature(v) for v in unsafe["violations"]] and repaired["passed"]
                and all(required <= ids for ids in reached)
                and not pinned.intersection(unsafe["unreached_tokens"])
                and not pinned.intersection(repaired["unreached_tokens"]))
        evidence.append({"pair": pairs, "accepted": okay,
                         "tokens": [s["token_id"] for s in candidate["schedule"]],
                         "unsafe_digest": unsafe["canonical_digest"], "candidate_digest": repaired["canonical_digest"]})
        return okay

    current = deepcopy(original)
    if not predicate(current):
        return {"status": "target_not_reproduced", "signature": target, "replay_pairs": pairs,
                "minimality": "not_checked", "evidence": evidence, "scenario": current,
                "original_tokens": len(original["schedule"]), "remaining_tokens": len(current["schedule"])}
    # Only deletions in this bounded grammar are supported; authoritative facts/faults/horizon stay pinned.
    changed, budget = True, False
    while changed:
        changed = False
        for step in list(current["schedule"]):
            if step.get("pinned") or step["action"] not in ("deliver", "wake", "restart_worker"):
                continue
            trial = deepcopy(current)
            trial["schedule"] = [s for s in trial["schedule"] if s["token_id"] != step["token_id"]]
            outcome = predicate(trial)
            if outcome is None:
                budget = True
                break
            if outcome:
                current, changed = trial, True
                break
        if budget:
            break
    return {"status": "budget_exhausted" if budget else "reproduced", "signature": target,
            "original_tokens": len(original["schedule"]), "remaining_tokens": len(current["schedule"]),
            "replay_pairs": pairs, "minimality": "not_proven" if budget else "deletion_minimal_within_optional_token_grammar",
            "evidence": evidence, "scenario": current}
