from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from faultleaf.oracle import check_retry_policy, evaluate, signature
from faultleaf.process import verify_process_recovery
from faultleaf.reduce import reduce_failure
from faultleaf.replay import Engine, replay
from faultleaf.scenario import ScenarioError, examples, load, loads
from faultleaf.services import ServiceError
from faultleaf.suite import run_suite


def fixture(name):
    return load(next(p for p in examples() if p.stem == name))


class DomainTests(unittest.TestCase):
    def test_declared_business_states_and_mutation_signatures_repeat(self):
        result = run_suite(repeat=True)
        self.assertTrue(result["passed"], result)
        self.assertGreaterEqual(len(result["cases"]), 20)

    def test_actual_http_lost_ack_at_every_service_and_no_duplicate(self):
        for name in ("FL04", "FL05", "FL06"):
            with self.subTest(name=name):
                result = replay(fixture(name), http=True)
                self.assertTrue(result["passed"], result["violations"])
                self.assertTrue(any(e["kind"] == "idempotency_hit" for e in result["timeline"]))
                self.assertEqual(result["canonical_digest"], replay(fixture(name), http=True)["canonical_digest"])

    def test_backoff_survives_reopen_without_early_request(self):
        result = replay(fixture("FL20"))
        attempts = result["state"]["calendar"]["attempts"]
        self.assertEqual([a["tick"] for a in attempts], [0, 5])
        self.assertEqual([a["outcome"] for a in attempts], ["rate_limited", "committed"])

    def test_pending_horizon_cannot_hide_busy_retry_or_earlier_due(self):
        result = replay(fixture("FL10"))
        self.assertTrue(result["retry_policy"]["passed"])
        self.assertEqual(result["claims"][0]["next_due"], 30)
        attempt = deepcopy(result["state"]["calendar"]["attempts"][0])
        attempt.update(tick=1, seq=len(result["timeline"]) + 1, occurrence=2)
        result["state"]["calendar"]["attempts"].append(attempt)
        result["claims"][0]["next_due"] = 1
        policy = check_retry_policy(result["state"], result["claims"], result["timeline"], 10)
        self.assertEqual({e["rule"] for e in policy["errors"]}, {"RETRY_AFTER_RESPECTED", "DURABLE_RETRY_DUE"})

    def test_outage_pending_then_same_durable_work_recovers(self):
        scenario = fixture("FL11")
        with tempfile.TemporaryDirectory() as path, ExitStack() as stack:
            engine = Engine(scenario, "candidate", Path(path), stack)
            before = engine.run()
            self.assertTrue(before["passed"], before["violations"])
            self.assertEqual(before["claims"][0]["state"], "pending")
            engine.restart("test_pending_recovery")
            scenario["horizon"] = 24
            scenario["expected"] = fixture("FL01")["expected"]
            after = engine.run()
            self.assertTrue(after["passed"], after["violations"])
            self.assertEqual(after["claims"][0]["state"], "complete")
            self.assertEqual(len([t for t in after["timeline"] if t["kind"] == "delivery_received"]), 1)
            queued = after["state"]["notifications"]["effects"]
            self.assertEqual([e["tick"] for e in queued], [11])

    def test_distinct_worker_processes_recover_uncheckpointed_effects(self):
        for name in ("FL19", "FL21"):
            with self.subTest(name=name):
                result = verify_process_recovery(name)
                self.assertTrue(result["passed"], result)
                self.assertEqual([p["returncode"] for p in result["processes"]], [73, 0])

    def test_idempotency_payload_conflict_does_not_commit(self):
        scenario = fixture("FL01")
        with tempfile.TemporaryDirectory() as path, ExitStack() as stack:
            engine = Engine(scenario, "candidate", Path(path), stack)
            body = {k: v for k, v in scenario["facts"]["E1"].items() if k not in ("event_id", "type")}
            store = engine.stores["crm"]
            store.request("PUT", body, "stable", "E1", 0)
            body["slot"] = "changed"
            with self.assertRaises(ServiceError) as caught:
                store.request("PUT", body, "stable", "E1", 1)
            self.assertEqual(caught.exception.status, 409)
            self.assertEqual(len(store.snapshot()["effects"]), 1)

    def test_oracle_catches_tampered_state_and_deleted_duplicate_history(self):
        scenario = fixture("FL02")
        unsafe = replay(scenario, "M01_no_idempotency")
        state = deepcopy(unsafe["state"])
        # Cancellation cannot erase the committed duplicate witness.
        state["crm"]["items"] = state["crm"]["items"][:1]
        violations = evaluate(scenario, state, unsafe["claims"], unsafe["timeline"])
        self.assertIn(scenario["mutations"][0]["target"], [signature(v) for v in violations])
        candidate = replay(scenario)
        candidate["state"]["calendar"]["items"][0]["crm_id"] = "nonexistent"
        violations = evaluate(scenario, candidate["state"], candidate["claims"], candidate["timeline"])
        self.assertTrue(any(v["rule"] == "CROSS_SERVICE_REFERENCE" for v in violations))
        candidate = replay(scenario)
        duplicate = deepcopy(candidate["state"]["crm"]["items"][0])
        duplicate["id"] = "another-contact"
        candidate["state"]["crm"]["items"].append(duplicate)
        violations = evaluate(scenario, candidate["state"], candidate["claims"], candidate["timeline"])
        self.assertTrue(any(v["class"] == "duplicate_effect" and v["service"] == "crm" for v in violations))

    def test_later_correct_revision_cannot_hide_earlier_false_success(self):
        scenario = fixture("FL09")
        reschedule = fixture("FL16")
        scenario["facts"] = reschedule["facts"]
        scenario["expected"] = reschedule["expected"]
        scenario["schedule"].append({"token_id": "LATER", "action": "deliver", "at": 5,
                                     "event": "E2", "delivery_id": "D2", "pinned": True})
        unsafe = replay(scenario, "M03_partial_state_is_success")
        self.assertFalse(unsafe["passed"])
        self.assertTrue(any(v["class"] == "false_success" and v["version"] == 1 for v in unsafe["violations"]))
        self.assertTrue(replay(scenario)["passed"])

    def test_reducer_removes_noise_retains_duplicate_and_exact_signature(self):
        scenario = fixture("FL02")
        scenario["schedule"].append({"token_id": "NOISE", "action": "wake", "at": 5, "pinned": False})
        reduced = reduce_failure(scenario)
        self.assertEqual(reduced["status"], "reproduced")
        self.assertEqual(reduced["remaining_tokens"], 2)
        self.assertEqual(reduced["scenario"]["horizon"], scenario["horizon"])
        self.assertIn("deletion_minimal", reduced["minimality"])
        self.assertTrue(replay(reduced["scenario"])["passed"])
        self.assertIn(reduced["signature"], [signature(v) for v in
            replay(reduced["scenario"], scenario["mutations"][0]["variant"])["violations"]])
        budget = reduce_failure(scenario, max_pairs=1)
        self.assertEqual(budget["status"], "budget_exhausted")
        self.assertEqual(budget["minimality"], "not_proven")

    def test_reducer_preserves_required_fault_and_repaired_control(self):
        scenario = fixture("FL04")
        scenario["schedule"].append({"token_id": "NOISE", "action": "wake", "at": 9, "pinned": False})
        reduced = reduce_failure(scenario)
        self.assertEqual(reduced["remaining_tokens"], 1)
        self.assertEqual(reduced["scenario"]["faults"], scenario["faults"])
        self.assertEqual(reduced["scenario"]["facts"], scenario["facts"])

    def test_unreached_schedule_or_fault_cannot_pass_as_replayed(self):
        scenario = fixture("FL01")
        scenario["schedule"].append({"token_id": "NEVER", "action": "restart_worker", "pinned": False,
            "after": {"service": "calendar", "operation": "ensure_booking", "lead_id": "L1",
                      "version": 1, "point": "rate_limited"}})
        result = replay(scenario)
        self.assertEqual(result["violations"], [])
        self.assertFalse(result["valid"])
        self.assertFalse(result["passed"])
        self.assertEqual(result["unreached_tokens"], ["NEVER"])


class InputTests(unittest.TestCase):
    def test_omitted_optional_collections_are_normalized(self):
        from faultleaf.scenario import validate
        scenario = fixture("FL01")
        scenario.pop("faults")
        scenario.pop("mutations")
        scenario["initial"] = {}
        normalized = validate(scenario)
        self.assertEqual(normalized["faults"], [])
        self.assertEqual(normalized["mutations"], [])
        self.assertEqual(normalized["initial"], dict(crm=[], calendar=[], notifications=[]))
        self.assertTrue(replay(normalized)["passed"])

    def test_rejects_alias_tags_duplicate_keys_and_deep_yaml(self):
        for text in ("a: &x []\nb: *x", "!!python/object:os.system {}", "id: one\nid: two",
                     "a: " + "[" * 30 + "]" * 30, "date: 2026-11-10", "name: '\ud800'",
                     "horizon: " + "9" * 5000):
            with self.subTest(text=text[:40]), self.assertRaises(ScenarioError):
                loads(text)

    def test_invalid_fault_or_reference_is_an_input_error(self):
        from faultleaf.scenario import validate
        for mutate in (lambda s: s["schedule"][0].update(event="missing"),
                       lambda s: s.update(horizon=True),
                       lambda s: s["expected"].update(services=[]),
                       lambda s: s["expected"].pop("services"),
                       lambda s: s["facts"]["E1"].pop("status"),
                       lambda s: s["schedule"][0].update(pinned=False),
                       lambda s: s["facts"]["E1"].update(lead_id="../../file")):
            scenario = fixture("FL01")
            mutate(scenario)
            with self.assertRaises(ScenarioError):
                validate(scenario)


if __name__ == "__main__":
    unittest.main()
