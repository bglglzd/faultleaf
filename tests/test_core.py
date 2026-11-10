from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
import io
import json
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from faultleaf.oracle import check_retry_policy, evaluate, signature
from faultleaf.process import verify_process_recovery
from faultleaf.reduce import reduce_failure
from faultleaf.replay import Engine, ReplayError, compare, replay
from faultleaf.scenario import ScenarioError, examples, load, loads
from faultleaf.services import HTTPTransport, ServiceError, WorkerCrash
from faultleaf.suite import run_suite


def fixture(name):
    return load(next(p for p in examples() if p.stem == name))


class DomainTests(unittest.TestCase):
    def test_clean_control_runs_a_genuinely_unsafe_baseline_without_inventing_failure(self):
        result = compare(fixture("FL01"))
        self.assertEqual(result["runs"]["unsafe"]["variant"], "M01_no_idempotency")
        self.assertEqual(result["runs"]["candidate"]["variant"], "candidate")
        self.assertTrue(all(run["passed"] for run in result["runs"].values()))

    def test_numeric_occurrence_and_through_are_both_required(self):
        scenario = fixture("FL11")
        scenario["horizon"] = 3
        scenario["faults"][0].update(occurrence=1, through=3)
        scenario["expected"] = fixture("FL01")["expected"]
        result = replay(scenario, http=True)
        self.assertTrue(result["passed"], result["violations"])
        applied = [e for e in result["timeline"] if e["kind"] == "fault_injected"]
        self.assertEqual([e["tick"] for e in applied], [0])
        self.assertEqual(result["state"]["notifications"]["effects"][0]["tick"], 1)

    def test_http_error_responses_close_for_retry_and_crash_paths(self):
        for crash in (False, True):
            stream = io.BytesIO(b'{"error":"synthetic"}')
            headers = {"X-Faultleaf-Crash": "1"} if crash else {"Retry-After": "3"}
            error = HTTPError("http://127.0.0.1:1/items/L1", 503 if crash else 429, "Synthetic", headers, stream)
            with self.subTest(crash=crash), patch("faultleaf.services.urlopen", side_effect=error):
                with self.assertRaises(WorkerCrash if crash else ServiceError):
                    HTTPTransport({"crm": "http://127.0.0.1:1"}).get("crm", "L1")
            self.assertTrue(stream.closed)

    def test_redelivery_after_operator_close_preserves_receipt_and_closed_state(self):
        scenario = fixture("FL01")
        scenario["schedule"][0]["delivery_id"] = "operator-C2"
        scenario["facts"]["C2"] = dict(scenario["facts"]["E1"], event_id="C2", version=2,
                                       status="closed", type="operator")
        scenario["facts"]["E3"] = dict(scenario["facts"]["E1"], event_id="E3", version=3)
        scenario["schedule"] += [
            {"token_id": "CLOSE", "action": "manual_status", "at": 1, "event": "C2", "pinned": True},
            {"token_id": "NEW", "action": "deliver", "at": 2, "event": "E3", "delivery_id": "D3", "pinned": True},
            {"token_id": "REDELIVERY", "action": "deliver", "at": 3, "event": "E3", "delivery_id": "D3", "pinned": False},
        ]
        scenario["expected"] = {"services": {"crm": [dict(lead_id="L1", version=3, status="closed",
            display_name="Alex Example", operator_tags=[])], "calendar": [], "notifications": []},
            "claims": [dict(lead_id="L1", version=3, state="blocked")]}
        for http in (False, True):
            with self.subTest(http=http):
                result = replay(scenario, http=http)
                self.assertTrue(result["passed"], result["violations"])
                self.assertEqual(result["claims"][0]["state"], "blocked")
                self.assertEqual(len([e for e in result["timeline"] if e["kind"] == "delivery_received"]), 4)
                self.assertTrue(any(row["version"] == 1 and row["status"] == "open" and not row["active"]
                                    for row in result["state"]["crm"]["items"]))
                for service in ("calendar", "notifications"):
                    noops = [row for row in result["state"][service]["effects"] if row.get("effect") == "noop"]
                    self.assertTrue(noops)
                    self.assertTrue(all(row["before"] == row["after"] == [] and row["kind"] == "operation_noop"
                                        for row in noops))

    def test_preempted_after_commit_fault_cannot_satisfy_reducer_context(self):
        scenario = fixture("FL07")
        scenario["faults"].append(dict(id="LOST_ACK", service="crm", operation="ensure_contact",
            lead_id="L1", version=1, occurrence=2, mode="timeout_after_commit"))
        scenario["reduction"]["required_fault_ids"].append("LOST_ACK")
        for http in (False, True):
            with self.subTest(http=http):
                candidate = replay(scenario, http=http)
                self.assertTrue(candidate["passed"], candidate["violations"])
                lost = next(e for e in candidate["timeline"] if e["kind"] == "fault_injected"
                            and e["detail"]["id"] == "LOST_ACK")
                self.assertGreater(lost["seq"], candidate["state"]["crm"]["effects"][0]["seq"])
                unsafe = replay(scenario, "M06_ignore_retry_after_and_exhaust_budget", http=http)
                self.assertFalse(unsafe["valid"])
                self.assertIn("LOST_ACK", unsafe["unreached_faults"])
                self.assertEqual(unsafe["state"]["crm"]["effects"], [])
                self.assertFalse(any(e["kind"] == "fault_injected" and e["detail"]["id"] == "LOST_ACK"
                                     for e in unsafe["timeline"]))
        self.assertEqual(reduce_failure(scenario)["status"], "target_not_reproduced")

    def test_idempotency_hit_does_not_inject_selected_after_commit_fault(self):
        scenario = fixture("FL04")
        scenario["faults"].append(dict(id="NO_COMMIT", service="crm", operation="ensure_contact",
            lead_id="L1", version=1, occurrence=2, mode="crash_worker_after_commit_before_checkpoint"))
        result = replay(scenario, http=True)
        self.assertFalse(result["valid"])
        self.assertIn("NO_COMMIT", result["unreached_faults"])
        self.assertFalse(any(e["kind"] == "worker_crash" for e in result["timeline"]))
        self.assertEqual(len(result["state"]["crm"]["effects"]), 1)
        self.assertTrue(any(e["kind"] == "fault_not_applied" and e["detail"].get("reason") == "idempotency_hit"
                            for e in result["timeline"]))

    def test_nonadvancing_manual_edit_is_harness_error_at_and_after_commit(self):
        for http in (False, True):
            for anchor in ("at", "after"):
                with self.subTest(http=http, anchor=anchor):
                    scenario = fixture("FL01")
                    scenario["facts"]["E1"]["version"] = 2
                    scenario["facts"]["C1"] = dict(scenario["facts"]["E1"], event_id="C1", version=1,
                                                   status="closed", type="operator")
                    step = {"token_id": "OLD_CLOSE", "action": "manual_status", "event": "C1", "pinned": True}
                    if anchor == "at":
                        step["at"] = 1
                    else:
                        step["after"] = dict(service="crm", operation="ensure_contact", lead_id="L1",
                                             version=2, point="effect_committed")
                    scenario["schedule"].append(step)
                    with self.assertRaisesRegex(ReplayError, "OLD_CLOSE must advance"):
                        replay(scenario, http=http)

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
    def test_non_open_requests_fail_before_writes_or_report_creation(self):
        from faultleaf.cli import main
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, event in (("FL01", "E1"), ("FL16", "E2")):
                for status in ("priority", "closed"):
                    scenario = fixture(name)
                    scenario["facts"][event]["status"] = status
                    path = root / f"{name}-{status}.json"
                    path.write_text(json.dumps(scenario), encoding="utf-8")
                    output, error = io.StringIO(), io.StringIO()
                    destination = root / f"{name}-{status}-report"
                    with self.subTest(name=name, status=status), patch("sys.stdout", output), patch("sys.stderr", error), \
                            patch("faultleaf.cli.compare") as execute:
                        self.assertEqual(main(["replay", str(path), "--http", "--out", str(destination)]), 2)
                        execute.assert_not_called()
                        self.assertIn(path.name, error.getvalue())
                        self.assertIn("Request status must be open", error.getvalue())
                        self.assertEqual(output.getvalue(), "")
                        self.assertFalse(destination.exists())

    def test_control_typos_and_malformed_targets_are_input_errors(self):
        from faultleaf.scenario import validate
        changes = [
            ("fact", lambda s: s["facts"]["E1"].update(note="ignored")),
            ("schedule", lambda s: s["schedule"][0].update(tick=3)),
            ("fault", lambda s: s["faults"][0].update(thru=s["faults"][0].pop("through"))),
            ("mutation.target", lambda s: s["mutations"][0]["target"].pop("version")),
            ("mutation.target", lambda s: s["mutations"][0]["target"].update(verison=1)),
            ("mutation.target class", lambda s: s["mutations"][0]["target"].update({"class": []})),
            ("mutation.target service", lambda s: s["mutations"][0]["target"].update(service="other")),
            ("mutation.target rule", lambda s: s["mutations"][0]["target"].update(rule={})),
            ("mutation.target lead_id", lambda s: s["mutations"][0]["target"].update(lead_id=[])),
            ("mutation.target version", lambda s: s["mutations"][0]["target"].update(version=True)),
        ]
        for message, change in changes:
            scenario = fixture("FL11")
            change(scenario)
            with self.subTest(message=message), self.assertRaisesRegex(ScenarioError, message):
                validate(scenario)

    def test_reopening_operator_events_are_rejected_by_the_declared_close_only_profile(self):
        from faultleaf.scenario import validate
        for status in ("open", "priority"):
            scenario = fixture("FL14")
            scenario["facts"]["M_close2"]["status"] = status
            with self.subTest(status=status), self.assertRaisesRegex(ScenarioError, "reopening is outside"):
                validate(scenario)

    def test_optional_description_produces_viewable_report_metadata(self):
        scenario = fixture("FL01")
        scenario.pop("description")
        report = compare(scenario)
        self.assertEqual(report["scenario"]["description"], "")
        from faultleaf.scenario import validate
        for value in (None, 12, [], "x" * 2001):
            scenario["description"] = value
            with self.subTest(value=type(value).__name__), self.assertRaises(ScenarioError):
                validate(scenario)

    def test_yaml_error_identifies_file_and_syntax_position(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "broken.yaml"
            path.write_text("schema_version: [\n", encoding="utf-8")
            with self.assertRaisesRegex(ScenarioError, r"broken.yaml: Invalid YAML at line \d+, column \d+"):
                load(path)

    def test_date_and_large_scalar_diagnostics_identify_the_input_location(self):
        with self.assertRaisesRegex(ScenarioError, "line 1, column 7 must be quoted"):
            loads("slot: 2030-01-01\n")
        with self.assertRaisesRegex(ScenarioError, "Invalid scalar for horizon at line 1, column 10"):
            loads("horizon: " + "1" * 5000 + "\n")

    def test_cli_distinguishes_checks_from_pending_complete_or_blocked_business_state(self):
        from faultleaf.cli import main
        with tempfile.TemporaryDirectory() as temporary:
            for name, state in (("FL01", "complete"), ("FL10", "pending"), ("FL14", "blocked")):
                output = io.StringIO()
                with self.subTest(name=name), patch("sys.stdout", output):
                    self.assertEqual(main(["replay", name, "--out", str(Path(temporary) / name)]), 0)
                summary = json.loads(output.getvalue())
                self.assertTrue(summary["candidate_passed"])
                self.assertEqual(summary["candidate_outcomes"][0]["state"], state)
                if state == "pending":
                    self.assertEqual(summary["candidate_outcomes"][0]["next_due"], 30)
                    self.assertEqual(summary["candidate_outcomes"][0]["remaining"], ["calendar", "notifications"])

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
                       lambda s: s["initial"].update(unexpected="note"),
                       lambda s: s["schedule"][0].update(event={}),
                       lambda s: s.update(mutations=[{"variant": {}, "target": {}}]),
                       lambda s: s["schedule"].append({"token_id": "R", "action": "restart_worker",
                           "after": {"point": "receipt_durable", "event_id": {}}, "pinned": False}),
                       lambda s: s["reduction"].update(pinned_facts=[{}]),
                       lambda s: s["reduction"].update(required_fault_ids=[{}]),
                       lambda s: s["schedule"][0].update(pinned=False),
                       lambda s: s["facts"]["E1"].update(lead_id="../../file")):
            scenario = fixture("FL01")
            mutate(scenario)
            with self.assertRaises(ScenarioError):
                validate(scenario)

    def test_conflicting_delivery_id_is_rejected_before_execution(self):
        from faultleaf.scenario import validate
        scenario = fixture("FL16")
        scenario["schedule"][1]["delivery_id"] = "D1"
        with self.assertRaisesRegex(ScenarioError, "delivery_id is reused"):
            validate(scenario)


if __name__ == "__main__":
    unittest.main()
