"""Exercise the fixed n8n graph against actual Faultleaf service stores."""

from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
import tempfile

from .oracle import evaluate
from .scenario import intent, load
from .services import HTTPTransport, Sandbox, open_stores


def check_n8n(executable, work_dir):
    from .n8n import N8N_VERSION, N8nCLI, N8nCLIError, N8nExecutionError

    root = Path(work_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    results = []
    cases = (("clean_duplicate", "FL01", (0, 1)), ("lost_crm_ack", "FL04", (0, 1)),
             ("calendar_429", "FL08", (0, 1, 4)), ("notification_429", "FL09", (0, 4)),
             ("operator_priority", "FL22", (0,)), ("operator_close", "FL01", (0, 1)))
    for name, fixture, ticks in cases:
        scenario = load(Path(__file__).parent / "scenarios" / f"{fixture}.yaml")
        timeline, current = [], {}

        def record(kind, **kwargs):
            entry = {"seq": len(timeline) + 1, "kind": kind, "tick": current.get("tick", 0), **kwargs}
            timeline.append(deepcopy(entry))
            return entry

        with tempfile.TemporaryDirectory(prefix="n8n-" + name + "-", dir=root) as temp, ExitStack() as stack:
            directory = Path(temp)
            stores = open_stores(stack, directory, scenario["initial"], scenario["faults"], record, lambda _a: None)
            sandbox = stack.enter_context(Sandbox(stores, lambda: deepcopy(current)))
            transport = HTTPTransport(sandbox.urls)
            adapter = N8nCLI(executable, cwd=directory / "runtime", timeout=120)
            attempts, claims = [], []
            for tick in ticks:
                current.update(intent={**intent(scenario["facts"]["E1"]), "closed": False},
                               event_id="E1", tick=tick, services=sandbox.urls)
                if name == "operator_close" and tick == 1:
                    closing = load(Path(__file__).parent / "scenarios" / "FL15.yaml")
                    fact = closing["facts"]["M_close2"]
                    record("fact_activated", lead_id="L1", version=2, detail={"event_id": "M_close2"})
                    stores["crm"].manual(fact, tick)
                    current.update(intent={**intent(fact), "closed": True}, event_id="M_close2")
                try:
                    execution = adapter.run(sandbox.urls, sandbox.urls["crm"] + "/delivery")
                    outcome = {"success": True, "execution": execution}
                    version = current["intent"]["version"]
                    state = "blocked" if current["intent"]["closed"] else "complete"
                    claims = [{"lead_id": "L1", "version": version, "state": state}]
                    record("worker_checkpoint", lead_id="L1", version=version, detail={"state": state})
                except (N8nCLIError, N8nExecutionError) as exc:
                    outcome = {"success": False, "error": type(exc).__name__}
                    claims = [{"lead_id": "L1", "version": 1, "state": "pending"}]
                attempts.append({"tick": tick, **outcome, "cli_runs": deepcopy(adapter.last_runs),
                                 "observed_state": transport.snapshot()})
            expected = scenario
            if name == "operator_close":
                expected = load(Path(__file__).parent / "scenarios" / "FL15.yaml")
            snapshot = transport.snapshot()
            violations = evaluate(expected, snapshot, claims, timeline)
            success_pattern = [a["success"] for a in attempts]
            wanted = {"clean_duplicate": [True, True], "lost_crm_ack": [False, True],
                      "calendar_429": [False, False, True], "notification_429": [False, True],
                      "operator_priority": [True], "operator_close": [True, True]}[name]
            stopped_downstream = True
            if name in ("lost_crm_ack", "calendar_429"):
                stopped_downstream = all(not a["observed_state"]["notifications"]["effects"]
                                         for a in attempts[:-1])
            passed = not violations and success_pattern == wanted and stopped_downstream
            results.append({"case": name, "passed": passed, "expected_success_pattern": wanted,
                            "actual_success_pattern": success_pattern, "violations": violations,
                            "claims": claims, "state": snapshot, "timeline": timeline, "attempts": attempts})
    return {"schema_version": 1, "passed": all(r["passed"] for r in results), "runtime": "n8n",
            "required_version": N8N_VERSION, "scope": "fixed five-node graph and six real service-state cases",
            "cases": results}
