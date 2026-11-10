"""Logical-clock replay with explicit semantic barriers."""

from contextlib import ExitStack
from copy import deepcopy
import hashlib
from pathlib import Path
import tempfile
import threading

from .oracle import check_retry_policy, evaluate
from .scenario import VARIANTS, validate
from .services import HTTPTransport, MemoryTransport, Sandbox, ServiceError, WorkerCrash, canonical, open_stores
from .worker import Worker


class ReplayError(RuntimeError):
    pass


class Engine:
    def __init__(self, scenario, variant, directory, stack, http=False):
        self.scenario, self.variant, self.directory = scenario, variant, directory
        self.timeline, self.tick, self.fired, self.restart_requested = [], 0, set(), False
        self.started = False
        self.harness_error = None
        self.known_versions = {}
        for rows in scenario["initial"].values():
            for item in rows:
                self.known_versions[item["lead_id"]] = max(item["version"], self.known_versions.get(item["lead_id"], -1))
        self.lock = threading.RLock()
        self.stores = open_stores(stack, directory, scenario["initial"], scenario.get("faults", []),
                                  self.record, self.barrier)
        self.initial_state = {service: store.snapshot() for service, store in self.stores.items()}
        if http:
            sandbox = stack.enter_context(Sandbox(self.stores))
            self.transport = HTTPTransport(sandbox.urls)
        else:
            self.transport = MemoryTransport(self.stores)
        self.worker = Worker(directory / "worker.db", self.transport, variant, self.record, self.barrier)
        stack.callback(lambda: self.worker.close())

    def record(self, kind, **kwargs):
        with self.lock:
            entry = {"seq": len(self.timeline) + 1, "tick": self.tick, "kind": kind, **kwargs}
            self.timeline.append(deepcopy(entry))
            return deepcopy(entry)

    def barrier(self, anchor):
        for step in self.scenario["schedule"]:
            if step["token_id"] in self.fired or "after" not in step:
                continue
            if all(anchor.get(k) == v for k, v in step["after"].items()):
                self.action(step)

    def action(self, step):
        self.fired.add(step["token_id"])
        self.record("schedule_action", detail={"token_id": step["token_id"], "action": step["action"]})
        if step["action"] in ("deliver", "manual_status"):
            fact = self.scenario["facts"][step["event"]]
            if (step["action"] == "manual_status" and
                    fact["version"] <= self.known_versions.get(fact["lead_id"], -1)):
                self.reject_operator(step)
                return
            self.known_versions[fact["lead_id"]] = max(fact["version"], self.known_versions.get(fact["lead_id"], -1))
            self.record("fact_activated", lead_id=fact["lead_id"], version=fact["version"],
                        detail={"event_id": fact["event_id"], "type": fact["type"]})
            if step["action"] == "manual_status":
                try:
                    self.stores["crm"].manual(fact, self.tick)
                except ServiceError:
                    self.reject_operator(step)
                    return
                if self.variant == "M04_no_manual_fence":
                    return
            # External delivery IDs cannot contain ':', so operator receipts have
            # a separate namespace and cannot conflict with a valid webhook ID.
            self.worker.receive(fact, step.get("delivery_id", "operator:" + fact["event_id"]), self.tick)
        elif step["action"] == "restart_worker":
            self.restart_requested = True

    def reject_operator(self, step):
        # Propagate authored schedule errors on the main replay thread. A barrier
        # failure must not masquerade as the HTTP client's stale-write response.
        self.harness_error = f"Operator action {step['token_id']} must advance the known lead version"
        self.record("schedule_rejected", detail={"token_id": step["token_id"], "reason": self.harness_error})

    def restart(self, reason):
        self.worker.close()
        self.worker = Worker(self.directory / "worker.db", self.transport, self.variant, self.record, self.barrier)
        self.worker.reset_backoff(self.tick)
        self.restart_requested = False
        self.record("worker_reopened", detail={"reason": reason, "boundary": "new_instance_same_sqlite"})

    def run(self):
        crash_due = None
        start = self.tick + 1 if self.started else 0
        self.started = True
        for self.tick in range(start, self.scenario["horizon"] + 1):
            if crash_due is not None and self.tick >= crash_due:
                self.restart("after_uncheckpointed_effect")
                crash_due = None
            for step in self.scenario["schedule"]:
                if step.get("at") == self.tick and step["token_id"] not in self.fired:
                    self.action(step)
            if self.harness_error:
                raise ReplayError(self.harness_error)
            if self.restart_requested:
                self.restart("authored_schedule")
            if crash_due is not None:
                continue
            for _ in range(1024):
                try:
                    progressed = self.worker.step(self.tick)
                    if self.harness_error:
                        raise ReplayError(self.harness_error)
                except WorkerCrash:
                    crash_due = self.tick + 1
                    break
                if self.restart_requested:
                    self.restart("authored_schedule")
                if not progressed:
                    break
            else:
                raise ReplayError("Replay exceeded per-tick operation bound")
            if self.harness_error:
                raise ReplayError(self.harness_error)
        unfired = [s["token_id"] for s in self.scenario["schedule"] if s["token_id"] not in self.fired]
        reached = {e["detail"]["id"] for e in self.timeline if e["kind"] == "fault_injected"}
        unreached_faults = [f["id"] for f in self.scenario["faults"] if f["id"] not in reached]
        valid = not unfired and not unreached_faults
        state = self.transport.snapshot()
        claims = self.worker.claims()
        violations = evaluate(self.scenario, state, claims, self.timeline)
        retry_policy = check_retry_policy(state, claims, self.timeline, self.scenario["horizon"])
        result = {"variant": self.variant, "engine": self.transport.name,
                  "passed": valid and not violations and retry_policy["passed"], "retry_policy": retry_policy,
                  "valid": valid, "unreached_faults": unreached_faults,
                  "initial_state": self.initial_state, "state": state, "claims": claims,
                  "timeline": self.timeline, "violations": violations,
                  "unreached_tokens": unfired, "restart_scope": "worker_instance_reopen; not OS process evidence"}
        result["canonical_digest"] = hashlib.sha256(canonical(result).encode()).hexdigest()
        return result


def replay(scenario, variant="candidate", *, http=False, directory=None):
    scenario = validate(scenario)
    if variant not in VARIANTS:
        raise ValueError("Unknown workflow variant")
    with ExitStack() as stack:
        if directory is None:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="faultleaf-")))
        else:
            directory = Path(directory)
            if directory.exists() and any(directory.iterdir()):
                raise ReplayError("Replay output store must be empty")
            directory.mkdir(parents=True, exist_ok=True)
        return Engine(scenario, variant, directory, stack, http).run()


def compare(scenario, variant=None, *, http=False):
    scenario = validate(scenario)
    if variant is None:
        variant = scenario.get("mutations", [{}])[0].get("variant", "candidate") if scenario.get("mutations") else "candidate"
    return {"schema_version": 1, "scenario": {k: scenario.get(k) for k in ("id", "title", "description", "horizon")},
            "synthetic": True, "runs": {"unsafe": replay(scenario, variant, http=http),
                                         "candidate": replay(scenario, "candidate", http=http)}}
