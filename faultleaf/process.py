"""A task-owned subprocess boundary for crash-before-checkpoint verification."""

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from .oracle import evaluate
from .replay import Engine
from .scenario import load
from .services import HTTPTransport, WorkerCrash, canonical
from .worker import Worker


def _child(job_path):
    job = json.loads(Path(job_path).read_text(encoding="utf-8"))
    marker = Path(job["marker"])
    marker.write_text(canonical({"pid": os.getpid(), "tick": job["tick"]}), encoding="utf-8")
    events = []

    def record(kind, **kwargs):
        events.append({"kind": kind, **kwargs})

    worker = Worker(job["database"], HTTPTransport(job["urls"]), "candidate", record, lambda _anchor: None)
    try:
        for _ in range(64):
            if not worker.step(job["tick"]):
                break
        else:
            raise RuntimeError("Child operation limit exceeded")
        result = {"pid": os.getpid(), "claims": worker.claims(), "events": events}
        print(canonical(result))
    except WorkerCrash:
        # Deliberate process termination: no Python cleanup can persist a missed checkpoint.
        os._exit(73)
    finally:
        worker.close()


def verify_process_recovery(scenario_id="FL19"):
    if scenario_id not in ("FL19", "FL21"):
        raise ValueError("Separate process proof supports FL19 and FL21")
    scenario = load(Path(__file__).parent / "scenarios" / f"{scenario_id}.yaml")
    with tempfile.TemporaryDirectory(prefix="faultleaf-process-") as temporary, ExitStack() as stack:
        directory = Path(temporary)
        engine = Engine(scenario, "candidate", directory, stack, http=True)
        engine.worker.receive(scenario["facts"]["E1"], "D1", 0)
        engine.worker.close()
        results = []
        for tick in (0, 1):
            job = {"database": str(directory / "worker.db"), "urls": engine.transport.urls,
                   "tick": tick, "marker": str(directory / f"child-{tick}.json")}
            path = directory / f"job-{tick}.json"
            path.write_text(canonical(job), encoding="utf-8")
            completed = subprocess.run([sys.executable, "-m", "faultleaf.process", "--child", str(path)],
                cwd=directory, capture_output=True, text=True, timeout=30,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            marker = json.loads(Path(job["marker"]).read_text(encoding="utf-8"))
            result = {**marker, "returncode": completed.returncode}
            if tick == 0:
                if completed.returncode != 73:
                    raise RuntimeError("First worker did not terminate at its uncheckpointed effect")
            else:
                if completed.returncode != 0:
                    raise RuntimeError("Recovery worker failed")
                result["output"] = json.loads(completed.stdout)
            results.append(result)
        engine.worker = Worker(directory / "worker.db", engine.transport, "candidate", engine.record, engine.barrier)
        state, claims = engine.transport.snapshot(), engine.worker.claims()
        violations = evaluate(scenario, state, claims, engine.timeline)
        passed = not violations and results[0]["pid"] != results[1]["pid"] and claims[0]["state"] == "complete"
        return {"scenario": scenario_id, "engine": "two_distinct_OS_processes_loopback_HTTP_SQLite",
                "passed": passed, "processes": results, "state": state, "claims": claims,
                "violations": violations, "timeline": engine.timeline}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", required=True)
    args = parser.parse_args()
    _child(args.child)


if __name__ == "__main__":
    main()
