"""Faultleaf command line."""

import argparse
import json
from pathlib import Path
import sys

from . import __version__
from .process import verify_process_recovery
from .reduce import reduce_failure
from .replay import compare
from .scenario import ScenarioError, examples, load
from .services import canonical
from .suite import run_suite


def _scenario(value):
    candidates = {p.stem: p for p in examples()}
    return load(candidates.get(value.upper(), Path(value)))


def _json_file(value, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="faultleaf", description="Replay one synthetic lead workflow and inspect business state.")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="List bundled YAML scenarios")
    replay_parser = sub.add_parser("replay", help="Compare a declared unsafe variant and durable candidate")
    replay_parser.add_argument("scenario", help="Bundled ID (FL01..FL24) or YAML path")
    replay_parser.add_argument("--out", type=Path, required=True, help="Output directory for report.html and report.json")
    replay_parser.add_argument("--variant", help="Declared executable mutation; defaults to scenario's first")
    replay_parser.add_argument("--http", action="store_true", help="Use three real loopback HTTP servers")
    replay_parser.add_argument("--reduce", action="store_true", help="Reduce eligible failure in deterministic memory mode")
    suite = sub.add_parser("suite", help="Check every candidate and exact declared mutation target")
    suite.add_argument("--out", type=Path, required=True)
    suite.add_argument("--http", action="store_true")
    suite.add_argument("--repeat", action="store_true", help="Compare repeated candidate canonical digests")
    process = sub.add_parser("process-check", help="Terminate and replace a task-owned worker process over HTTP")
    process.add_argument("--scenario", choices=("FL19", "FL21"), default="FL19")
    process.add_argument("--out", type=Path, required=True)
    n8n = sub.add_parser("n8n-check", help="Exercise six fixed-graph cases using a separately installed n8n CLI")
    n8n.add_argument("--executable", default="n8n", help="n8n executable or task-owned wrapper; no shell string")
    n8n.add_argument("--work-dir", type=Path, required=True, help="Parent for task-owned temporary n8n data")
    n8n.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "list":
            for path in examples():
                scenario = load(path)
                print(f"{scenario['id']}  {scenario['title']}")
            return 0
        if args.command == "replay":
            from .report import write_report
            scenario = _scenario(args.scenario)
            report = compare(scenario, args.variant, http=args.http)
            if args.reduce:
                report["reduction"] = reduce_failure(scenario, args.variant)
            write_report(report, args.out)
            outcomes = []
            for claim in report["runs"]["candidate"]["claims"]:
                outcome = {key: claim[key] for key in ("lead_id", "version", "state")}
                if claim["state"] == "pending":
                    outcome.update(next_due=claim["next_due"], remaining=claim["remaining"])
                outcomes.append(outcome)
            print(canonical({"candidate_passed": report["runs"]["candidate"]["passed"],
                             "candidate_outcomes": outcomes,
                             "unsafe_violations": len(report["runs"]["unsafe"]["violations"]),
                             "report": str(args.out / "report.html")}))
            # The intentionally unsafe run is expected to fail; candidate state controls this exit.
            return 0 if report["runs"]["candidate"]["passed"] else 1
        if args.command == "suite":
            result = run_suite(http=args.http, repeat=args.repeat)
        elif args.command == "process-check":
            result = verify_process_recovery(args.scenario)
        else:
            from .n8n_check import check_n8n
            result = check_n8n(args.executable, args.work_dir)
        _json_file(result, args.out)
        print(canonical({"passed": result["passed"], "output": str(args.out)}))
        return 0 if result["passed"] else 1
    except (ScenarioError, ValueError, OSError, RuntimeError) as exc:
        print(f"faultleaf: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
