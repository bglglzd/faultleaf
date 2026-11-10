"""Boundary tests use synthetic CLI output/processes, not a real n8n runtime."""

from importlib.resources import files
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from faultleaf.n8n import (
    EXPECTED_NODES,
    N8nCLI,
    N8nCLIError,
    N8nExecutionError,
    instantiate_workflow,
    parse_execution,
)

SERVICES = {
    name: "http://127.0.0.1:" + str(port)
    for name, port in [("crm", 31001), ("calendar", 31002), ("notifications", 31003)]
}
DELIVERY = "http://127.0.0.1:31004/delivery"


def successful_execution():
    runs = {}
    for name in EXPECTED_NODES:
        item = (
            {}
            if name in EXPECTED_NODES[:2]
            else {"statusCode": 200, "body": {"item": {"lead_id": "L1"}, "applied": True, "replayed": False}}
        )
        runs[name] = [{"executionStatus": "success", "data": {"main": [[{"json": item}]]}}]
    return {
        "mode": "cli",
        "status": "success",
        "finished": True,
        "data": {
            "resultData": {"lastNodeExecuted": EXPECTED_NODES[-1], "runData": runs},
            "resumeToken": "do-not-return-this-operational-token",
        },
    }


class WorkflowTests(unittest.TestCase):
    def test_fixed_graph_uses_original_delivery_and_pins_service_authorities(self):
        source = files("faultleaf").joinpath("workflows/lead_to_appointment.json").read_bytes()
        first = instantiate_workflow(SERVICES, DELIVERY)
        self.assertEqual([node["name"] for node in first["nodes"]], list(EXPECTED_NODES))
        self.assertEqual(first["nodes"][1]["parameters"]["url"], DELIVERY)
        for node, name in zip(first["nodes"][2:], SERVICES):
            self.assertEqual(node["type"], "n8n-nodes-base.httpRequest")
            self.assertEqual(node["typeVersion"], 4.5)
            self.assertFalse(node["retryOnFail"])
            params = node["parameters"]
            self.assertIn(json.dumps(SERVICES[name]), params["url"])
            self.assertNotIn(".services.", params["url"])
            self.assertIn("encodeURIComponent", params["url"])
            self.assertIn("$('Load delivery').item.json.intent", params["jsonBody"])
            if name == "crm":
                self.assertEqual(params["method"], "PUT")
            else:
                self.assertIn("'DELETE' : 'PUT'", params["method"])
                self.assertIn(".closed ? $('Load delivery').item.json.intent :", params["jsonBody"])
                self.assertIn("$('CRM write').item.json.body.items[0].id", params["jsonBody"])
            if name == "notifications":
                self.assertIn("$('Calendar write').item.json.body.items[0].id", params["jsonBody"])
            headers = {v["name"]: v["value"] for v in params["headerParameters"]["parameters"]}
            self.assertIn(name + ":", headers["Idempotency-Key"])
            self.assertIn(".event_id", headers["X-Faultleaf-Event"])
            self.assertIn(".tick", headers["X-Faultleaf-Tick"])
        first["nodes"][2]["parameters"]["url"] = "mutated"
        second = instantiate_workflow(SERVICES, DELIVERY)
        self.assertNotEqual(second["nodes"][2]["parameters"]["url"], "mutated")
        self.assertEqual(
            files("faultleaf").joinpath("workflows/lead_to_appointment.json").read_bytes(), source
        )

    def test_urls_cannot_embed_credentials_expressions_or_external_authorities(self):
        bad = [
            "https://127.0.0.1:1",
            "http://example.com:80",
            "http://localhost:80",
            "http://user:secret@127.0.0.1:1",
            "http://127.0.0.1:999999",
            "http://127.0.0.1:1/?key=x",
            "http://127.0.0.1:1/#x",
            "http://127.0.0.1:1/?",
            "http://127.0.0.1:1/#",
            "http://127.0.0.1:1/{{evil}}",
            "http://127.0.0.1:1/\n",
        ]
        for url in bad:
            with self.subTest(url=url), self.assertRaises(ValueError):
                instantiate_workflow({**SERVICES, "crm": url}, DELIVERY)
        with self.assertRaises(ValueError):
            instantiate_workflow({**SERVICES, "extra": "http://127.0.0.1:1"}, DELIVERY)
        with self.assertRaises(ValueError):
            instantiate_workflow(SERVICES, "http://127.0.0.1:1/not-delivery")


class ExecutionTests(unittest.TestCase):
    def test_real_shape_with_startup_json_and_operational_token_redaction(self):
        payload = successful_execution()
        text = '[license SDK] Startup\n{"diagnostic":true}\n' + json.dumps(payload) + "\nFinished\n"
        result = parse_execution(text)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["data"]["resumeToken"], "[REDACTED]")
        self.assertNotIn("do-not-return", json.dumps(result))

    def test_failed_partial_skipped_and_continue_on_fail_cannot_pass(self):
        variants = []
        p = successful_execution()
        p["status"] = "error"
        variants.append(p)
        p = successful_execution()
        p["finished"] = False
        variants.append(p)
        p = successful_execution()
        p["data"]["resultData"]["error"] = {}
        variants.append(p)
        p = successful_execution()
        del p["data"]["resultData"]["runData"]["Calendar write"]
        variants.append(p)
        p = successful_execution()
        p["data"]["resultData"]["lastNodeExecuted"] = "CRM write"
        variants.append(p)
        for name, mutate in [
            ("skipped", lambda entry: entry.update(executionStatus="skipped")),
            ("error-item", lambda entry: entry["data"]["main"][0][0].update(json={"error": "failed"})),
            ("http429", lambda entry: entry["data"]["main"][0][0]["json"].update(statusCode=429)),
            (
                "extra-error-output",
                lambda entry: entry["data"].update(error=[[{"json": {"error": "failed"}}]]),
            ),
        ]:
            p = successful_execution()
            mutate(p["data"]["resultData"]["runData"]["CRM write"][0])
            variants.append(p)
        for payload in variants:
            with self.subTest(payload=payload), self.assertRaises(N8nExecutionError):
                parse_execution(json.dumps(payload))

    def test_multiple_wrapped_malformed_and_truncated_records_are_rejected(self):
        record = json.dumps(successful_execution())
        for text in [
            record + record,
            json.dumps({"wrapper": successful_execution()}),
            record[:-2],
            '{"data":{"resultData":oops}}',
            record + '\n{"data":{"resultData":',
        ]:
            with self.subTest(text=text[:30]), self.assertRaises(N8nExecutionError):
                parse_execution(text)
        with self.assertRaises(N8nExecutionError):
            parse_execution(record.replace('"finished": true', '"extra": 1e400, "finished": true'))


FAKE_CLI = r"""
import json,os,pathlib,sys,time
args=sys.argv[1:]
with open('argv-log.jsonl','a') as log:log.write(json.dumps(args)+'\n')
if args==['--version']:
 print('2.46.7')
elif args[0]=='import:workflow':
 path=pathlib.Path(args[1].removeprefix('--input=')); data=json.loads(path.read_text())
 assert len(data['nodes'])==5
 assert os.environ['DB_TYPE']=='sqlite' and os.environ['DB_SQLITE_DATABASE']=='database.sqlite'
 assert 'GITLAB_TOKEN' not in os.environ and 'HTTP_PROXY' not in os.environ
 assert 'NODE_OPTIONS' not in os.environ
 print('Successfully imported 1 workflow.')
else:
 mode=pathlib.Path('mode.txt').read_text() if pathlib.Path('mode.txt').exists() else 'success'
 if mode=='timeout':time.sleep(5)
 if mode=='overflow':print('x'*100000);sys.exit(0)
 record=json.loads(pathlib.Path('record.json').read_text())
 if mode=='failed-zero-exit':record['data']['resultData']['error']={'message':'failed'}
 if mode=='nonzero':
  print('Authorization: Bearer reflected-marker',file=sys.stderr)
  print('{"resumeToken":"not-for-logs"}')
  sys.exit(2)
 print(json.dumps(record))
"""


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="faultleaf argv & spaces ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.script = self.root / "fake cli.py"
        self.script.write_text(FAKE_CLI)
        (self.root / "record.json").write_text(json.dumps(successful_execution()))

    def cli(self, **kwargs):
        return N8nCLI([sys.executable, str(self.script)], cwd=self.root, **kwargs)

    def test_real_process_argv_and_owned_environment_are_safe(self):
        with patch.dict(
            os.environ,
            {
                "GITLAB_TOKEN": "never-forward",
                "HTTP_PROXY": "http://not-used",
                "NODE_OPTIONS": "--not-used",
                "DB_TYPE": "postgresdb",
            },
        ):
            runner = self.cli()
            result = runner.run(SERVICES, DELIVERY)
        self.assertTrue(result["finished"])
        args = [json.loads(line) for line in (self.root / "argv-log.jsonl").read_text().splitlines()]
        self.assertEqual(args[0], ["--version"])
        self.assertEqual(args[1][0], "import:workflow")
        self.assertEqual(len(args[1]), 2)
        self.assertEqual(args[2], ["execute", "--id=flLeadFlow2026111", "--rawOutput"])
        self.assertFalse((self.root / ".faultleaf-n8n.lock").exists())
        self.assertFalse(list(self.root.glob("faultleaf-n8n-*.json")))
        self.assertNotIn("do-not-return", json.dumps(runner.last_runs))

    def test_failed_execution_with_zero_exit_is_not_completion(self):
        (self.root / "mode.txt").write_text("failed-zero-exit")
        with self.assertRaises(N8nCLIError) as failure:
            self.cli().run(SERVICES, DELIVERY)
        self.assertEqual(failure.exception.stage, "execute")
        self.assertEqual(failure.exception.returncode, 0)
        self.assertFalse((self.root / ".faultleaf-n8n.lock").exists())

    def test_nonzero_exception_redacts_both_outputs(self):
        (self.root / "mode.txt").write_text("nonzero")
        with self.assertRaises(N8nCLIError) as failure:
            self.cli(redact_values=["reflected-marker"]).run(SERVICES, DELIVERY)
        self.assertNotIn("not-for-logs", failure.exception.stdout)
        self.assertNotIn("reflected-marker", failure.exception.stderr)
        self.assertIn("[REDACTED]", failure.exception.stdout)

    def test_output_limit_timeout_and_existing_lock_fail_closed(self):
        for mode, options in [("overflow", {"max_output_bytes": 1024}), ("timeout", {"timeout": 0.5})]:
            (self.root / "mode.txt").write_text(mode)
            with self.subTest(mode=mode), self.assertRaises(N8nCLIError):
                self.cli(**options).run(SERVICES, DELIVERY)
            self.assertFalse((self.root / ".faultleaf-n8n.lock").exists())
        lock = self.root / ".faultleaf-n8n.lock"
        lock.write_text("existing-owner")
        with self.assertRaises(N8nCLIError):
            self.cli().run(SERVICES, DELIVERY)
        self.assertEqual(lock.read_text(), "existing-owner")

    def test_data_directory_cannot_escape_owned_cwd(self):
        with self.assertRaises(ValueError):
            self.cli(data_dir=self.root.parent)
        self.assertEqual(self.cli(data_dir="relative-data").data_dir, self.root / "relative-data")


if __name__ == "__main__":
    unittest.main()
