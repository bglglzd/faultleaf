import copy
import json
import tempfile
import unittest
from html.parser import HTMLParser
from pathlib import Path

from faultleaf.report import render_report, write_report


def example_report():
    empty = {"items": [], "effects": [], "attempts": []}
    candidate = {
        "variant": "candidate",
        "engine": "python",
        "passed": True,
        "violations": [],
        "state": {name: copy.deepcopy(empty) for name in ("crm", "calendar", "notifications")},
        "claims": [
            {
                "lead_id": "L1",
                "version": 1,
                "state": "pending",
                "phase": "calendar",
                "next_due": 4,
                "attempts": 1,
            }
        ],
        "timeline": [
            {
                "seq": 1,
                "tick": 0,
                "kind": "attempt",
                "service": "calendar",
                "lead_id": "L1",
                "version": 1,
                "detail": {"status": 429, "next_due": 4},
            }
        ],
        "canonical_digest": "a" * 64,
    }
    unsafe = {
        **copy.deepcopy(candidate),
        "variant": "unsafe",
        "passed": False,
        "violations": [
            {
                "class": "false_success",
                "rule": "complete_requires_effects",
                "service": "calendar",
                "lead_id": "L1",
                "version": 1,
                "expected": "A booking before completion",
                "actual": "No booking",
                "witness": {"state": "calendar.items", "seq": 1},
            }
        ],
        "claims": [
            {
                "lead_id": "L1",
                "version": 1,
                "state": "complete",
                "phase": "calendar",
                "next_due": None,
                "attempts": 1,
            }
        ],
    }
    return {
        "schema_version": 1,
        "scenario": {
            "id": "rate-limit",
            "title": "Calendar backoff",
            "description": "A 429 delays work.",
            "horizon": 10,
        },
        "runs": {"unsafe": unsafe, "candidate": candidate},
    }


class EmbeddedData(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_data = False
        self.payload = []
        self.external_resources = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script" and attrs.get("id") == "report-data":
            self.in_data = True
        if tag in ("script", "img", "iframe", "link"):
            url = attrs.get("src", attrs.get("href", ""))
            if url.startswith(("http:", "https:", "//")):
                self.external_resources.append(url)

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_data = False

    def handle_data(self, data):
        if self.in_data:
            self.payload.append(data)


class ReportTests(unittest.TestCase):
    def test_hostile_fields_round_trip_without_closing_the_data_script(self):
        report = example_report()
        attack = '</script><img src=x onerror="window.reportInjected=1">\u2028\u2029'
        report["scenario"]["title"] = attack
        report["runs"]["unsafe"]["violations"][0]["witness"] = {attack: attack}
        html = render_report(report)
        parsed = EmbeddedData()
        parsed.feed(html)
        self.assertEqual(json.loads("".join(parsed.payload)), report)
        self.assertNotIn(attack, html)
        self.assertNotIn("\u2028", html)
        self.assertNotIn("\u2029", html)
        self.assertIn("\\u003c/script\\u003e", html)

    def test_viewer_has_real_evidence_sections_and_no_loaded_external_resources(self):
        html = render_report(example_report())
        for title in (
            "Resulting business state",
            "Worker claims",
            "Logical timeline",
            "Invariant checks",
            "Committed effect ledger",
            "Delivery attempts",
            "Reduced scenario",
            "Raw report JSON",
        ):
            self.assertIn(title, html)
        parsed = EmbeddedData()
        parsed.feed(html)
        self.assertEqual(parsed.external_resources, [])
        self.assertNotIn("innerHTML", html)
        self.assertNotIn("document.write", html)
        self.assertNotIn("localStorage", html)
        self.assertIn("connect-src 'none'", html)
        self.assertIn('aria-label="Run variant"', html)
        self.assertIn('aria-live="polite"', html)

    def test_directory_and_named_html_write_the_same_json_evidence(self):
        report = example_report()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertIsNone(write_report(report, root / "saved"))
            self.assertEqual(json.loads((root / "saved/report.json").read_text(encoding="utf-8")), report)
            self.assertIn("report-data", (root / "saved/report.html").read_text(encoding="utf-8"))
            self.assertIsNone(write_report(report, root / "named/view.html"))
            self.assertTrue((root / "named/view.html").is_file())
            self.assertEqual(json.loads((root / "named/report.json").read_text(encoding="utf-8")), report)

    def test_nonfinite_or_oversized_report_is_not_written(self):
        for value in (float("nan"), float("inf")):
            report = example_report()
            report["untrusted_metadata"] = value
            with self.assertRaises(ValueError):
                render_report(report)
        report = example_report()
        report["untrusted_metadata"] = "x" * (2 * 1024 * 1024)
        with self.assertRaisesRegex(ValueError, "2 MiB"):
            render_report(report)

    def test_missing_optional_evidence_still_produces_a_viewable_document(self):
        report = {"schema_version": 1, "scenario": {"id": "partial"}, "runs": {"candidate": {}}}
        html = render_report(report)
        parsed = EmbeddedData()
        parsed.feed(html)
        self.assertEqual(json.loads("".join(parsed.payload)), report)
        self.assertIn("State unavailable.", html)
        self.assertIn("Worker claims unavailable.", html)


if __name__ == "__main__":
    unittest.main()
