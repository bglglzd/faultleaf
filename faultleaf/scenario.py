"""Bounded, data-only replay documents. No executable YAML extensions."""

from copy import deepcopy
from pathlib import Path
import re

import yaml

SERVICES = ("crm", "calendar", "notifications")
OPERATIONS = dict(zip(SERVICES, ("ensure_contact", "ensure_booking", "queue_confirmation")))
MAX_BYTES = 512 * 1024
VARIANTS = {
    "candidate", "M01_no_idempotency", "M01_no_business_revision_dedupe",
    "M02_fresh_key_on_retry", "M03_partial_state_is_success", "M04_no_revision_fence",
    "M04_no_manual_fence", "M04_no_effect_execution_fence", "M04_replace_whole_contact",
    "M05_volatile_pending_work", "M06_ignore_retry_after_and_exhaust_budget",
    "M06_reset_backoff_on_restart", "M07_key_by_display_name", "M07_global_step_key_or_backoff",
}


class ScenarioError(ValueError):
    """The scenario cannot be interpreted by the bounded v1 grammar."""


class _Loader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ScenarioError("YAML aliases are not supported")
        self._depth = getattr(self, "_depth", 0) + 1
        if self._depth > 24:
            raise ScenarioError("YAML nesting exceeds 24")
        try:
            return super().compose_node(parent, index)
        finally:
            self._depth -= 1


def _mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str) or key in result:
            raise ScenarioError("Mapping keys must be unique strings")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _integer(value, label, maximum=1000):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ScenarioError(f"{label} must be an integer in 0..{maximum}")


def _identifier(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", value):
        raise ScenarioError(f"{label} must be a short ASCII identifier")


def _objects(value, label, maximum):
    if not isinstance(value, list) or len(value) > maximum or any(not isinstance(x, dict) for x in value):
        raise ScenarioError(f"{label} must contain at most {maximum} objects")
    return value


def validate(document):
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ScenarioError("Expected scenario schema_version: 1")
    data = deepcopy(document)
    _identifier(data.get("id"), "id")
    if not isinstance(data.get("title"), str) or len(data["title"]) > 160:
        raise ScenarioError("title must be text of at most 160 characters")
    _integer(data.get("horizon"), "horizon")
    _integer(data.get("seed", 0), "seed", 2**31 - 1)
    facts = data.get("facts")
    if not isinstance(facts, dict) or not 1 <= len(facts) <= 32:
        raise ScenarioError("facts must contain 1..32 immutable events")
    business = {}
    for event_id, fact in facts.items():
        _identifier(event_id, "fact ID")
        if not isinstance(fact, dict):
            raise ScenarioError("Each fact must be an object")
        fact.setdefault("event_id", event_id)
        if fact["event_id"] != event_id:
            raise ScenarioError("fact key and event_id differ")
        _identifier(fact.get("lead_id"), "lead_id")
        _integer(fact.get("version"), "version", 1_000_000)
        if fact.get("type") not in ("request", "operator"):
            raise ScenarioError("fact type must be request or operator")
        if fact.get("status", "open") not in ("open", "priority", "closed"):
            raise ScenarioError("Unknown status")
        for key in ("slot", "display_name"):
            if not isinstance(fact.get(key), str) or len(fact[key]) > 160:
                raise ScenarioError(f"{key} must be short text")
        semantic = {k: v for k, v in fact.items() if k != "event_id"}
        identity = (fact["lead_id"], fact["version"])
        if identity in business and business[identity] != semantic:
            raise ScenarioError("Conflicting facts share one lead/version")
        business[identity] = semantic
    seen = set()
    for step in _objects(data.get("schedule"), "schedule", 64):
        _identifier(step.get("token_id"), "token_id")
        if step["token_id"] in seen:
            raise ScenarioError("Duplicate schedule token")
        seen.add(step["token_id"])
        if step.get("action") not in ("deliver", "manual_status", "restart_worker", "wake"):
            raise ScenarioError("Unsupported schedule action")
        if ("at" in step) == ("after" in step):
            raise ScenarioError("Each token needs exactly one at/after anchor")
        if "at" in step:
            _integer(step["at"], "schedule tick", data["horizon"])
        else:
            if not isinstance(step["after"], dict) or not step["after"]:
                raise ScenarioError("after must be a nonempty semantic barrier")
        if step["action"] in ("deliver", "manual_status"):
            if step.get("event") not in facts:
                raise ScenarioError("Schedule references an unknown fact")
        if step["action"] == "deliver":
            _identifier(step.get("delivery_id"), "delivery_id")
    seen = set()
    for fault in _objects(data.get("faults", []), "faults", 24):
        _identifier(fault.get("id"), "fault id")
        if fault["id"] in seen:
            raise ScenarioError("Duplicate fault ID")
        seen.add(fault["id"])
        if fault.get("service") not in SERVICES or fault.get("operation") != OPERATIONS[fault["service"]]:
            raise ScenarioError("Fault service/operation mismatch")
        _identifier(fault.get("lead_id"), "fault lead")
        _integer(fault.get("version"), "fault version", 1_000_000)
        if fault.get("occurrence") != "all":
            _integer(fault.get("occurrence"), "fault occurrence", 10_000)
            if fault["occurrence"] == 0:
                raise ScenarioError("Fault occurrence starts at 1")
        if fault.get("mode") not in ("timeout_after_commit", "rate_limit_before_commit",
                                     "unavailable_before_commit", "crash_worker_after_commit_before_checkpoint"):
            raise ScenarioError("Unsupported fault mode")
        for key in ("retry_after", "through"):
            if key in fault:
                _integer(fault[key], key, 10_000)
    initial = data.get("initial")
    expected = data.get("expected")
    if not isinstance(initial, dict) or not isinstance(expected, dict):
        raise ScenarioError("initial and expected must be objects")
    for service in SERVICES:
        for row in _objects(initial.get(service, []), "initial service rows", 32):
            _identifier(row.get("lead_id"), "initial lead")
            _integer(row.get("version"), "initial version", 1_000_000)
        _objects(expected.get("services", {}).get(service, []), "expected service rows", 32)
    _objects(expected.get("claims", []), "expected claims", 32)
    for mutation in _objects(data.get("mutations", []), "mutations", 8):
        if mutation.get("variant") not in VARIANTS - {"candidate"} or not isinstance(mutation.get("target"), dict):
            raise ScenarioError("Unknown mutation or missing target signature")
    reduction = data.setdefault("reduction", {"eligible": False, "required_fault_ids": [], "pinned_facts": []})
    if not isinstance(reduction, dict) or not set(reduction.get("required_fault_ids", [])) <= seen:
        raise ScenarioError("Reduction requires an unknown fault")
    return data


def loads(text):
    if not isinstance(text, str) or len(text.encode("utf-8")) > MAX_BYTES:
        raise ScenarioError("Scenario exceeds 512 KiB")
    try:
        return validate(yaml.load(text, Loader=_Loader))
    except (yaml.YAMLError, RecursionError, TypeError) as exc:
        raise ScenarioError(f"Invalid YAML: {type(exc).__name__}") from exc


def load(path):
    path = Path(path)
    if path.stat().st_size > MAX_BYTES:
        raise ScenarioError("Scenario exceeds 512 KiB")
    return loads(path.read_text(encoding="utf-8"))


def examples():
    return sorted((Path(__file__).parent / "scenarios").glob("*.yaml"))


def intent(fact):
    return {key: fact[key] for key in ("lead_id", "version", "slot", "display_name", "status")}
