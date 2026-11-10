"""Inspectable synthetic APIs; successful acknowledgments are not the oracle."""

from copy import deepcopy
from contextlib import ExitStack
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import sqlite3
import threading
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import Request, urlopen

from .scenario import OPERATIONS, SERVICES


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class ServiceError(Exception):
    def __init__(self, status, message, retry_after=1):
        super().__init__(message)
        self.status, self.retry_after = status, retry_after


class WorkerCrash(Exception):
    """A committed write was not checkpointed by its caller."""


class Store:
    def __init__(self, service, path, initial, faults, record, barrier):
        self.service, self.record, self.barrier = service, record, barrier
        self.faults = [deepcopy(f) for f in faults if f["service"] == service]
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        try:
            self._initialize(initial)
        except BaseException:
            self.db.close()
            raise

    def _initialize(self, initial):
        self.db.execute("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT)")
        row = self.db.execute("SELECT body FROM state WHERE id=1").fetchone()
        if row:
            self.data = json.loads(row[0])
        else:
            self.data = {"items": [], "effects": [], "attempts": [], "keys": {}, "windows": {}}
            for item in initial:
                new = deepcopy(item)
                new.setdefault("display_name", "Alex Example")
                new.setdefault("operator_tags", [])
                new.setdefault("status", "open")
                new.setdefault("slot", "A")
                new.update(id=f"{self.service}-{len(self.data['items']) + 1}", active=True)
                self.data["items"].append(new)
            self._save()

    def _save(self):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO state VALUES (1, ?)", (canonical(self.data),))

    def close(self):
        self.db.close()

    def snapshot(self):
        with self.lock:
            return deepcopy({key: self.data[key] for key in ("items", "effects", "attempts")})

    def get(self, lead):
        with self.lock:
            return deepcopy([i for i in self.data["items"] if i["lead_id"] == lead and i["active"]])

    def manual(self, fact, tick):
        with self.lock:
            rows = [i for i in self.data["items"] if i["lead_id"] == fact["lead_id"] and i["active"]]
            before = deepcopy(rows)
            if rows and max(r["version"] for r in rows) >= fact["version"]:
                raise ServiceError(409, "Operator version must advance current state")
            if not rows:
                row = {k: fact[k] for k in ("lead_id", "version", "slot", "display_name", "status")}
                row.update(id=f"{self.service}-{len(self.data['items']) + 1}", active=True, operator_tags=[])
                self.data["items"].append(row)
            else:
                for row in rows:
                    row.update(version=fact["version"], status=fact["status"])
            entry = self.record("operator_edit", service=self.service, lead_id=fact["lead_id"],
                                version=fact["version"], tick=tick, detail={"status": fact["status"]})
            self.data["effects"].append({**entry, "operation": "operator_edit", "before": before,
                                         "after": deepcopy(self.get(fact["lead_id"]))})
            self._save()

    def request(self, method, body, key, event, tick, unsafe_replace=False):
        lead, version = body["lead_id"], body["version"]
        operation = OPERATIONS[self.service]
        meta = dict(service=self.service, operation=operation, lead_id=lead, version=version, tick=tick)
        fault, outcome, retry_after = None, None, 1
        with self.lock:
            occurrence = 1 + sum(a["lead_id"] == lead and a["version"] == version
                                 and a["operation"] == operation for a in self.data["attempts"])
            attempt = {**meta, "occurrence": occurrence, "event_id": event, "method": method, "key": key}
            self.data["attempts"].append(attempt)
            received = self.record("service_attempt", **meta,
                                   detail={"occurrence": occurrence, "event_id": event, "method": method, "key": key})
            attempt["seq"] = received["seq"]
            signature = canonical([lead, version, operation])
            for candidate in self.faults:
                if (candidate["lead_id"] == lead and candidate["version"] == version
                        and candidate["operation"] == operation
                        and ("through" not in candidate or tick <= candidate["through"])
                        and (candidate["occurrence"] == "all" or candidate["occurrence"] == occurrence
                             or ("through" in candidate and tick <= candidate["through"]))):
                    fault = candidate
                    break
            window = self.data["windows"].get(signature, 0)
            if tick < window:
                outcome, retry_after = "rate_limited", window - tick
            elif fault and fault["mode"] == "rate_limit_before_commit":
                retry_after = max(1, fault.get("retry_after", 1))
                self.data["windows"][signature] = tick + retry_after
                outcome = "rate_limited"
            elif fault and fault["mode"] == "unavailable_before_commit":
                outcome = "unavailable"
            if fault:
                self.record("fault_injected", **meta, detail={"id": fault["id"], "mode": fault["mode"]})
            if outcome:
                attempt["outcome"] = outcome
                self.record(outcome, **meta, detail={"retry_after": retry_after})
                self._save()
            else:
                fingerprint = hashlib.sha256(canonical([method, body]).encode()).hexdigest()
                if method != "POST" and not key:
                    attempt["outcome"] = "rejected"
                    self._save()
                    raise ServiceError(400, "PUT/DELETE require an idempotency key")
                if key and key in self.data["keys"]:
                    old = self.data["keys"][key]
                    if old["fingerprint"] != fingerprint:
                        attempt["outcome"] = "key_conflict"
                        self._save()
                        raise ServiceError(409, "Idempotency key reused with different body")
                    attempt["outcome"] = "idempotency_hit"
                    self.record("idempotency_hit", **meta, detail={"key": key})
                    self._save()
                    return deepcopy(old["response"])
                active = [i for i in self.data["items"] if i["lead_id"] == lead and i["active"]]
                before = deepcopy(active)
                if method != "POST" and active and max(i["version"] for i in active) > version:
                    attempt["outcome"] = "stale_rejected"
                    self._save()
                    raise ServiceError(409, "Stale business version")
                if method == "DELETE":
                    for item in active:
                        item["active"] = False
                    effect_kind = "cancel" if self.service == "calendar" else "withdraw"
                else:
                    new = {k: body[k] for k in ("lead_id", "version", "slot", "display_name", "status")}
                    for reference in ("crm_id", "booking_id"):
                        if reference in body:
                            new[reference] = body[reference]
                    new["operator_tags"] = list(body.get("operator_tags", []))
                    if method == "PUT" and active and self.service == "crm":
                        new["status"] = active[0]["status"]
                        new["operator_tags"] = active[0].get("operator_tags", [])
                        if new["status"] == "closed":
                            new["version"] = max(version, active[0]["version"])
                    replace = method == "PUT" or unsafe_replace
                    if replace and active:
                        for item in active:
                            item["active"] = False
                        new["id"] = active[0]["id"]
                        effect_kind = "update"
                    else:
                        new["id"] = f"{self.service}-{len(self.data['items']) + 1}"
                        effect_kind = "create"
                    new["active"] = True
                    self.data["items"].append(new)
                after = deepcopy([i for i in self.data["items"] if i["lead_id"] == lead and i["active"]])
                entry = self.record("effect_committed", **meta, detail={"effect": effect_kind, "key": key})
                self.data["effects"].append({**entry, "operation": operation, "effect": effect_kind,
                                             "before": before, "after": after})
                response = {"items": after, "effect": effect_kind}
                if key:
                    self.data["keys"][key] = {"fingerprint": fingerprint, "response": response}
                attempt["outcome"] = "committed"
                self._save()
        # Authored barriers run only after the service transaction has committed.
        self.barrier({**meta, "point": outcome or "effect_committed"})
        if outcome:
            raise ServiceError(429 if outcome == "rate_limited" else 503, outcome, retry_after)
        if fault and fault["mode"] == "timeout_after_commit":
            raise ServiceError(0, "Acknowledgment lost after commit")
        if fault and fault["mode"] == "crash_worker_after_commit_before_checkpoint":
            raise WorkerCrash()
        return response


class MemoryTransport:
    name = "deterministic-memory"

    def __init__(self, stores):
        self.stores = stores

    def get(self, service, lead):
        return self.stores[service].get(lead)

    def write(self, service, method, body, key, event, tick, unsafe_replace=False):
        return self.stores[service].request(method, body, key, event, tick, unsafe_replace)

    def snapshot(self):
        return {service: self.stores[service].snapshot() for service in SERVICES}


class _Server(ThreadingHTTPServer):
    daemon_threads = True


class Sandbox:
    """Three loopback listeners, all owned and closed by this context manager."""

    def __init__(self, stores, delivery=None):
        self.stores, self.delivery, self.servers, self.threads, self.urls = stores, delivery, [], [], {}

    def __enter__(self):
        try:
            for service in SERVICES:
                store = self.stores[service]
                outer = self

                class Handler(BaseHTTPRequestHandler):
                    service_store = store

                    def log_message(self, *_args):
                        pass

                    def reply(self, status, value, headers=None):
                        encoded = canonical(value).encode()
                        self.send_response(status)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(encoded)))
                        for k, v in (headers or {}).items():
                            self.send_header(k, str(v))
                        self.end_headers()
                        self.wfile.write(encoded)

                    def do_GET(self):
                        path = urlsplit(self.path).path
                        if path == "/state":
                            self.reply(200, self.service_store.snapshot())
                        elif path == "/delivery" and outer.delivery is not None:
                            self.reply(200, outer.delivery())
                        elif path.startswith("/items/"):
                            self.reply(200, {"items": self.service_store.get(unquote(path[7:]))})
                        else:
                            self.reply(404, {"error": "Unknown endpoint"})

                    def write_item(self):
                        try:
                            length = int(self.headers.get("Content-Length", "0"))
                            if not 0 < length <= 8192:
                                raise ValueError("Body must contain 1..8192 bytes")
                            body = json.loads(self.rfile.read(length))
                            if not isinstance(body, dict) or type(body.get("version")) is not int:
                                raise ValueError("Invalid item")
                            for key in ("lead_id", "slot", "display_name", "status"):
                                if not isinstance(body.get(key), str) or len(body[key]) > 160:
                                    raise ValueError(f"Invalid {key}")
                            if not 0 <= body["version"] <= 1_000_000 or body["status"] not in ("open", "priority", "closed"):
                                raise ValueError("Invalid version/status")
                            path = urlsplit(self.path).path
                            if ((self.command == "POST" and path != "/items") or
                                (self.command != "POST" and path != "/items/" + quote(body["lead_id"], safe=""))):
                                self.reply(400, {"error": "Item path/body mismatch"})
                                return
                            tick = int(self.headers.get("X-Faultleaf-Tick", "0"))
                            result = self.service_store.request(self.command, body,
                                self.headers.get("Idempotency-Key"), self.headers.get("X-Faultleaf-Event", ""),
                                tick, self.headers.get("X-Faultleaf-Write-Mode") == "replace")
                            self.reply(200, result)
                        except ServiceError as exc:
                            if exc.status == 0:
                                self.close_connection = True
                                self.connection.shutdown(socket.SHUT_RDWR)
                            else:
                                self.reply(exc.status, {"error": str(exc)}, {"Retry-After": exc.retry_after})
                        except WorkerCrash:
                            self.reply(503, {"error": "Crash checkpoint"}, {"X-Faultleaf-Crash": "1"})
                        except (ValueError, KeyError, TypeError):
                            self.reply(400, {"error": "Invalid bounded item request"})

                    do_PUT = do_POST = do_DELETE = write_item

                server = _Server(("127.0.0.1", 0), Handler)
                self.servers.append(server)
                self.urls[service] = f"http://127.0.0.1:{server.server_port}"
                thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
                thread.start()
                self.threads.append(thread)
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_args):
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)


class HTTPTransport:
    name = "loopback-http"

    def __init__(self, urls):
        self.urls = dict(urls)

    def _request(self, url, method="GET", body=None, headers=None):
        request = Request(url, data=canonical(body).encode() if body is not None else None,
                          method=method, headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urlopen(request, timeout=5) as response:
                return json.loads(response.read(2 * 1024 * 1024))
        except HTTPError as exc:
            if exc.headers.get("X-Faultleaf-Crash") == "1":
                raise WorkerCrash() from exc
            raise ServiceError(exc.code, f"Service HTTP {exc.code}", int(exc.headers.get("Retry-After", "1"))) from exc
        except (URLError, OSError, ConnectionError) as exc:
            raise ServiceError(0, "No acknowledgment") from exc

    def get(self, service, lead):
        return self._request(self.urls[service] + "/items/" + quote(lead, safe=""))["items"]

    def write(self, service, method, body, key, event, tick, unsafe_replace=False):
        headers = {"X-Faultleaf-Event": event, "X-Faultleaf-Tick": str(tick)}
        if key:
            headers["Idempotency-Key"] = key
        if unsafe_replace:
            headers["X-Faultleaf-Write-Mode"] = "replace"
        path = "/items" if method == "POST" else "/items/" + quote(body["lead_id"], safe="")
        return self._request(self.urls[service] + path, method, body, headers)

    def snapshot(self):
        return {service: self._request(self.urls[service] + "/state") for service in SERVICES}


def open_stores(stack: ExitStack, directory: Path, initial, faults, record, barrier):
    stores = {}
    for service in SERVICES:
        stores[service] = Store(service, directory / f"{service}.db", initial.get(service, []),
                                faults, record, barrier)
        stack.callback(stores[service].close)
    return stores
