"""One durable candidate and bounded, executable workflow mutations."""

from copy import deepcopy
import json
import sqlite3

from .scenario import SERVICES, intent
from .services import ServiceError, WorkerCrash, canonical


class Worker:
    def __init__(self, path, transport, variant, record, barrier):
        self.transport, self.variant, self.record, self.barrier = transport, variant, record, barrier
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS worker (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT)")
        row = self.db.execute("SELECT body FROM worker WHERE id=1").fetchone()
        self.data = json.loads(row[0]) if row and variant != "M05_volatile_pending_work" else {
            "receipts": {}, "latest": {}, "jobs": [], "global_due": 0,
        }
        self.save()

    def save(self):
        if self.variant != "M05_volatile_pending_work":
            with self.db:
                self.db.execute("INSERT OR REPLACE INTO worker VALUES (1, ?)", (canonical(self.data),))

    def close(self):
        self.db.close()

    def receive(self, fact, delivery, tick):
        self.record("delivery_received", tick=tick, lead_id=fact["lead_id"], version=fact["version"],
                    detail={"delivery_id": delivery, "event_id": fact["event_id"]})
        variant, data = self.variant, self.data
        duplicate_receipt = delivery in data["receipts"]
        payload = intent(fact)
        if duplicate_receipt and data["receipts"][delivery] != payload:
            raise ValueError("Delivery ID reused with different business content")
        if variant != "M01_no_idempotency" and duplicate_receipt:
            return
        # Receipt identity is the original delivery, before operator-state projection.
        data["receipts"][delivery] = deepcopy(payload)
        old = data["latest"].get(fact["lead_id"])
        no_version_fence = variant == "M04_no_revision_fence"
        if variant == "M07_key_by_display_name" and any(
                row["display_name"] == fact["display_name"] for row in data["latest"].values()):
            self.save()
            return
        if old and not no_version_fence:
            if old["version"] > fact["version"]:
                self.save()
                return
            if (old["version"] == fact["version"] and
                    variant not in ("M01_no_idempotency", "M01_no_business_revision_dedupe")):
                self.save()
                return
            if old["status"] == "closed" and fact["type"] != "operator":
                payload["status"] = "closed"
        data["latest"][fact["lead_id"]] = payload
        job = {"id": len(data["jobs"]) + 1, "lead_id": fact["lead_id"], "version": fact["version"],
               "intent": payload, "event_id": fact["event_id"], "phase": 0, "attempts": 0,
               "next_due": tick, "state": "pending"}
        data["jobs"].append(job)
        self.save()
        self.barrier({"point": "receipt_durable", "event_id": fact["event_id"],
                      "lead_id": fact["lead_id"], "version": fact["version"], "tick": tick})

    def reset_backoff(self, tick):
        if self.variant == "M06_reset_backoff_on_restart":
            for job in self.data["jobs"]:
                if job["state"] == "pending":
                    job["next_due"] = tick
            self.save()

    def _fenced(self, job):
        latest = self.data["latest"].get(job["lead_id"])
        if self.variant.startswith("M04_") or self.variant.startswith("M01_"):
            return False
        return latest is not None and latest != job["intent"]

    def step(self, tick):
        """Perform at most one external effect; checkpoint only after acknowledgment."""
        variant = self.variant
        for job in self.data["jobs"]:
            if job["state"] != "pending" or job["next_due"] > tick:
                continue
            if variant == "M07_global_step_key_or_backoff" and self.data["global_due"] > tick:
                continue
            if self._fenced(job):
                job["state"] = "superseded"
                self.save()
                return True
            body = deepcopy(job["intent"])
            closed = body["status"] == "closed"
            service = SERVICES[job["phase"]]
            if not closed and service != "crm":
                contacts = self.transport.get("crm", body["lead_id"])
                body["crm_id"] = contacts[0]["id"] if contacts else None
            if not closed and service == "notifications":
                bookings = self.transport.get("calendar", body["lead_id"])
                body["booking_id"] = bookings[0]["id"] if bookings else None
            # Re-read operator state before every continuation, including retries.
            if variant not in ("M04_no_manual_fence", "M04_replace_whole_contact") and not closed:
                crm = self.transport.get("crm", body["lead_id"])
                if crm and crm[0]["status"] == "closed":
                    job["state"] = "superseded"
                    self.save()
                    return True
            key = f"{service}:{body['lead_id']}:{body['version']}:{'close' if closed else 'upsert'}"
            method = "DELETE" if closed and service != "crm" else "PUT"
            unsafe_replace = False
            if variant.startswith(("M01_", "M02_")) and not closed:
                method, key = "POST", None
            if variant.startswith("M04_") and not closed:
                method, key = "POST", None
                unsafe_replace = service != "notifications"
            job["attempts"] += 1
            self.save()
            try:
                self.transport.write(service, method, body, key, job["event_id"], tick, unsafe_replace)
            except WorkerCrash:
                self.record("worker_crash", tick=tick, service=service, lead_id=body["lead_id"],
                            version=body["version"], detail={"checkpoint": "before_local_commit"})
                raise
            except ServiceError as exc:
                if variant == "M03_partial_state_is_success":
                    job["state"] = "complete"
                    job["phase"] = 3
                elif exc.status == 409:
                    job["state"] = "superseded"
                else:
                    ignore_due = variant in ("M06_ignore_retry_after_and_exhaust_budget",
                                              "M06_reset_backoff_on_restart")
                    job["next_due"] = tick + (1 if ignore_due else max(1, exc.retry_after))
                    if ignore_due and job["attempts"] >= 3:
                        job["state"] = "exhausted"
                    if variant == "M07_global_step_key_or_backoff":
                        self.data["global_due"] = job["next_due"]
                self.record("worker_checkpoint", tick=tick, lead_id=body["lead_id"], version=body["version"],
                            service=service, detail={"state": job["state"], "next_due": job["next_due"]})
                self.save()
                return True
            # A new authoritative fact may have arrived at the effect barrier.
            if self._fenced(job):
                job["state"] = "superseded"
            else:
                job["phase"] += 1
                if job["phase"] == 3:
                    job["state"] = "blocked" if closed else "complete"
            self.save()
            self.record("worker_checkpoint", tick=tick, service=service, lead_id=body["lead_id"],
                        version=body["version"], detail={"state": job["state"], "phase": job["phase"]})
            if job["state"] in ("complete", "blocked"):
                self.barrier({"point": "worker_completed", "lead_id": body["lead_id"],
                              "version": body["version"], "tick": tick})
            return True
        return False

    def claims(self):
        result = []
        for lead in sorted({j["lead_id"] for j in self.data["jobs"]}):
            jobs = [j for j in self.data["jobs"] if j["lead_id"] == lead]
            job = max(jobs, key=lambda j: (j["version"], j["id"]))
            result.append({k: job[k] for k in ("lead_id", "version", "state", "attempts", "next_due")} |
                          {"phase": SERVICES[job["phase"]] if job["phase"] < 3 else "done",
                           "remaining": list(SERVICES[job["phase"]:])})
        return result
