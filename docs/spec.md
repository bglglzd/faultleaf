# v0.1 acceptance specification

User: an automation engineer checking one lead-to-appointment integration. Load a YAML scenario, compare an intentionally unsafe Python workflow with a durable candidate under the same initial state/fault schedule, inspect the three resulting service states, recover pending work and reduce a selected failure without changing its identity.

## Domain

Each immutable fact has `event_id`, `lead_id`, integer `version`, `slot`, `display_name` and `status` (`open`, `priority`, `closed`). Normal requests preserve operator-owned status/tags. A higher-version operator close cancels an active synthetic booking and withdraws the queued confirmation. A later normal request cannot reopen an operator-closed lead. Rescheduling retains one active booking/current queue item. No notification is actually sent.

Three separate loopback APIs support `GET /state`, `GET /items/<lead>`, safe `PUT /items/<lead>`, safe `DELETE /items/<lead>` and intentionally unkeyed `POST /items`. PUT/DELETE use `Idempotency-Key`, `X-Faultleaf-Event`, `X-Faultleaf-Tick`. Idempotency replays the prior acknowledgment without another effect; conflicting reuse fails. Unkeyed creates can duplicate business identities. State includes a retained before/after effect ledger and separate attempt records.

## Required behavior

1. At least twenty meaningful predeclared scenarios cover duplicate deliveries/business revisions, lost acknowledgments after each service commit, logical Retry-After, pending horizons, stale revisions, operator edits/closure, rescheduling, restart checkpoints and cross-lead isolation. Explicit expected profiles are independent of workflow output.
2. Logical time and stable semantic fault keys determine replay. Canonical report state/timeline/digest repeats for the same inputs. Wall-clock duration, ports and machine provenance are separate.
3. The candidate persists received/latest intents, step/checkpoint, attempt count and next due tick in SQLite. It coalesces older revisions, preserves manual fields, reuses per-service operation keys and never calls a partial state complete. Reopening the durable store must preserve work; at least one separate task-owned process restart is exercised before a process-recovery claim.
4. Classifiers inspect actual states and retained effects: duplicate_effect, missing_effect, stale_overwrite and false_success. Expected pending/closed outcomes are valid, distinct from false success. Correct state cannot hide duplicate committed history.
5. Faults select service/semantic operation/lead/version/occurrence. After-commit timeout means the effect commits before the connection closes. Rate limiting opens a logical no-write window. Worker crash hooks occur at documented barriers; the harness deterministically resumes at the next tick unless an explicit restart is scheduled.
6. The reducer pins authoritative target facts/faults/horizon and removes only declared optional schedule tokens. It preserves exact class/rule/service/lead/version and required fault IDs, rejects harness failures, and requires the repaired replay to pass. Deletion-minimality is claimed only after the bounded final sweep, never globally minimal cause.
7. A genuine n8n export runs separate HTTP nodes against these APIs. The input endpoint supplies data only. Supported real scenario subset and runtime version are recorded; simulation is not n8n evidence. No real accounts or paid services are required for the default demo.
8. CLI JSON/HTML output includes before/after state, attempts/effects, logical timeline, exact violation witness, recovery and reduction evidence. Static viewer labels saved/synthetic data, imports local reports safely and has no marketing hero/slogan. Controls, keyboard, mobile and actual screenshots are checked.
9. Installation, build/static/domain/integration checks, current CI, independent source/acceptance reviews and exact artifact hashes cover the released commit before publication.

## Limits

One bounded synthetic business process; not arbitrary production replay, a CRM product or a general automation platform. No exactly-once delivery claim; idempotent effects are scoped to these APIs. n8n is an external runtime under its own license, not redistributed as MIT. Failure minimization and state-based evaluation are established methods; the product packages portable integrator scenarios and inspectable business effects.

## Architecture

Scenario parsing/validation → logical scheduler → durable worker or n8n execution adapter → three inspectable service stores → independent invariant checks → reducer and report. A small SQLite store is justified by restart evidence. No shared portfolio framework or cloud service is required.
