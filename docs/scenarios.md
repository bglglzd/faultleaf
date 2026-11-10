# Scenario fixtures

The 24 YAML v1 fixtures are predeclared inputs and expected semantic state for the lead-to-appointment sandbox. They contain no executable YAML, aliases, secrets or real customer data. A loaded fixture is not evidence that its workflow or mutation passed: the parser, services, worker, oracle and reducer must execute and record actual results separately.

Each file is self-contained: authoritative facts, initial CRM/calendar/notification rows, stable schedule tokens, semantic fault selectors, expected current service rows, worker claim fields, mutation target signatures and reduction pins. Scenario IDs, seeds, horizons, fault modes, authored token order and mutation variant names are retained from the preparation plan. The plan remains private; these public fixtures do not depend on it at runtime.

## Field mapping

| Preparation field | YAML v1 field |
| --- | --- |
| `revision` | `version` |
| `slot_id` | `slot` |
| `sales_status` | `status` |
| `lead.request` / `operator.status` | fact `type: request` / `type: operator` |
| source fact name | map key and explicit `event_id` |
| `authoritative_event_anchor` / pinned token list | schedule `pinned` boolean |
| `attempt_occurrence` | fault `occurrence` |
| `all_attempts_through: 10` | fault `occurrence: all`, `through: 10` |
| `worker_claims` | `expected.claims` |
| `ensure_contact`, `ensure_booking`, `queue_confirmation` in pending `remaining` | `crm`, `calendar`, `notifications` |
| profile `calendar_active`, `notifications_queued` | `expected.services.calendar`, `expected.services.notifications` |

Expected service rows describe current active/queued state. Active flags are implicit in the service projection; opaque generated IDs are not literal expectations. Only supplied row fields are matched. Operator tags are preserved. Request facts carry `status: open`; the worker must preserve operator-owned priority/closed state when applying a normal request. The close fact uses the request's display name and slot, with `status: closed`. The existing FL22 contact defaults to `display_name: Alex Example` at version 0. Every notification is a synthetic queue record, never an external send.

`E1_equivalent` in FL03 is a distinct event ID with the same lead/version/payload as E1. It is optional delivery noise, not another pinned authoritative revision. The fixtures do not reuse one event ID with conflicting payload.

## Logical barriers and faults

A scheduled action has exactly one integer `at` time or an `after` barrier. Equal-time actions retain their authored list order. A barrier is a structured selector, not a sleep or a global request number.

| Preparation barrier | YAML v1 barrier |
| --- | --- |
| worker completed for lead/revision | `point: worker_completed`, `lead_id`, `version` |
| worker receipt durable for event E1 | `point: receipt_durable`, `event_id: E1` |
| service effect committed | original service/operation/lead selector, mapped `version`, `point: effect_committed` |
| service rate limited | original service/operation/lead selector, mapped `version`, `point: rate_limited` |

Service operation names remain `ensure_contact`, `ensure_booking` and `queue_confirmation`. Fault mode strings are unchanged. FL11's outage applies to every matching attempt through tick 10, rather than only its first attempt. A rate-limit fault opens the authored no-write window until its retry eligibility; numeric wall-clock sleeping is not part of this deterministic fixture contract. The observation horizon is fixed and is never a removable token.

`restart_worker` requests destruction of volatile worker state while retaining the same durable store and external service ledgers. It does not itself prove an operating-system process restart. The crash-after-commit fault modes retain their names and must be exercised by the actual engine. A separate process-restart check is needed for any advertised process-crash recovery claim.

## Cases

| ID | Scenario | Expected profile | Mutation variants | Reduction eligible |
| --- | --- | --- | --- | --- |
| FL01 | Clean appointment control | `ready_A1` | Control only | No |
| FL02 | Same delivery repeated before processing | `ready_A1` | `M01_no_idempotency` | Yes |
| FL03 | New delivery IDs for the same lead revision | `ready_A1` | `M01_no_business_revision_dedupe` | Yes |
| FL04 | CRM write commits but reply is lost | `ready_A1` | `M02_fresh_key_on_retry` | Yes |
| FL05 | Calendar write commits but reply is lost | `ready_A1` | `M02_fresh_key_on_retry` | Yes |
| FL06 | Queue insertion commits but reply is lost | `ready_A1` | `M02_fresh_key_on_retry` | Yes |
| FL07 | CRM throttling and successful recovery | `ready_A1` | `M06_ignore_retry_after_and_exhaust_budget` | Yes |
| FL08 | Duplicate arrives during calendar backoff | `ready_A1` | `M03_partial_state_is_success` | Yes |
| FL09 | Notification throttling exposes final-step pending state | `ready_A1` | `M03_partial_state_is_success` | Yes |
| FL10 | Retry-After exceeds the observation horizon | `pending_calendar_A1` | `M03_partial_state_is_success` | No |
| FL11 | Queue outage remains visible then recovers | `pending_notification_A1` | `M03_partial_state_is_success` | No |
| FL12 | Newer revision is delivered first | `ready_B2_no_old_notification` | `M04_no_revision_fence` | Yes |
| FL13 | Old revision is replayed after newer completion | `ready_B2_no_old_notification` | `M04_no_revision_fence` | Yes |
| FL14 | Operator closes after CRM commit | `blocked_before_booking` | `M04_no_manual_fence` | No |
| FL15 | Operator closes after booking but before confirmation | `blocked_after_booking` | `M04_no_manual_fence` | No |
| FL16 | Requested slot changes after the first ready state | `ready_B2_after_ready_A1` | `M04_no_revision_fence` | Yes |
| FL17 | Old pending confirmation races a new request | `ready_B2_no_old_notification` | `M04_no_effect_execution_fence` | Yes |
| FL18 | Worker restarts after durable receipt, before first write | `ready_A1` | `M05_volatile_pending_work` | Yes |
| FL19 | Worker crashes after CRM commit, before checkpoint | `ready_A1` | `M02_fresh_key_on_retry` | Yes |
| FL20 | Worker restarts during persisted backoff | `ready_A1` | `M05_volatile_pending_work`, `M06_reset_backoff_on_restart` | Yes |
| FL21 | Worker crashes after queue commit, before final checkpoint | `ready_A1` | `M02_fresh_key_on_retry` | Yes |
| FL22 | Existing contact retains operator-owned data | `ready_A1_preserve_operator_fields` | `M04_replace_whole_contact` | Yes |
| FL23 | Two distinct leads share a display name | `two_same_name_ready` | `M07_key_by_display_name` | Yes |
| FL24 | One lead waits while another completes | `L1_pending_L2_ready` | `M07_global_step_key_or_backoff` | No |

## Preparation corrections and scope

FL23 and FL24 target the missing L2 effect, correcting the preparation plan's L1 target. At logical tick 0 L1 is authored first; the display-name/global-key mutation absorbs or delays the second lead, L2. In FL24, L1's missing booking is legitimate pending state under its own rate-limit window, so it cannot be the missing-effect detection target. The corrected signatures retain the original class, rule, service and version. This is a specification correction, not a claim that either mutation was observed to fail.

FL10, FL11 and FL24 intentionally end with declared pending work. The human FL10 `next_due` expression is omitted: it is not a machine-readable expected value. Actual durable due times must be checked independently against the fault window. FL11 remains the horizon-10 pending fixture; the preparation plan's clear-at-11/horizon-24 recovery extension is a separate recovery check, not silently folded into this fixture or counted as a 25th scenario.

`forbid_old_notifications: true` appears only for profiles that forbid an old revision's insertion (FL12, FL13 and FL17). FL16 permits a previously queued version-1 confirmation to remain as withdrawn history after version 2 supersedes it. Final-row expectations do not replace ledger and temporal safety checks: cancelled/superseded history, duplicate commits, premature completion claims and fault reachability must be inspected by the runtime oracle. No free-text checkpoint assertion has been copied into YAML as if the engine already executed it.

Reduction uses stable token IDs and explicit pins. Required fault IDs, authoritative facts, initial snapshot and horizon remain fixed. Eligibility does not promise that a smaller trace exists. A reduced run must preserve the exact class/rule/service/lead/version signature, reach its required faults, and retain a repaired passing control under the independent oracle. Only a completed final deletion sweep supports bounded deletion-minimality; no global minimum is claimed.

Data validation for this conversion uses PyYAML 6.0.3 `safe_load`, exact generated-object round trips, the 24 unique IDs, schedule/fact/fault reference checks and a token scan rejecting YAML tags, aliases and anchors. Product execution and review results are deliberately not asserted here.
