# n8n adapter

The optional adapter imports and executes a fixed five-node workflow using the
n8n Server CLI **2.46.7**. It uses separate HTTP Request nodes for CRM, calendar
and notification writes. There is no Code node or call to the Python workflow
runner. The default Faultleaf simulation does not require n8n.

The supported runtime is a caller-owned Linux n8n installation or an equivalent
wrapper that forwards argument lists, paths and environment to a dedicated
runtime. No real service credentials are used. n8n is an external dependency,
not included in the Faultleaf package. Community portions use the
[Sustainable Use License 1.0](https://github.com/n8n-io/n8n/blob/n8n%402.46.7/LICENSE.md),
with separate enterprise exclusions in that file. Faultleaf's license does not
relicense n8n.

## Workflow input and writes

`instantiate_workflow(services, delivery_url)` returns a new workflow dictionary.
`services` must contain exactly `crm`, `calendar` and `notifications`. Each value
must be an HTTP base URL with a literal loopback IP, explicit port and no path,
credentials, query or fragment. The delivery URL follows the same restrictions
and must have the path `/delivery`. `localhost`, HTTPS and remote services are
outside this local sandbox adapter's supported profile.

The graph is:

```text
Start → Load delivery → CRM write → Calendar write → Notification write
```

`GET /delivery` supplies data only:

```json
{
  "intent": {
    "lead_id": "L1",
    "version": 1,
    "display_name": "Synthetic lead",
    "slot": "2030-01-01T10:00:00Z",
    "status": "open",
    "closed": false
  },
  "event_id": "event-1",
  "tick": 0,
  "services": {
    "crm": "http://127.0.0.1:31001",
    "calendar": "http://127.0.0.1:31002",
    "notifications": "http://127.0.0.1:31003"
  }
}
```

The Python caller pins write authorities from its `services` argument. Values
under delivery `services` cannot redirect them. Every write retains the original
delivery intent; headers and operation choice also use that item. Explicit CRM
and booking references come from the prior HTTP receipts described below.
CRM always uses `PUT /items/<lead_id>` and retains its closed item. Calendar and
notification nodes use PUT for an open intent and DELETE for a closed intent.
The lead identifier is URL-encoded.

Open calendar bodies add `crm_id` from the CRM response's `body.items[0].id`.
Open notification bodies add that CRM id and `booking_id` from the calendar
response. Closed bodies contain only the original intent; they do not index an
empty array returned by a DELETE. The sandbox must return full HTTP JSON
responses with `body: {items: [...], effect: ...}`. A missing required open-item
reference stops the workflow rather than fabricating a reference.

Each write sends:

- `Idempotency-Key: <service>:<lead_id>:<version>:upsert` or `:close`;
- `X-Faultleaf-Event` containing the stable event id;
- `X-Faultleaf-Tick` containing the logical tick.

Node retries are disabled. A non-2xx response or lost connection stops the graph.
The outer durable scheduler decides whether to replay the whole workflow at a
later logical tick using the same business-operation keys. n8n does not implement
the logical clock or the scenario reducer.

## Calling the CLI adapter

```python
from pathlib import Path
from faultleaf.n8n import N8nCLI

run_dir = Path("n8n-run").resolve()
adapter = N8nCLI(
    executable="n8n",
    cwd=run_dir,
    data_dir=run_dir / "n8n-user",
    timeout=120,
)
execution = adapter.run(services, delivery_url)
```

The caller creates the three sandbox APIs and data-only delivery endpoint first.
The adapter checks the CLI version once, writes a temporary workflow file,
imports it with `import:workflow --input=<file>`, then executes its fixed ID with
`execute --id=<id> --rawOutput`. The removed `execute --file` path is not used.
All process arguments are separate list items; no shell is invoked.

The child environment uses a SQLite n8n user folder within `data_dir`, removes
inherited database/proxy/credential settings and disables optional diagnostics,
community packages and task runners. The adapter does not modify global n8n
settings, initialize a real account, pull images or start/stop containers. A
container wrapper must itself mount the supplied paths, forward this isolation
environment and clean up its task-owned runtime when terminated. The adapter
cannot enforce isolation inside an arbitrary wrapper.

Use an executable path or argument prefix, such as
`["node", "/path/to/n8n/bin/n8n"]`. Windows batch wrappers are rejected because
their argument interpretation can invoke a shell. The real integration profile
is Linux; synthetic subprocess tests also exercise paths containing spaces.

Do not share a run/data directory between concurrent adapters. An exclusive lock
prevents concurrent imports into that directory. If a worker is killed while the
lock remains, reconcile the task-owned CLI process before removing the lock and
replaying; do not assume a second invocation is safe just because the first
client disappeared. The data directory is retained for subsequent invocations.

## Execution evidence

`parse_execution(stdout)` accepts startup messages around one complete execution
JSON object. It rejects missing or ambiguous records, failed/partial/skipped
nodes, unexpected graph nodes, unsuccessful HTTP outputs and error items hidden
by continue-on-fail behavior. All five nodes must have completed once and the
last executed node must be the notification write. An exit code of zero is not
sufficient evidence.

`N8nCLIError` exposes `stage`, `returncode`, `stdout` and `stderr`; diagnostics
and `adapter.last_runs` are bounded and redact operational token fields. Optional
`redact_values` remove caller-known sensitive markers. The default combined
stdout/stderr cap is 8 MiB per command; the timeout applies to each command.
On Linux the adapter terminates only its own process group on timeout or output
overflow. A wrapper must not detach an untracked runtime.

Execution success means the fixed nodes completed. It does **not** establish
that the three business states agree, that pending work recovered or that no
duplicate effect occurred. The caller must inspect `/state`, effect ledgers and
the independent scenario invariants before marking business work complete.

`tests/test_n8n.py` uses synthetic execution records and fake local CLI processes.
It verifies failure/argument/environment/output boundaries; it is not a real
n8n installation or product-acceptance result. Final release evidence must bind
an actual supported n8n execution and business snapshots to the released source.
