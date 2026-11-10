"""Execute the fixed lead workflow with a caller-owned n8n Server CLI."""

from __future__ import annotations

from collections.abc import Mapping
from importlib.resources import files
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit
import uuid

N8N_VERSION = "2.46.7"
EXPECTED_NODES = ("Start", "Load delivery", "CRM write", "Calendar write", "Notification write")
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
_SENSITIVE = re.compile(
    r"token|password|secret|credentials?|authorization|api.?key|encryption.?key|cookie", re.I
)
_JSON_SECRET = re.compile(
    r'("[^"\n]*(?:token|password|secret|authorization|api.?key|encryption.?key|cookie)[^"\n]*"\s*:\s*)'
    r'"(?:\\.|[^"\\])*"',
    re.I,
)
_TEXT_SECRET = re.compile(
    r"(\b[\w.-]*(?:token|password|secret|api.?key|encryption.?key|cookie)[\w.-]*\s*[:=]\s*)"
    r"(?:\"(?:\\.|[^\"\\])*\"|'[^']*'|[^\s,;]+)",
    re.I,
)
_AUTH_HEADER = re.compile(r"(\bauthorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;\"]+", re.I)


class N8nExecutionError(ValueError):
    """The captured execution cannot establish completion of the fixed graph."""


class N8nCLIError(RuntimeError):
    """A bounded command failed; diagnostic fields are already redacted."""

    def __init__(self, message, *, stage, returncode=None, stdout="", stderr=""):
        super().__init__(message)
        self.stage = stage
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _redact_text(text, redactions=()):
    text = _JSON_SECRET.sub(r'\1"[REDACTED]"', text)
    text = _TEXT_SECRET.sub(r"\1[REDACTED]", text)
    text = _AUTH_HEADER.sub(r"\1[REDACTED]", text)
    for secret in redactions:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return text


def _redact(value, redactions=()):
    if isinstance(value, dict):
        return {
            _redact_text(key, redactions): "[REDACTED]"
            if _SENSITIVE.search(key)
            else _redact(item, redactions)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item, redactions) for item in value]
    return _redact_text(value, redactions) if isinstance(value, str) else value


def _loopback_url(value, *, delivery=False):
    if not isinstance(value, str) or len(value) > 2048 or any(ord(c) > 126 or ord(c) < 33 for c in value):
        raise ValueError("n8n sandbox URLs must be ASCII loopback HTTP URLs without whitespace")
    try:
        parsed = urlsplit(value)
        host = ipaddress.ip_address(parsed.hostname or "")
        port = parsed.port
    except ValueError:
        raise ValueError("n8n sandbox URLs require a literal loopback IP and explicit port") from None
    if (
        parsed.scheme != "http"
        or not host.is_loopback
        or parsed.username is not None
        or parsed.password is not None
        or port is None
        or not 1 <= port <= 65535
        or parsed.query
        or parsed.fragment
        or "?" in value
        or "#" in value
        or parsed.path not in (("/delivery",) if delivery else ("", "/"))
    ):
        raise ValueError("Unsupported n8n sandbox URL")
    return value if delivery else value.rstrip("/")


def instantiate_workflow(services: dict[str, str], delivery_url: str) -> dict:
    """Create the fixed HTTP graph, pinning writes to caller-selected loopback APIs."""
    names = ("crm", "calendar", "notifications")
    if not isinstance(services, Mapping) or set(services) != set(names):
        raise ValueError("services must contain exactly crm, calendar and notifications")
    endpoints = {name: _loopback_url(services[name]) for name in names}
    delivery_url = _loopback_url(delivery_url, delivery=True)
    workflow = json.loads(files("faultleaf").joinpath("workflows/lead_to_appointment.json").read_text())
    workflow["versionId"] = str(uuid.uuid4())
    workflow["nodes"][1]["parameters"]["url"] = delivery_url
    for node, name in zip(workflow["nodes"][2:], names):
        # Configuration becomes a JSON string literal, never expression source supplied by an event.
        node["parameters"]["url"] = (
            "={{ "
            + json.dumps(endpoints[name])
            + " + '/items/' + encodeURIComponent($('Load delivery').item.json.intent.lead_id) }}"
        )
    return workflow


def _json_records(stdout):
    """Read complete top-level JSON fragments amid n8n startup messages."""
    start = None
    stack = []
    quoted = escaped = False
    for index, char in enumerate(stdout):
        if start is None:
            if char in "{[":
                start = index
                stack = [char]
            continue
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            quoted = True
        elif char in "{[":
            stack.append(char)
            if len(stack) > 128:
                raise N8nExecutionError("Execution JSON nesting exceeds the supported limit")
        elif char in "}]":
            if not stack or (stack[-1], char) not in (("{", "}"), ("[", "]")):
                raise N8nExecutionError("Mismatched brackets in CLI output")
            stack.pop()
            if not stack:
                raw = stdout[start : index + 1]
                start = None
                try:

                    def finite_float(text):
                        value = float(text)
                        if not math.isfinite(value):
                            raise ValueError("Nonfinite JSON")
                        return value

                    value = json.loads(
                        raw,
                        parse_float=finite_float,
                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
                    )
                except (ValueError, RecursionError):
                    if '"resultData"' in raw:
                        raise N8nExecutionError("Malformed execution JSON") from None
                    continue
                yield value
    if start is not None and '"resultData"' in stdout[start:]:
        raise N8nExecutionError("Truncated execution JSON")


def parse_execution(stdout: str) -> dict:
    """Return a sanitized full success record, rejecting ambiguous or partial evidence."""
    if not isinstance(stdout, str) or len(stdout.encode("utf-8", errors="replace")) > MAX_OUTPUT_BYTES:
        raise N8nExecutionError("CLI output exceeds the supported limit")
    candidates = [
        value
        for value in _json_records(stdout)
        if isinstance(value, dict) and isinstance(value.get("data"), dict) and "resultData" in value["data"]
    ]
    if len(candidates) != 1:
        raise N8nExecutionError("Expected exactly one complete n8n execution record")
    value = candidates[0]
    result = value["data"]["resultData"]
    if (
        value.get("mode") != "cli"
        or value.get("status") != "success"
        or value.get("finished") is not True
        or value.get("error") is not None
        or not isinstance(result, dict)
        or result.get("error") is not None
        or result.get("lastNodeExecuted") != EXPECTED_NODES[-1]
    ):
        raise N8nExecutionError("n8n workflow did not finish successfully")
    runs = result.get("runData")
    if not isinstance(runs, dict) or set(runs) != set(EXPECTED_NODES):
        raise N8nExecutionError("Execution did not cover the fixed five-node graph")
    for name in EXPECTED_NODES:
        entries = runs[name]
        if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
            raise N8nExecutionError("Each fixed node must have one completed run")
        entry = entries[0]
        if entry.get("executionStatus") != "success" or entry.get("error") is not None:
            raise N8nExecutionError("A workflow node failed or was skipped")
        data = entry.get("data")
        main = data.get("main") if isinstance(data, dict) else None
        if (
            not isinstance(data, dict)
            or set(data) != {"main"}
            or not isinstance(main, list)
            or len(main) != 1
            or not isinstance(main[0], list)
            or len(main[0]) != 1
            or not isinstance(main[0][0], dict)
        ):
            raise N8nExecutionError("A workflow node has missing or unexpected output")
        item = main[0][0].get("json")
        if not isinstance(item, dict) or item.get("error") is not None:
            raise N8nExecutionError("A node returned an error item instead of a completed output")
        if name in EXPECTED_NODES[2:]:
            status = item.get("statusCode")
            body = item.get("body")
            if (
                type(status) is not int
                or not 200 <= status < 300
                or not isinstance(body, dict)
                or body.get("error") is not None
            ):
                raise N8nExecutionError("A service write did not return a successful HTTP response")
    return _redact(value)


class N8nCLI:
    """Sequential import/execute adapter; the caller owns its CLI runtime and directories."""

    def __init__(
        self,
        executable="n8n",
        *,
        cwd,
        data_dir=None,
        timeout=120,
        max_output_bytes=MAX_OUTPUT_BYTES,
        redact_values=(),
    ):
        try:
            command = (
                [os.fspath(executable)] if isinstance(executable, (str, os.PathLike)) else list(executable)
            )
        except TypeError:
            raise ValueError("Use an executable or an argv prefix") from None
        resolved_program = shutil.which(command[0]) if command and isinstance(command[0], str) else None
        if (
            not command
            or any(not isinstance(arg, str) or not arg or "\x00" in arg for arg in command)
            or (os.name == "nt" and Path(resolved_program or command[0]).suffix.lower() in (".cmd", ".bat"))
        ):
            raise ValueError("Use an executable or an argv prefix; Windows batch files are unsupported")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or not 0 < timeout <= 600
        ):
            raise ValueError("timeout must be finite and between 0 and 600 seconds")
        if type(max_output_bytes) is not int or not 1 <= max_output_bytes <= MAX_OUTPUT_BYTES:
            raise ValueError("max_output_bytes must be between 1 and 8 MiB")
        self.command = tuple(command)
        self.cwd = Path(cwd).resolve()
        selected_data = Path(data_dir) if data_dir is not None else Path("n8n-user")
        self.data_dir = (selected_data if selected_data.is_absolute() else self.cwd / selected_data).resolve()
        if self.data_dir == self.cwd or not self.data_dir.is_relative_to(self.cwd):
            raise ValueError("data_dir must be a dedicated child of cwd")
        self.cwd.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if isinstance(redact_values, (str, bytes)):
            raise ValueError("redact_values must be a collection of complete strings")
        redact_values = tuple(redact_values)
        if any(not isinstance(value, str) for value in redact_values):
            raise ValueError("redact_values must contain strings")
        self.timeout = timeout
        self.max_output_bytes = max_output_bytes
        self.redact_values = tuple(redact_values)
        self.last_runs = []
        self._version_verified = False

    def _environment(self):
        env = {
            key: value
            for key, value in os.environ.items()
            if not _SENSITIVE.search(key)
            and not key.upper().startswith(
                (
                    "GH_",
                    "GITHUB_",
                    "GITLAB_",
                    "CLAUDE_",
                    "ANTHROPIC_",
                    "AWS_",
                    "AZURE_",
                    "N8N_",
                    "DB_",
                    "QUEUE_",
                    "EXECUTIONS_",
                    "NPM_CONFIG_",
                )
            )
            and key.upper() not in ("NODE_OPTIONS", "NODE_PATH", "WEBHOOK_URL")
            and "proxy" not in key.lower()
        }
        env.update(
            {
                "HOME": str(self.data_dir),
                "USERPROFILE": str(self.data_dir),
                "N8N_USER_FOLDER": str(self.data_dir),
                "XDG_CONFIG_HOME": str(self.data_dir / "config"),
                "XDG_CACHE_HOME": str(self.data_dir / "cache"),
                "XDG_DATA_HOME": str(self.data_dir / "data"),
                "APPDATA": str(self.data_dir / "config"),
                "LOCALAPPDATA": str(self.data_dir / "cache"),
                "DB_TYPE": "sqlite",
                "DB_SQLITE_DATABASE": "database.sqlite",
                "N8N_DIAGNOSTICS_ENABLED": "false",
                "N8N_VERSION_NOTIFICATIONS_ENABLED": "false",
                "N8N_TEMPLATES_ENABLED": "false",
                "N8N_PERSONALIZATION_ENABLED": "false",
                "N8N_COMMUNITY_PACKAGES_ENABLED": "false",
                "N8N_BLOCK_ENV_ACCESS_IN_NODE": "true",
                "N8N_ENFORCE_SETTINGS_FILE_PERMISSIONS": "true",
                "N8N_RUNNERS_ENABLED": "false",
                "N8N_LISTEN_ADDRESS": "127.0.0.1",
                "N8N_RUNNERS_BROKER_LISTEN_ADDRESS": "127.0.0.1",
                "TZ": "UTC",
                "GENERIC_TIMEZONE": "UTC",
            }
        )
        return env

    @staticmethod
    def _stop(process):
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        elif process.poll() is None:
            process.kill()

    def _invoke(self, stage, arguments):
        chunks = {"stdout": [], "stderr": []}
        lock = threading.Lock()
        overflow = threading.Event()
        total = 0
        try:
            process = subprocess.Popen(
                [*self.command, *arguments],
                cwd=self.cwd,
                env=self._environment(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                start_new_session=os.name == "posix",
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
        except OSError:
            raise N8nCLIError("Could not start the configured n8n executable", stage=stage) from None

        def read(pipe, destination):
            nonlocal total
            try:
                while block := os.read(pipe.fileno(), 4096):
                    with lock:
                        room = max(0, self.max_output_bytes - total)
                        if room:
                            chunks[destination].append(block[:room])
                        total += len(block)
                        if total > self.max_output_bytes:
                            overflow.set()
            finally:
                pipe.close()

        readers = [
            threading.Thread(target=read, args=(pipe, name), daemon=True)
            for pipe, name in [(process.stdout, "stdout"), (process.stderr, "stderr")]
        ]
        for reader in readers:
            reader.start()
        deadline = time.monotonic() + self.timeout
        reason = None
        while process.poll() is None:
            if overflow.is_set() or time.monotonic() >= deadline:
                reason = "output_limit" if overflow.is_set() else "timeout"
                self._stop(process)
                break
            try:
                process.wait(timeout=min(0.05, max(0.001, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
        process.wait()
        for reader in readers:
            reader.join(timeout=1)
        if any(reader.is_alive() for reader in readers):
            self._stop(process)
            reason = "incomplete_output"
        if overflow.is_set():
            reason = "output_limit"
        output = {name: b"".join(parts).decode("utf-8", errors="replace") for name, parts in chunks.items()}
        record = {
            "stage": stage,
            "returncode": process.returncode,
            **{key: _redact_text(text, self.redact_values) for key, text in output.items()},
        }
        self.last_runs.append(record)
        if reason or process.returncode != 0:
            raise N8nCLIError("n8n command failed: " + (reason or "nonzero_exit"), **record)
        return output

    def run(self, services: dict[str, str], delivery_url: str) -> dict:
        """Import and execute once; the outer scheduler decides if/when to retry."""
        workflow = instantiate_workflow(services, delivery_url)
        self.last_runs = []
        lock_path = self.cwd / ".faultleaf-n8n.lock"
        try:
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            raise N8nCLIError(
                "Adapter directory is busy; reconcile its existing lock before retrying", stage="lock"
            ) from None
        path = None
        try:
            os.write(lock_fd, str(os.getpid()).encode())
            if not self._version_verified:
                output = self._invoke("version", ["--version"])
                if output["stdout"].strip() != N8N_VERSION:
                    raise N8nCLIError(
                        "This adapter requires n8n " + N8N_VERSION,
                        stage="version",
                        returncode=0,
                        stdout=_redact_text(output["stdout"], self.redact_values),
                    )
                self._version_verified = True
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                prefix="faultleaf-n8n-",
                dir=self.cwd,
                delete=False,
            ) as stream:
                path = Path(stream.name)
                json.dump(workflow, stream, ensure_ascii=True, allow_nan=False)
            imported = self._invoke("import", ["import:workflow", "--input=" + str(path)])
            if "Successfully imported 1 workflow." not in imported["stdout"]:
                raise N8nCLIError(
                    "n8n did not acknowledge importing the fixed workflow",
                    stage="import",
                    returncode=0,
                    stdout=_redact_text(imported["stdout"], self.redact_values),
                    stderr=_redact_text(imported["stderr"], self.redact_values),
                )
            output = self._invoke("execute", ["execute", "--id=" + workflow["id"], "--rawOutput"])
            try:
                result = parse_execution(output["stdout"])
            except N8nExecutionError:
                raise N8nCLIError(
                    "n8n execution was failed, partial or ambiguous",
                    stage="execute",
                    returncode=0,
                    stdout=_redact_text(output["stdout"], self.redact_values),
                    stderr=_redact_text(output["stderr"], self.redact_values),
                ) from None
            if self.redact_values:
                result = _redact(result, self.redact_values)
            return result
        finally:
            if path is not None:
                path.unlink(missing_ok=True)
            os.close(lock_fd)
            lock_path.unlink(missing_ok=True)
