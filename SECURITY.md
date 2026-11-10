# Reporting a vulnerability

Do not post credentials or customer data in issues. Use GitHub private vulnerability reporting when enabled; otherwise contact the repository owner through their public profile with a description that contains no sensitive payload.

The sandbox deliberately offers unsafe writes for diagnosing faulty clients. Listeners bind only to loopback; never expose them as public services. Scenario YAML is data-only, bounded and rejects aliases, custom tags and duplicate keys. Reports embed escaped JSON and render imported strings as text. The offline viewer makes no automatic network requests.

External n8n installation and its dependencies are separate from this Python package. Use the documented task-owned isolated runtime. Do not run untrusted workflow exports, attach production credentials or mount personal configuration into the sandbox.
