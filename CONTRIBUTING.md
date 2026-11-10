# Contributing

Use Python 3.11 or newer. Create a virtual environment, then run:

```sh
python -m pip install -e . -r requirements-dev.txt
python -m ruff check faultleaf tests
python -m unittest discover -s tests -v
python -m faultleaf suite --repeat --out reports/suite.json
```

For behavioral changes, add a meaningful scenario or independent assertion covering the failure and recovery. Keep expected state separate from workflow implementation. Record a reproducible command, expected and actual state, runtime version and source commit in bug reports. Use synthetic data only. Never include customer traces, credentials or operational tokens. Preserve third-party licenses; n8n remains a separately installed runtime under its own license.
