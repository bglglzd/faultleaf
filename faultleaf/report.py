"""Write an offline viewer for one saved synthetic replay."""

import json
from pathlib import Path


MAX_REPORT_BYTES = 2 * 1024 * 1024


def _encoded(report: dict) -> str:
    if not isinstance(report, dict):
        raise ValueError("Report must be an object")
    raw = json.dumps(report, ensure_ascii=False, allow_nan=False, indent=2)
    if len(raw.encode("utf-8")) > MAX_REPORT_BYTES:
        raise ValueError("Report exceeds the 2 MiB viewer limit")
    pending, nodes = [(report, 0)], 0
    while pending:
        value, depth = pending.pop()
        nodes += 1
        if nodes > 25000 or depth > 32:
            raise ValueError("Report exceeds the viewer shape limit; reduce the replay horizon")
        if isinstance(value, list):
            if len(value) > 2000:
                raise ValueError("Report list exceeds 2000 viewer records; reduce the replay horizon")
            pending.extend((child, depth + 1) for child in value)
        elif isinstance(value, dict):
            if len(value) > 200:
                raise ValueError("Report object exceeds 200 viewer fields")
            pending.extend((child, depth + 1) for child in value.values())
        elif isinstance(value, str) and len(value) > 262144:
            raise ValueError("Report string exceeds the 256 KiB viewer limit")
    return raw


def render_report(report: dict) -> str:
    """Return self-contained HTML; imported strings never become HTML markup."""
    raw = _encoded(report)
    embedded = (
        raw.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    assets = Path(__file__).with_name("assets")
    template = (assets / "report.html").read_text(encoding="utf-8")
    mark = (assets / "mark.svg").read_text(encoding="utf-8")
    mark = mark.replace('role="img"', 'aria-hidden="true" focusable="false"')
    mark = mark.replace('aria-label="Faultleaf mark"', "")
    return template.replace("__FAULTLEAF_MARK__", mark).replace("__FAULTLEAF_DATA__", embedded)


def write_report(report: dict, path: Path) -> None:
    """Write report.html and report.json, or an explicitly named HTML file."""
    path = Path(path)
    html = render_report(report)
    raw = _encoded(report)
    target = path if path.suffix.lower() == ".html" else path / "report.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.with_name("report.json").write_text(raw + "\n", encoding="utf-8")
    target.write_text(html, encoding="utf-8")
