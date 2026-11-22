"""Emit visible Clippy review metrics; never turn compiler errors into successful review."""
import argparse
import collections
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def compare_findings(findings, baseline):
    def signature(finding):
        return finding["file"], finding["code"], finding["message"]

    current = collections.Counter(signature(finding) for finding in findings)
    previous = collections.Counter(signature(finding) for finding in baseline)
    return {
        "existing": sum((current & previous).values()),
        "new": sum((current - previous).values()),
        "resolved": sum((previous - current).values()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="quality-review.json")
    args = parser.parse_args()
    baseline_path = ROOT / "docs/engineering/complexity-review.json"
    baseline = json.loads(baseline_path.read_text())["findings"]
    package = tomllib.loads((ROOT / "Cargo.toml").read_text())
    lints = package["package"]["metadata"]["quality"]["advisory_lints"]
    command = ["cargo", "clippy", "--workspace", "--all-targets", "--message-format=json", "--"]
    for name in lints:
        command += ["-W", f"clippy::{name}"]
    environment = dict(os.environ, CLIPPY_CONF_DIR=str(ROOT / ".config/clippy-review"))
    process = subprocess.run(command, cwd=ROOT, env=environment, text=True, capture_output=True)
    findings = []
    for line in process.stdout.splitlines():
        if not line.startswith("{"):
            continue
        record = json.loads(line)
        if record.get("reason") != "compiler-message":
            continue
        message = record["message"]
        if message["level"] not in {"warning", "error"}:
            continue
        span = next((span for span in message["spans"] if span["is_primary"]), None)
        finding = {"level": message["level"], "code": (message.get("code") or {}).get("code"),
                   "message": message["message"], "file": span["file_name"] if span else None,
                   "line": span["line_start"] if span else None}
        if finding not in findings:
            findings.append(finding)
    comparison = compare_findings(findings, baseline)
    report = {"command": command, "exit_code": process.returncode, "findings": findings,
              "baseline_comparison": comparison}
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    counts = collections.Counter(finding["code"] for finding in findings)
    print("Advisory complexity and invariant review:", dict(counts))
    print("Compared with the reviewed baseline:", comparison)
    for finding in findings:
        print(f"{finding['file']}:{finding['line']}: {finding['code']}: {finding['message']}")
    if process.returncode:
        print(process.stderr, file=sys.stderr)
    return process.returncode


if __name__ == "__main__":
    sys.exit(main())
