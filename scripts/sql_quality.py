"""Lint static Rust SQL and maintained .sql files without rewriting migration history."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

from sql_source import inline_queries

ROOT = Path(__file__).resolve().parents[1]
HISTORY = ROOT / "docs/engineering/migration-history.json"


def migration_history() -> dict[str, str]:
    history = json.loads(HISTORY.read_text())
    for name, expected in history.items():
        path = ROOT / name
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Applied migration changed or disappeared: {name}")
    return history


def maintained_files(history):
    for path in sorted(ROOT.rglob("*.sql")):
        relative = path.relative_to(ROOT)
        if any(part in {"target", ".git", ".venv"} for part in relative.parts):
            continue
        if str(relative) not in history:
            yield path


def rust_literal(sql: str) -> str:
    if len(sql.strip().splitlines()) == 1 and len(sql.strip()) <= 60:
        return json.dumps(sql.strip(), ensure_ascii=False)
    hashes = "#"
    while '"' + hashes in sql:
        hashes += "#"
    return f'r{hashes}"\n{sql.rstrip()}\n"{hashes}'


def parser_limitations():
    return json.loads((ROOT / "docs/engineering/sql-parser-limitations.json").read_text())


def lint_sql(linter, sql, name, *, fix=False):
    result = linter.lint_string(sql.strip() + "\n", fname=name, fix=fix)
    violations = result.get_violations()
    if any(violation.rule_code() in {"PRS", "LXR", "TMP"} for violation in violations):
        fix = False
    formatted = result.fix_string()[0] if fix else sql
    limitations = parser_limitations()
    digest = hashlib.sha256(sql.strip().encode()).hexdigest()
    if digest in limitations:
        limitation = limitations[digest]
        if not limitation.get("reason") or not limitation.get("validation"):
            raise ValueError(f"Undocumented SQL parser limitation: {name}")
        if not any(violation.rule_code() == "PRS" for violation in violations):
            raise ValueError(f"Stale SQLFluff parser limitation; remove its entry: {name}")
        violations = [violation for violation in violations if violation.rule_code() != "PRS"]
    return formatted, [
        {"code": violation.rule_code(), "line": violation.line_no, "message": violation.desc()}
        for violation in violations
    ]


def check_dynamic(queries):
    manifest_path = ROOT / "docs/engineering/dynamic-sql.json"
    known = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    discovered = {}
    for query in queries:
        if query.dynamic:
            digest = hashlib.sha256(query.sql.encode()).hexdigest()
            discovered[digest] = {"path": str(query.path.relative_to(ROOT)), "line": query.line}
    unknown = sorted(
        digest for digest in discovered
        if digest not in known or not known[digest].get("validation")
        or not known[digest].get("purpose") or not known[digest].get("input_safety")
    )
    issues = [
        {"source": discovered[digest], "code": "DYNAMIC",
         "message": "Review and register a new SQL template with its rendered validation"}
        for digest in unknown
    ]
    issues.extend(
        {"source": digest, "code": "DYNAMIC",
         "message": "Remove the stale SQL template registration"}
        for digest in known.keys() - discovered.keys()
    )
    return discovered, issues


def run(args):
    from sqlfluff.core import FluffConfig, Linter

    history = migration_history()
    linter = Linter(config=FluffConfig.from_path(str(ROOT)))
    queries = list(inline_queries())
    dynamic, issues = check_dynamic(queries)
    # Render actual source fragments rather than pretending placeholders are SQL.
    # Layout of composed fragments is reviewed at their source; parse every variant.
    from sql_templates import render_queries

    rendered = render_queries()
    for name, sql in rendered.items():
        parsed = linter.parse_string(sql)
        issues.extend(
            {"source": f"rendered:{name}", "code": violation.rule_code(),
             "message": violation.desc()}
            for violation in parsed.violations
        )
    replacements = {}
    limitations = parser_limitations()
    seen_limitations = set()
    for query in queries:
        if query.dynamic:
            continue
        formatted, violations = lint_sql(
            linter, query.sql, query.identity, fix=args.command == "fix"
        )
        digest = hashlib.sha256(query.sql.strip().encode()).hexdigest()
        if digest in limitations:
            seen_limitations.add(digest)
        if args.command == "fix" and formatted != query.sql:
            replacements.setdefault(query.path, []).append(
                (query.start, query.end, rust_literal(formatted))
            )
        issues.extend({"source": query.identity, **violation} for violation in violations)
    sql_files = list(maintained_files(history))
    for path in sql_files:
        name = str(path.relative_to(ROOT))
        formatted, violations = lint_sql(
            linter, path.read_text(), name, fix=args.command == "fix"
        )
        if args.command == "fix":
            path.write_text(formatted)
        issues.extend({"source": name, **violation} for violation in violations)
    for path, edits in replacements.items():
        source = path.read_text()
        for start, end, replacement in sorted(edits, reverse=True):
            source = source[:start] + replacement + source[end:]
        path.write_text(source)
    issues.extend(
        {"source": digest, "code": "PARSER", "message": "Remove the stale parser limitation"}
        for digest in limitations.keys() - seen_limitations
    )
    report = {"static_rust_queries": len(queries) - len(dynamic), "dynamic_templates": dynamic,
              "rendered_variants": list(rendered),
              "maintained_sql_files": [str(path.relative_to(ROOT)) for path in sql_files],
              "parser_limitations": limitations,
              "immutable_migrations": len(history), "violations": issues}
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    if args.command == "fix":
        print("Formatted maintained SQL; rerun lint and review the source diff.")
        return 0
    for issue in issues:
        print(f"{issue['source']}: {issue['code']}: {issue['message']}")
    print(f"SQL: {report['static_rust_queries']} inline statements, "
          f"{len(sql_files)} SQL files, {len(dynamic)} reviewed templates, "
          f"{len(history)} immutable migrations")
    print(f"PostgreSQL integration validation required for {len(seen_limitations)} "
          "documented SQLFluff grammar limitations")
    return bool(issues)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["lint", "fix"])
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        return run(args)
    except (ValueError, OSError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
