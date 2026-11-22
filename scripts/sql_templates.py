"""Render the existing, finite SQL builder variants for parsing and DB plan validation."""
from pathlib import Path
import re

from sql_source import ROOT, rust_strings

REPOSITORY = ROOT / "src/infrastructure/event_repository.rs"


def constant_sql(name: str, path: Path = REPOSITORY) -> str:
    source = path.read_text()
    declaration = re.search(rf"\bconst\s+{name}\s*:\s*&str\s*=\s*", source)
    if not declaration:
        raise ValueError(f"SQL constant not found: {path}:{name}")
    included = re.match(r'include_str!\("([^\"]+)"\)', source[declaration.end():])
    if included:
        sql_path = (path.parent / included[1]).resolve()
        if not sql_path.is_relative_to(ROOT):
            raise ValueError(f"SQL include escapes the project: {path}:{name}")
        return sql_path.read_text()
    for start, _, sql in rust_strings(source):
        if start == declaration.end():
            return sql
    raise ValueError(f"Unsupported SQL constant declaration: {path}:{name}")


def function_strings(name: str, path: Path = REPOSITORY) -> list[str]:
    source = path.read_text()
    declaration = re.search(rf"(?m)^(?:pub(?:\(crate\))?\s+)?(?:async\s+)?fn\s+{name}\b", source)
    if not declaration:
        raise ValueError(f"SQL builder not found: {path}:{name}")
    following = re.search(
        r"(?m)^(?:pub(?:\(crate\))?\s+)?(?:async\s+)?fn\s+", source[declaration.end():]
    )
    end = declaration.end() + following.start() if following else len(source)
    return [sql for _, _, sql in rust_strings(source[declaration.start():end])]


def render_queries() -> dict[str, str]:
    fragments = {
        name: constant_sql(name) for name in ["EVENT_FIELDS", "DICTS", "AS_OF", "MATCHING", "ORDER"]
    }
    filtered = next(sql for sql in function_strings("filtered_sql") if "{select}" in sql)
    core = filtered.format(select="e.id", joins="", **fragments)
    display = filtered.format(
        select=fragments["EVENT_FIELDS"], joins=fragments["DICTS"], **fragments
    )
    matched = function_strings("matched_sql")[0]
    materialization = function_strings("snapshot_materialization_sql")
    projection = materialization[0].format(**fragments)
    materialized_rows = filtered.format(select=projection, joins="", **fragments)
    probe = next(
        sql for sql in function_strings("verify_affinity", ROOT / "src/bootstrap.rs")
        if "{marker}" in sql
    )
    return {
        "filtered_core": core,
        "filtered_display": display,
        "matched_core": matched.format(core, **fragments),
        "matched_display": matched.format(display, **fragments),
        "current": function_strings("current")[0].format(**fragments),
        "page": function_strings("page_sql")[0].format(**fragments),
        "snapshot_count": function_strings("snapshot_count_sql")[0].format(core, 100001),
        "snapshot_materialization": materialization[1].format(select=materialized_rows),
        "export_count": function_strings("export_count_sql")[0].format(core),
        "affinity_probe": probe.format(marker="00000000-0000-0000-0000-000000000001"),
    }
