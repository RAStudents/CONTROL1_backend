"""Read SQL-bearing Rust string tokens without executing Rust or touching database state."""
from dataclasses import dataclass
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]

SQL_START = re.compile(
    r"^(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|SET|SHOW|RESET|"
    r"TRUNCATE|ANALYZE|EXPLAIN|COMMENT|GRANT|REVOKE|VACUUM|CALL|DO|"
    r"BEGIN|COMMIT|ROLLBACK|SAVEPOINT)(?:\s|$)", re.I,
)
RAW_START = re.compile(r'r(#{0,32})"')
CHARACTER = re.compile(r"'(?:\\.|[^'\\])'")


@dataclass(frozen=True)
class RustSql:
    path: Path
    start: int
    end: int
    line: int
    sql: str
    dynamic: bool

    @property
    def identity(self) -> str:
        return f"{self.path.relative_to(ROOT)}:{self.line}"


def rust_strings(source: str):
    """Locate Rust string tokens; comments and character literals are not SQL."""
    position = 0
    while position < len(source):
        if source.startswith("//", position):
            end = source.find("\n", position)
            position = len(source) if end < 0 else end + 1
            continue
        if source.startswith("/*", position):
            depth = 1
            position += 2
            while position < len(source) and depth:
                if source.startswith("/*", position):
                    depth += 1
                    position += 2
                elif source.startswith("*/", position):
                    depth -= 1
                    position += 2
                else:
                    position += 1
            continue
        character = CHARACTER.match(source, position)
        if character:
            position = character.end()
            continue
        raw = RAW_START.match(source, position)
        if raw:
            delimiter = '"' + raw[1]
            end = source.find(delimiter, raw.end())
            if end < 0:
                raise ValueError("Unterminated Rust raw string")
            yield position, end + len(delimiter), source[raw.end():end]
            position = end + len(delimiter)
            continue
        if source[position] != '"':
            position += 1
            continue
        start = position
        position += 1
        while position < len(source):
            if source[position] == "\\":
                position += 2
            elif source[position] == '"':
                position += 1
                break
            else:
                position += 1
        token = source[start:position]
        try:
            decoded = json.loads(token, strict=False)
        except json.JSONDecodeError:
            if SQL_START.match(token[1:].lstrip()):
                raise ValueError(f"Unsupported Rust escape in SQL at offset {start}")
            continue
        yield start, position, decoded


def inline_queries():
    paths = sorted((ROOT / "src").rglob("*.rs"))
    for path in paths:
        source = path.read_text()
        for start, end, sql in rust_strings(source):
            statement = sql_without_header_comments(sql)
            if not SQL_START.match(statement):
                continue
            before = source[max(0, start - 100):start]
            dynamic = bool(re.search(r"\b(?:format|format_args)!\s*\(\s*$", before))
            # Lowercase command names and error messages are not SQL. Query calls
            # and constants still accept lowercase SQL so linting can correct it.
            query_context = re.search(
                r"(?:query\w*|raw_sql)(?:::[^()]+)?\s*\(\s*$"
                r"|const\s+\w+\s*:\s*&str\s*=\s*$", before,
            )
            if statement.split(maxsplit=1)[0].islower() and not (dynamic or query_context):
                continue
            yield RustSql(path, start, end, source.count("\n", 0, start) + 1, sql, dynamic)


def sql_without_header_comments(sql):
    statement = sql.lstrip()
    while statement.startswith(("--", "/*")):
        if statement.startswith("--"):
            newline = statement.find("\n")
            statement = statement[newline + 1:] if newline >= 0 else ""
        else:
            depth = 1
            position = 2
            while position < len(statement) and depth:
                if statement.startswith("/*", position):
                    depth += 1
                    position += 2
                elif statement.startswith("*/", position):
                    depth -= 1
                    position += 2
                else:
                    position += 1
            statement = statement[position:]
        statement = statement.lstrip()
    return statement
