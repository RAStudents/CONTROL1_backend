"""Enforce source layer boundaries and Python syntax without dependencies."""
import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT / "src/domain").glob("*.rs"):
    source = path.read_text()
    assert not re.search(r"\b(axum|sqlx|tokio|async_nats|infrastructure|transport|application)::", source), path
for path in (ROOT / "src/application").rglob("*.rs"):
    source = path.read_text()
    assert "axum::" not in source and "crate::transport" not in source, path
    # All schema-specific SQL belongs to infrastructure repositories/catalogs.
    assert not re.search(r'"(?:SELECT|INSERT|UPDATE|DELETE|WITH|SET)\b', source), path
for path in (ROOT / "src/transport").rglob("*.rs"):
    source = path.read_text()
    assert "sqlx::" not in source and ".pool" not in source, path
for path in (ROOT / "tests").glob("*.py"):
    ast.parse(path.read_text(), filename=str(path))
print("Architecture boundaries and Python syntax passed")
