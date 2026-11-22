"""Regression tests for source-safe quality gates, not Rust implementation details."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import sql_quality
from complexity_review import compare_findings
from sql_source import RustSql, rust_strings, sql_without_header_comments
from sql_templates import render_queries


class SqlSourceTests(unittest.TestCase):
    def test_sql_header_comments_do_not_hide_query_from_discovery(self):
        self.assertEqual(
            sql_without_header_comments("-- pagination\n/* nested /* note */ */\nSELECT 1"),
            "SELECT 1",
        )

    def test_comments_and_character_literals_are_not_strings(self):
        source = '''// "SELECT wrong"
            /* nested /* "SELECT wrong" */ comment */
            let quote = '"'; let lifetime: &'static str = "SELECT right";
        '''
        self.assertEqual([item[2] for item in rust_strings(source)], ["SELECT right"])

    def test_rust_literals_round_trip_without_changing_sql_values(self):
        statements = [
            "SELECT 'Русский текст', 'device\\path'",
            'SELECT \'quote"# and "##\'\nFROM events',
            "SELECT '\\n' AS literal, E'\\\\' AS escape",
        ]
        for sql in statements:
            with self.subTest(sql=sql):
                literal = sql_quality.rust_literal(sql)
                decoded = list(rust_strings(literal))
                self.assertEqual(len(decoded), 1)
                self.assertEqual(decoded[0][2].strip(), sql.strip())

    def test_unsupported_sql_escape_fails_instead_of_skipping_sql(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Rust escape"):
            list(rust_strings(r'"SELECT \\x41"'.replace("\\\\", "\\")))

    def test_unclosed_raw_string_fails(self):
        with self.assertRaisesRegex(ValueError, "Unterminated"):
            list(rust_strings('r##"SELECT 1'))

    def test_template_renderer_reads_real_filters_and_preserves_count_bound(self):
        queries = render_queries()
        self.assertIn("$9", queries["snapshot_count"])
        self.assertIn("LIMIT 100001", queries["snapshot_count"])
        self.assertNotIn("ORDER BY", queries["snapshot_count"])
        self.assertIn("row_number() OVER (ORDER BY", queries["snapshot_materialization"])
        self.assertIn("$10", queries["snapshot_materialization"])


class GateIntegrityTests(unittest.TestCase):
    def test_advisory_comparison_ignores_line_shifts_and_counts_new_findings(self):
        previous = [{"file": "src/a.rs", "code": "nesting", "message": "nested", "line": 5}]
        current = [dict(previous[0], line=10), dict(previous[0], line=20)]
        self.assertEqual(compare_findings(current, previous),
                         {"existing": 1, "new": 1, "resolved": 0})

    def test_applied_migration_edit_and_deletion_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            migration = root / "0001.sql"
            migration.write_text("SELECT 1;\n")
            history = root / "history.json"
            history.write_text(json.dumps({"0001.sql": hashlib.sha256(migration.read_bytes()).hexdigest()}))
            with patch.object(sql_quality, "ROOT", root), patch.object(sql_quality, "HISTORY", history):
                self.assertEqual(len(sql_quality.migration_history()), 1)
                migration.write_text("SELECT 2;\n")
                with self.assertRaisesRegex(ValueError, "changed or disappeared"):
                    sql_quality.migration_history()
                migration.unlink()
                with self.assertRaisesRegex(ValueError, "changed or disappeared"):
                    sql_quality.migration_history()

    def test_unregistered_dynamic_sql_fails(self):
        query = RustSql(sql_quality.ROOT / "src/example.rs", 0, 10, 1, "SELECT {input}", True)
        discovered, issues = sql_quality.check_dynamic([query])
        self.assertEqual(len(discovered), 1)
        self.assertTrue(any(issue["code"] == "DYNAMIC" for issue in issues))




if __name__ == "__main__":
    unittest.main()
