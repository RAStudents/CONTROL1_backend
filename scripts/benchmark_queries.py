"""Compare real query plans in a newly created disposable Compose database only."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import uuid

from sql_source import ROOT
from sql_templates import render_queries

COMPANY = "00000000-0000-0000-0000-000000000001"
OTHER_COMPANY = "00000000-0000-0000-0000-000000000002"
FILTER_TYPES = "uuid, bigint, text, text, timestamptz, timestamptz, text[], text[], text"
FILTER_VALUES = f"'{COMPANY}', 2000000, NULL, NULL, NULL, NULL, '{{}}', '{{}}', 'ALL'"


class DisposableDatabase:
    def __init__(self):
        if not os.environ.get("COMPOSE_PROJECT_NAME"):
            raise ValueError("Set COMPOSE_PROJECT_NAME to the isolated test stack")
        self.name = "control_quality_bench_" + uuid.uuid4().hex

    def command(self, *arguments, sql=None):
        command = ["docker", "compose", "exec", "-T", "postgres", *arguments]
        result = subprocess.run(command, input=sql, text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(f"Disposable database command failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def sql(self, statement):
        return self.command("psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "control",
                            "-d", self.name, sql=statement)

    def __enter__(self):
        self.command("createdb", "-U", "control", self.name)
        try:
            for path in sorted((ROOT / "migrations").glob("*.sql")):
                sql = path.read_text()
                statement = sql if sql.startswith("-- no-transaction") else f"BEGIN;\n{sql}\nCOMMIT;"
                self.sql(statement)
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *_):
        self.command("dropdb", "-U", "control", self.name)


def seed(database, rows):
    database.sql(f"""
        INSERT INTO companies (id, name) VALUES
            ('{COMPANY}', 'Main benchmark tenant'),
            ('{OTHER_COMPANY}', 'Other benchmark tenant');
        UPDATE company_stream_counters SET last_sequence = 2000000
        WHERE company_id IN ('{COMPANY}', '{OTHER_COMPANY}');
        INSERT INTO events (
            company_id, id, created_sequence, event_type_id, category_code,
            location_id, device_id, occurred_at, received_at, preview_url
        )
        SELECT tenant, 'event-' || lpad(number::text, 8, '0'), number,
            'motion', 'SECURITY', 'entrance', 'camera',
            '2026-01-01'::timestamptz + number * INTERVAL '1 second',
            '2026-01-01'::timestamptz, 'https://cdn.example.test/preview'
        FROM (VALUES ('{COMPANY}'::uuid, {rows}),
                     ('{OTHER_COMPANY}'::uuid, {rows // 5})) AS tenants(tenant, count)
        CROSS JOIN LATERAL generate_series(1, count) AS series(number);
        INSERT INTO event_versions (
            company_id, event_id, event_version, classification_version,
            acknowledgement_version, valid_from_sequence, valid_to_sequence, status, is_erroneous
        )
        SELECT company_id, id, 1, 1, 1, created_sequence,
            CASE WHEN created_sequence % 5 = 0 THEN 1500000 END, 'NEW', false FROM events;
        INSERT INTO event_versions (
            company_id, event_id, event_version, classification_version,
            acknowledgement_version, valid_from_sequence, status, is_erroneous
        )
        SELECT company_id, id, 2, 1, 1, 1500000, 'NEW', false
        FROM events WHERE created_sequence % 5 = 0;
        INSERT INTO event_snapshots (
            snapshot_id, company_id, user_id, filter_hash, as_of_sequence, total_count, expires_at
        ) VALUES ('benchmark-snapshot', '{COMPANY}', '{COMPANY}', 'benchmark',
                  2000000, {rows}, clock_timestamp() + INTERVAL '30 minutes');
        INSERT INTO event_snapshot_items (snapshot_id, company_id, position, event_id, occurred_at)
        SELECT 'benchmark-snapshot', company_id, created_sequence - 1, id, occurred_at
        FROM events WHERE company_id = '{COMPANY}';
        ANALYZE events;
        ANALYZE event_versions;
        ANALYZE event_snapshots;
        ANALYZE event_snapshot_items;
    """)


def validate_rendered(database, queries):
    for name, sql in queries.items():
        if name == "affinity_probe":
            database.sql(sql)
            continue
        types = FILTER_TYPES
        if name == "current":
            types = "uuid, text"
        elif name == "page":
            types = "uuid, bigint, text, integer, bigint"
        elif name == "snapshot_materialization":
            types += ", text"
        database.sql(f"PREPARE checked ({types}) AS {sql}; DEALLOCATE checked;")


def plan(database, sql, *, before_index=False, prepare=""):
    drop = "DROP INDEX idx_event_snapshot_items_event;" if before_index else ""
    output = database.sql(
        f"BEGIN; {drop} {prepare} EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}; ROLLBACK;"
    )
    return json.loads(output)[0]


def compare(database, before, after, *, prepare="", remove_index=False):
    measurements = {"before": [], "after": []}
    # Warm both variants, then alternate their order to reduce cache-order bias.
    for variant in measurements:
        plan(database, before if variant == "before" else after,
             before_index=remove_index and variant == "before", prepare=prepare)
    for iteration in range(5):
        order = ("before", "after") if iteration % 2 == 0 else ("after", "before")
        for variant in order:
            measurements[variant].append(plan(
                database, before if variant == "before" else after,
                before_index=remove_index and variant == "before", prepare=prepare,
            ))
    return {
        "median_execution_ms": {
            variant: statistics.median(item["Execution Time"] for item in plans)
            for variant, plans in measurements.items()
        },
        "plans": measurements,
    }


def measure(database, queries):
    before = f"SELECT COUNT(*)::int FROM ({queries['matched_core']} LIMIT 100001) matched"
    after = queries["snapshot_count"]
    prepare = f"PREPARE previous ({FILTER_TYPES}) AS {before}; "
    prepare += f"PREPARE current ({FILTER_TYPES}) AS {after};"
    values = database.sql(
        f"{prepare} EXECUTE previous ({FILTER_VALUES}); EXECUTE current ({FILTER_VALUES});"
    ).splitlines()
    if len(values) != 2 or values[0] != values[1]:
        raise ValueError(f"Bounded count results differ: {values}")
    counts = compare(database, f"EXECUTE previous ({FILTER_VALUES})",
                     f"EXECUTE current ({FILTER_VALUES})", prepare=prepare)
    counts["equivalent_count"] = int(values[0])
    counts["before_sql"] = before
    counts["after_sql"] = after

    delete = f"""DELETE FROM events WHERE company_id = '{COMPANY}'
        AND created_sequence <= 100 RETURNING id"""
    results = []
    for drop in ("DROP INDEX idx_event_snapshot_items_event;", ""):
        results.append(database.sql(f"""BEGIN; {drop}
            WITH removed AS ({delete}) SELECT jsonb_agg(id ORDER BY id) FROM removed;
            ROLLBACK;"""))
    if results[0] != results[1] or len(json.loads(results[0])) != 100:
        raise ValueError("Cascade delete results differ")
    cascade = compare(database, delete, delete, remove_index=True)
    cascade["equivalent_deleted_rows"] = 100
    cascade["sql"] = delete
    return {"bounded_snapshot_count": counts, "snapshot_item_fk_cascade": cascade}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disposable", required=True, action="store_true")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--output", default="query-plans.json")
    args = parser.parse_args()
    if not 1000 <= args.rows <= 1000000:
        parser.error("--rows must be between 1000 and 1000000")
    with DisposableDatabase() as database:
        seed(database, args.rows)
        queries = render_queries()
        validate_rendered(database, queries)
        report = {
            "postgres_version": database.sql("SHOW server_version;"),
            "dataset": {"main_tenant_events": args.rows, "other_tenant_events": args.rows // 5,
                        "historical_versions_fraction": 0.2, "snapshot_items": args.rows},
            "method": "ANALYZE fixtures; warm both; 5 alternating runs; DML rolled back",
            "validated_rendered_queries": list(queries),
            "pg_stat_statements_available": database.sql(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');"
            ) == "t",
            "comparisons": measure(database, queries),
        }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    for name, result in report["comparisons"].items():
        print(name, result["median_execution_ms"])


if __name__ == "__main__":
    main()
