"""Real process crash/reclaim. Use only on a disposable Compose stack.

Explicit opt-in is required because this stops the API container.
"""
import os
import subprocess
import time
import uuid

import compose_smoke as t
from contract_regressions import export_fixture, claims, poll

COMPANY = str(uuid.uuid4())


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, timeout=45,
                   stdout=subprocess.DEVNULL)


def run():
    assert os.getenv("CONTROL_TEST_CRASH") == "1", "Set CONTROL_TEST_CRASH=1 only for a disposable stack"
    user, pair = t.create_account()
    t.sql(f"INSERT INTO companies(id,name) VALUES ('{COMPANY}','Crash reclaim');"
          f"INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{COMPANY}','ADMIN')")
    locker = subprocess.Popen(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "control", "-d", "control"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        locker.stdin.write("BEGIN; LOCK TABLE events IN ACCESS EXCLUSIVE MODE; SELECT pg_backend_pid();\n")
        locker.stdin.flush()
        assert locker.stdout.readline().strip().isdigit()
        job = export_fixture(COMPANY, user, claims(pair["access_token"])["sid"], state="QUEUED", attempts=0)
        poll(f"SELECT status FROM export_jobs WHERE export_id='{job}'", lambda s: s == "PROCESSING", seconds=3)
        before = t.sql(f"SELECT lease_token::text||'|'||processing_started_at::text||'|'||absolute_deadline_at::text FROM export_jobs WHERE export_id='{job}'")
        # Await the real query, so the kill happens during generation, not after completion.
        poll("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' "
             "AND query ~ 'SELECT[[:space:]]+jsonb_build_object[(]'", lambda n: int(n) > 0, seconds=3)
        compose("kill", "-s", "SIGKILL", "api")
        t.sql(f"UPDATE export_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE export_id='{job}'")
    finally:
        locker.stdin.write("ROLLBACK;\n")
        locker.stdin.close()
        locker.wait(timeout=5)
        compose("up", "-d", "api")
    poll(f"SELECT status FROM export_jobs WHERE export_id='{job}'", lambda s: s in ("COMPLETED", "FAILED"))
    assert t.sql(f"SELECT status FROM export_jobs WHERE export_id='{job}'") == "COMPLETED"
    after = t.sql(f"SELECT storage_path||'|'||processing_started_at::text||'|'||absolute_deadline_at::text FROM export_jobs WHERE export_id='{job}'")
    old_token, started, deadline = before.split("|")
    key, resumed, preserved_deadline = after.split("|")
    assert resumed == started and preserved_deadline == deadline
    assert old_token not in key
    assert t.sql(f"SELECT attempts FROM export_jobs WHERE export_id='{job}'") == "2"
    old_path = f"/exports/exports/{COMPANY}/{job}/{old_token}.csv"
    deadline_cleanup = time.monotonic() + 45
    while time.monotonic() < deadline_cleanup:
        check = subprocess.run(["docker", "compose", "exec", "-T", "api", "sh", "-c", 'test ! -e "$1"', "old-object", old_path])
        if check.returncode == 0:
            break
        time.sleep(0.5)
    else:
        raise AssertionError("old worker's token-scoped orphan file was not cleaned")
    subprocess.run(["docker", "compose", "exec", "-T", "api", "sh", "-c", 'test -f "$1"', "published-object", "/exports/" + key], check=True)
    print("Worker crash passed: real SIGKILL, reclaim, new ownership token, preserved first deadline")


if __name__ == "__main__":
    try:
        run()
    finally:
        if os.getenv("CONTROL_TEST_CRASH") == "1":
            compose("up", "-d", "api")
            t.sql(f"DELETE FROM company_stream_counters WHERE company_id='{COMPANY}';"
                  f"DELETE FROM companies WHERE id='{COMPANY}'")
            for user in t.ACCOUNTS:
                t.sql(f"DELETE FROM accounts WHERE id='{user}'")
