"""Exercise production anchor barriers through API/worker/GC transitions."""
import hashlib
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import compose_smoke as t
from contract_regressions import export_fixture, claims, poll

COMPANY = str(uuid.uuid4())
ROOT = f"/api/v1/companies/{COMPANY}/events"
NAMESPACE = 0x4354524C
KEY = int.from_bytes(hashlib.sha256(uuid.UUID(COMPANY).bytes).digest()[:4], "big", signed=True)
LOCKED = (f"SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid={NAMESPACE} "
          f"AND objid={KEY & 0xffffffff} AND objsubid=2 AND NOT granted")


class Barrier:
    def __init__(self, shared, company=COMPANY):
        self.shared = shared
        self.key = int.from_bytes(hashlib.sha256(uuid.UUID(company).bytes).digest()[:4], "big", signed=True)
        self.proc = None

    def __enter__(self):
        self.proc = subprocess.Popen(
            ["docker", "compose", "exec", "-T", "postgres", "psql", "-qAt",
             "-v", "ON_ERROR_STOP=1", "-U", "control", "-d", "control"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        suffix = "_shared" if self.shared else ""
        self.proc.stdin.write(f"SELECT pg_backend_pid() FROM (SELECT pg_advisory_lock{suffix}({NAMESPACE},{self.key})) locked;\n")
        self.proc.stdin.flush()
        self.pid = self.proc.stdout.readline().strip()
        assert self.pid.isdigit(), self.pid
        self.proc.stdin.write("BEGIN; SELECT pg_backend_pid();\n")
        self.proc.stdin.flush()
        assert self.proc.stdout.readline().strip() == self.pid
        return self

    def __exit__(self, *_):
        suffix = "_shared" if self.shared else ""
        self.proc.stdin.write(f"COMMIT; SELECT pg_backend_pid() FROM (SELECT pg_advisory_unlock{suffix}({NAMESPACE},{self.key})) unlocked;\n")
        self.proc.stdin.flush()
        assert self.proc.stdout.readline().strip() == self.pid
        self.proc.stdin.close()
        self.proc.wait(timeout=5)
        assert self.proc.returncode == 0


def run():
    user, pair = t.create_account()
    token = pair["access_token"]
    session = claims(token)["sid"]
    t.sql(f"INSERT INTO companies(id,name) VALUES ('{COMPANY}','Anchor races');"
          f"INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{COMPANY}','ADMIN')")
    with ThreadPoolExecutor(2) as executor:
        with Barrier(False):
            bootstrap = executor.submit(t.request, ROOT + "/bootstrap", token=token)
            poll(LOCKED, lambda n: int(n) > 0, seconds=3)
            assert not bootstrap.done(), "snapshot registered without a shared barrier"
        bootstrap.result()

        # Completion must stay PROCESSING while an external shared anchor barrier is held.
        with Barrier(True):
            job = export_fixture(COMPANY, user, session, state="QUEUED", attempts=0)
            poll(f"SELECT status FROM export_jobs WHERE export_id='{job}'", lambda s: s == "PROCESSING", seconds=3)
            poll(LOCKED, lambda n: int(n) > 0, seconds=3)
            assert t.sql(f"SELECT status FROM export_jobs WHERE export_id='{job}'") == "PROCESSING"
        poll(f"SELECT status FROM export_jobs WHERE export_id='{job}'", lambda s: s == "COMPLETED")
        assert t.sql(f"SELECT (lease_token IS NULL AND lease_until IS NULL AND finished_at IS NOT NULL "
                     f"AND file_sha256 IS NOT NULL AND file_size_bytes IS NOT NULL)::int FROM export_jobs WHERE export_id='{job}'") == "1"

        # Expiry is also serialized with active anchor registration/GC.
        t.sql(f"UPDATE export_jobs SET expires_at=clock_timestamp() WHERE export_id='{job}'")
        with Barrier(True):
            status = executor.submit(t.request, ROOT + f"/exports/{job}", token=token)
            poll(LOCKED, lambda n: int(n) > 0, seconds=3)
            assert not status.done()
            assert t.sql(f"SELECT status FROM export_jobs WHERE export_id='{job}'") == "COMPLETED"
        expired, _ = status.result()
        assert expired["status"] == "EXPIRED" and expired["failure_code"] == "EXPORT_EXPIRED"

        # A permission denial cannot remove the active job anchor before barrier commit.
        with Barrier(True):
            failed = export_fixture(COMPANY, user, session, state="QUEUED", attempts=0)
            t.sql(f"UPDATE memberships SET role='OBSERVER' WHERE user_id='{user}' AND company_id='{COMPANY}'")
            poll(f"SELECT status FROM export_jobs WHERE export_id='{failed}'", lambda s: s == "PROCESSING", seconds=3)
            poll(LOCKED, lambda n: int(n) > 0, seconds=3)
            assert t.sql(f"SELECT status FROM export_jobs WHERE export_id='{failed}'") == "PROCESSING"
        poll(f"SELECT status FROM export_jobs WHERE export_id='{failed}'", lambda s: s == "FAILED")
        assert t.sql(f"SELECT failure_code FROM export_jobs WHERE export_id='{failed}'") == "EXPORT_ACCESS_REVOKED"
        t.sql(f"UPDATE memberships SET role='ADMIN' WHERE user_id='{user}' AND company_id='{COMPANY}'")

        # The background cleaner must not remove a snapshot while the shared barrier is held.
        boot, _ = t.request(ROOT + "/bootstrap", token=token)
        snapshot = boot["events"]["pagination"]["snapshot_id"]
        with Barrier(True):
            t.sql(f"UPDATE event_snapshots SET expires_at=clock_timestamp() WHERE snapshot_id='{snapshot}'")
            poll(LOCKED, lambda n: int(n) > 0, seconds=40)
            assert t.sql(f"SELECT count(*) FROM event_snapshots WHERE snapshot_id='{snapshot}'") == "1"
        poll(f"SELECT count(*) FROM event_snapshots WHERE snapshot_id='{snapshot}'", lambda n: n == "0")
    print("Anchor races passed: backend affinity, registration, COMPLETED/FAILED/EXPIRED and snapshot cleanup")


if __name__ == "__main__":
    try:
        run()
    finally:
        t.sql(f"DELETE FROM company_stream_counters WHERE company_id='{COMPANY}';"
              f"DELETE FROM companies WHERE id='{COMPANY}'")
        for user in t.ACCOUNTS:
            t.sql(f"DELETE FROM accounts WHERE id='{user}'")
