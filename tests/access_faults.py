"""Real PostgreSQL faults distinguish infrastructure uncertainty from revocation."""
import hashlib
import json
import subprocess
import uuid

import compose_smoke as t
from contract_regressions import poll

COMPANY = str(uuid.uuid4())
ROOT = f"/api/v1/companies/{COMPANY}/events"


class TableLock:
    def __init__(self, table):
        assert table in ("accounts", "tenant_outbox")
        self.table = table

    def __enter__(self):
        self.proc = subprocess.Popen(
            ["docker", "compose", "exec", "-T", "postgres", "psql", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "control", "-d", "control"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.proc.stdin.write(f"BEGIN; LOCK TABLE {self.table} IN ACCESS EXCLUSIVE MODE; SELECT pg_backend_pid();\n")
        self.proc.stdin.flush()
        assert self.proc.stdout.readline().strip().isdigit()
        return self

    def __exit__(self, *_):
        self.proc.stdin.write("ROLLBACK;\n")
        self.proc.stdin.close()
        self.proc.wait(timeout=5)
        assert self.proc.returncode == 0


def run():
    user, pair = t.create_account()
    token = pair["access_token"]
    t.sql(f"INSERT INTO companies(id,name) VALUES ('{COMPANY}','Access faults');"
          f"INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{COMPANY}','ADMIN')")
    summary, _ = t.request(ROOT + "/summary", token=token)
    summary.pop("stream_cursor")
    baseline = t.sql(f"SELECT count(*) FROM audit_events WHERE company_id='{COMPANY}' AND action='STREAM_ACCESS_DENIED'")
    with t.WebSocket(ROOT + "/ws") as ws:
        ws.send("AUTH", access_token=token, replay_id=1, after_sequence=0)
        ws.expect("SUBSCRIBED")
        ws.expect("REPLAY_COMPLETED")
        with TableLock("accounts"):
            t.sql("SELECT pg_notify('access_change','contract-fault')")
            unavailable = ws.expect("ACCESS_CHECK_UNAVAILABLE")
            assert unavailable["retry_after_ms"] > 0
            # A genuine new commit arrives while authorization is unknown.
            summary["summary_as_of_sequence"] = 1
            payload = {"schema_version": 1, "type": "TENANT_COMMIT", "tenant_sequence": 1,
                       "event_created": None, "event_updated": None, "summary_updated": summary,
                       "filter_options_updated": None}
            raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            digest = hashlib.sha256(raw.encode()).hexdigest()
            # The fixture payload has only ASCII names, integer counts and RFC3339 timestamps.
            t.sql(f"BEGIN; UPDATE company_stream_counters SET last_sequence=1 WHERE company_id='{COMPANY}';"
                  f"INSERT INTO tenant_outbox(company_id,tenant_sequence,payload,payload_sha256) "
                  f"VALUES ('{COMPANY}',1,'{raw}'::jsonb,'{digest}'); COMMIT")
        for _ in range(10):
            message = ws.receive()
            assert message["type"] in ("ACCESS_CHECK_UNAVAILABLE", "CLOSE"), message
            if message["type"] == "CLOSE":
                assert message["code"] == 1013
                break
        else:
            raise AssertionError("uncertain authorization never ended with retryable reconnect")
    assert t.sql(f"SELECT count(*) FROM audit_events WHERE company_id='{COMPANY}' AND action='STREAM_ACCESS_DENIED'") == baseline

    # Reconnect establishes a new AUTH generation and replays the suppressed commit.
    with t.WebSocket(ROOT + "/ws") as ws:
        ws.send("AUTH", access_token=token, replay_id=1, after_sequence=0)
        ws.expect("SUBSCRIBED")
        assert ws.expect("TENANT_COMMIT")["tenant_sequence"] == 1
        ws.expect("REPLAY_COMPLETED")
        # Revocation must preempt a replay query blocked inside PostgreSQL.
        with TableLock("tenant_outbox"):
            ws.send("REPLAY_REQUEST", replay_id=2, after_sequence=0)
            poll("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' "
                 "AND query ~ 'SELECT[[:space:]]+c[.]last_sequence,'", lambda n: int(n) > 0, seconds=3)
            t.sql(f"DELETE FROM memberships WHERE user_id='{user}' AND company_id='{COMPANY}'")
            assert ws.expect("ACCESS_INVALIDATED")["reason"] == "COMPANY_ACCESS_DENIED"
            assert ws.expect("CLOSE")["code"] == 1008
    print("Access faults passed: DB uncertainty pauses business without revocation audit; denial cancels replay")


if __name__ == "__main__":
    try:
        run()
    finally:
        t.sql(f"DELETE FROM company_stream_counters WHERE company_id='{COMPANY}';"
              f"DELETE FROM companies WHERE id='{COMPANY}'")
        for user in t.ACCOUNTS:
            t.sql(f"DELETE FROM accounts WHERE id='{user}'")
