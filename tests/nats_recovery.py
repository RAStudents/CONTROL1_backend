"""Broker outage/reconnect on a disposable Compose stack; explicit opt-in required."""
import os
import subprocess
import uuid

import compose_smoke as t
from contract_regressions import poll

COMPANY = str(uuid.uuid4())
ROOT = f"/api/v1/companies/{COMPANY}/events"


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, timeout=45, stdout=subprocess.DEVNULL)


def replay(ws, token, subscribe=True):
    if subscribe:
        ws.send("AUTH", access_token=token, replay_id=1, after_sequence=0)
        ws.expect("SUBSCRIBED")
    sequences = []
    while len(sequences) < 10:
        message = ws.receive()
        if message["type"] == "CLOSE":
            assert message["code"] == 1013
            return False
        assert message["type"] == "TENANT_COMMIT", message
        sequences.append(message["tenant_sequence"])
    assert sequences == list(range(1, 11))
    if subscribe:
        ws.expect("REPLAY_COMPLETED")
    return True


def run():
    assert os.getenv("CONTROL_TEST_NATS_OUTAGE") == "1", "Use only a disposable Compose stack"
    user, pair = t.create_account()
    token = pair["access_token"]
    t.sql(f"INSERT INTO companies(id,name) VALUES ('{COMPANY}','NATS reconnect');"
          f"INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{COMPANY}','ADMIN')")
    with t.WebSocket(ROOT + "/ws") as ws:
        ws.send("AUTH", access_token=token, replay_id=1, after_sequence=0)
        ws.expect("SUBSCRIBED")
        ws.expect("REPLAY_COMPLETED")
        try:
            compose("stop", "nats")
            t.request(ROOT + "/ingest", method="POST", token=token, key=t.key(),
                      data={"events": [t.row(f"outage-{n}") for n in range(10)]}, expect=201)
            assert t.sql(f"SELECT count(*) FROM tenant_outbox WHERE company_id='{COMPANY}'") == "10"
        finally:
            compose("up", "-d", "nats")
        poll(f"SELECT count(*) FROM tenant_outbox WHERE company_id='{COMPANY}' AND broker_dispatched_at IS NOT NULL", lambda n: n == "10")
        continued = replay(ws, token, subscribe=False)
    if not continued:
        with t.WebSocket(ROOT + "/ws") as ws:
            assert replay(ws, token)
    print("NATS recovery passed: HTTP commits survive broker outage; all sequences replay contiguously")


if __name__ == "__main__":
    try:
        run()
    finally:
        if os.getenv("CONTROL_TEST_NATS_OUTAGE") == "1":
            compose("up", "-d", "nats")
            t.sql(f"DELETE FROM company_stream_counters WHERE company_id='{COMPANY}';DELETE FROM companies WHERE id='{COMPANY}'")
            for user in t.ACCOUNTS:
                t.sql(f"DELETE FROM accounts WHERE id='{user}'")
