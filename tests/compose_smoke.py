"""Exercise an isolated set of records against the running Compose services."""
import base64
import csv
import io
import json
import os
import secrets
import socket
import struct
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

BASE = os.getenv("CONTROL_TEST_URL", "http://127.0.0.1:8080")
CDN = os.getenv("CONTROL_TEST_CDN", "cdn.example.test")
COMPANY = str(uuid.uuid4())
OTHER = str(uuid.uuid4())
ROOT = f"/api/v1/companies/{COMPANY}"
EVENTS = ROOT + "/events"
ACCOUNTS = []


def sql(query):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql",
         "-v", "ON_ERROR_STOP=1", "-U", "control", "-d", "control", "-At"],
        input=query, check=True, text=True, capture_output=True
    ).stdout.strip()


def request(path, *, method="GET", data=None, token=None, key=None, extra=None, expect=200):
    headers = dict(extra or {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if key:
        headers["Idempotency-Key"] = key
    if data is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(data).encode()
    req = Request(BASE + path, data=data, headers=headers, method=method)
    try:
        response = urlopen(req, timeout=20)
    except HTTPError as err:
        response = err
    with response:
        raw = response.read()
        body = json.loads(raw) if raw and "json" in response.headers.get("Content-Type", "") else raw
        assert response.code == expect, (method, path, response.code, expect, body)
        return body, response.headers


def create_account(global_permissions=""):
    email = f"smoke-{uuid.uuid4()}@example.test"
    password = secrets.token_urlsafe(32)
    env = dict(os.environ, ACCOUNT_EMAIL=email, ACCOUNT_PASSWORD=password,
               ACCOUNT_GLOBAL_PERMISSIONS=global_permissions)
    proc = subprocess.run(
        ["docker", "compose", "exec", "-T", "-e", "ACCOUNT_EMAIL",
         "-e", "ACCOUNT_PASSWORD", "-e", "ACCOUNT_GLOBAL_PERMISSIONS",
         "api", "control-backend", "create-account"],
        env=env, check=True, capture_output=True, text=True
    )
    user = str(uuid.UUID(proc.stdout.splitlines()[-1]))
    ACCOUNTS.append(user)
    tokens, _ = request("/api/auth/login", method="POST", data={"email": email, "password": password})
    return user, tokens


class WebSocket:
    """Minimal RFC 6455 client for complete text and close frames."""
    def __init__(self, path):
        origin = urlsplit(BASE)
        self.sock = socket.create_connection((origin.hostname, origin.port or 80), timeout=10)
        self.read = self.sock.makefile("rb")
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall(
            (f"GET {path} HTTP/1.1\r\nHost: {origin.netloc}\r\nUpgrade: websocket\r\n"
             f"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\n\r\n").encode()
        )
        status = self.read.readline()
        assert b" 101 " in status, status
        while self.read.readline() not in (b"\r\n", b""):
            pass

    def send(self, kind, **fields):
        body = json.dumps({"schema_version": 1, "type": kind, **fields}).encode()
        mask = os.urandom(4)
        length = bytes([128 + len(body)]) if len(body) < 126 else b"\xfe" + struct.pack(">H", len(body))
        self.sock.sendall(b"\x81" + length + mask +
                          bytes(byte ^ mask[i % 4] for i, byte in enumerate(body)))

    def receive(self):
        header = self.read.read(2)
        assert len(header) == 2, "socket closed without a frame"
        opcode, length = header[0] & 15, header[1] & 127
        if length == 126:
            length = struct.unpack(">H", self.read.read(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self.read.read(8))[0]
        body = self.read.read(length)
        if opcode == 8:
            return {"type": "CLOSE", "code": struct.unpack(">H", body[:2])[0]}
        assert opcode == 1, (opcode, body)
        return json.loads(body)

    def expect(self, kind):
        frame = self.receive()
        assert frame["type"] == kind, frame
        return frame

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.read.close()
        self.sock.close()


def row(id_):
    return {"id": id_, "event_type_id": "helmet", "category_code": "safety",
            "location_id": "workshop", "device_id": "camera",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "preview_url": f"https://{CDN}/example.jpg"}


def key():
    return str(uuid.uuid4())


def cancel_replay(token):
    """Hold a table lock, then verify a superseded replay SQL is cancelled."""
    with WebSocket(EVENTS + "/ws") as ws:
        ws.send("AUTH", access_token=token, replay_id=1, after_sequence=7)
        ws.expect("SUBSCRIBED")
        ws.expect("REPLAY_COMPLETED")
        with subprocess.Popen(
            ["docker", "compose", "exec", "-T", "postgres", "psql", "-qAt",
             "-v", "ON_ERROR_STOP=1", "-U", "control", "-d", "control"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
        ) as locker:
            locker.stdin.write("BEGIN; LOCK TABLE tenant_outbox IN ACCESS EXCLUSIVE MODE; SELECT pg_backend_pid();\n")
            locker.stdin.flush()
            assert locker.stdout.readline().strip().isdigit()
            try:
                ws.send("REPLAY_REQUEST", replay_id=2, after_sequence=6)
                pid = ""
                for _ in range(20):
                    pid = sql("SELECT pid FROM pg_stat_activity WHERE pid<>pg_backend_pid() "
                              "AND wait_event_type='Lock' AND query ~ 'SELECT[[:space:]]+c[.]last_sequence,' LIMIT 1")
                    if pid:
                        break
                    time.sleep(0.05)
                assert pid.isdigit(), "the first replay query did not block"
                ws.send("REPLAY_REQUEST", replay_id=3, after_sequence=7)
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    active = sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND wait_event_type='Lock'")
                    if active == "0":
                        break
                    time.sleep(0.05)
                assert active == "0", "superseded PostgreSQL query is still blocked"
            finally:
                locker.stdin.write("ROLLBACK;\n")
                locker.stdin.close()
        assert ws.expect("SUBSCRIBED")["replay_id"] == 3
        assert ws.expect("REPLAY_COMPLETED")["replay_id"] == 3


def smoke():
    request("/healthz")
    user, tokens = create_account("COMPANIES_MANAGE")
    viewer, reader_tokens = create_account()
    token, read_token = tokens["access_token"], reader_tokens["access_token"]
    grants = key()
    sql(f"""
        INSERT INTO companies(id,name,business_timezone) VALUES
          ('{COMPANY}','Smoke A {COMPANY}','Europe/Riga'),('{OTHER}','Smoke Z {COMPANY}','UTC');
        INSERT INTO memberships(user_id,company_id,permissions) VALUES
          ('{user}','{COMPANY}',ARRAY['EVENTS_READ','EVENTS_REVIEW','REPORTS_EXPORT','DICTIONARIES_MANAGE','DEVICES_CREATE']),
          ('{viewer}','{COMPANY}',ARRAY[]::text[]);
        UPDATE memberships SET role='OBSERVER' WHERE user_id='{viewer}' AND company_id='{COMPANY}';
        INSERT INTO support_grants(grant_id,company_id,user_id,mode,ticket_reference,granted_by_user_id,expires_at)
          VALUES ('{grants}','{COMPANY}','{user}','SUPPORT','test','{user}',clock_timestamp()+interval '15 minutes');
    """)
    context, _ = request("/api/v1/auth/context", token=token)
    assert context["user_id"] == user and context["can_browse_all_companies"]
    access, headers = request(ROOT + "/access", token=token)
    assert access["permission_sources"]["EVENTS_READ"] == ["DIRECT_MEMBERSHIP"]
    assert access["permission_sources"]["DEVICE_RESTART"] == ["SUPPORT_GRANT"]
    request(ROOT + "/access", token=token, extra={"If-None-Match": headers["ETag"]})
    _, reader_headers = request(ROOT + "/access", token=read_token)
    request(ROOT + "/access", token=read_token, extra={"If-None-Match": reader_headers["ETag"]}, expect=304)
    page_args = {"search": COMPANY, "limit": 1}
    vendor_page, _ = request("/api/v1/vendor/companies?" + urlencode(page_args), token=token)
    assert vendor_page["has_more"] and vendor_page["items"][0]["id"] == COMPANY
    page_args["cursor"] = vendor_page["next_cursor"]
    vendor_next, _ = request("/api/v1/vendor/companies?" + urlencode(page_args), token=token)
    assert vendor_next["items"][0]["id"] == OTHER
    request("/api/v1/vendor/companies/health", token=token)
    config = {"event_types": [{"id": "helmet", "name": "\u00a0=1+1", "icon_key": "helmet"}],
              "locations": [{"id": "workshop", "name": "Workshop"}],
              "devices": [{"id": "camera", "name": "Camera", "location_id": "workshop", "health": "OK"}],
              "categories": [{"id": "safety", "name": "Safety", "score": 200, "trend": "STABLE"}]}
    change, _ = request(EVENTS + "/configuration", method="PUT", token=token, data=config)
    assert change["stream_cursor"] == 1
    options, options_headers = request(EVENTS + "/filter-options", token=token)
    assert options["locations"][0]["devices"][0]["id"] == "camera"
    request(EVENTS + "/filter-options", token=token,
            extra={"If-None-Match": options_headers["ETag"]}, expect=304)

    def ingest(ids):
        return request(EVENTS + "/ingest", method="POST", data={"events": [row(i) for i in ids]},
                       token=token, key=key(), expect=201)[0]

    with ThreadPoolExecutor(2) as executor:
        batches = sorted(executor.map(ingest, [["aaa", "aaa-extra"], ["aaa-more", "other"]]),
                         key=lambda batch: batch["first_sequence"])
    assert [(b["first_sequence"], b["last_sequence"]) for b in batches] == [(2, 3), (4, 5)]
    boot, _ = request(EVENTS + "/bootstrap?limit=1&search_id=aaa", token=token)
    cursor = boot["events"]["pagination"]["next_cursor"]
    assert boot["events"]["items"][0]["id"] == "aaa"
    assert boot["stream_cursor"] == boot["summary"]["summary_as_of_sequence"] == 5
    request(EVENTS + "?" + urlencode({"cursor": cursor}), token=read_token, expect=400)
    request(EVENTS + "?" + urlencode({"cursor": cursor, "search_id": "other"}), token=token, expect=400)
    request(EVENTS + "/summary", token=token)
    request(EVENTS + "/aaa/acknowledge", method="PUT", token=read_token, expect=403)

    # Empty replay, a live commit, replay rewind and a protocol-invalid future ACK.
    with WebSocket(EVENTS + "/ws") as ws:
        ws.send("AUTH", access_token=token, replay_id=1, after_sequence=5)
        subscribed = ws.expect("SUBSCRIBED")
        assert subscribed["replay_empty"] and subscribed["replay_from_sequence"] is None
        ws.expect("REPLAY_COMPLETED")
        ws.send("HEARTBEAT", client_confirmed_seq=5)
        heartbeat = ws.expect("HEARTBEAT_ACK")
        assert heartbeat["last_replay_written_seq"] is None
        ack, _ = request(EVENTS + "/aaa/acknowledge", method="PUT", token=token)
        assert ack["event_version"] == 2 and ack["classification_version"] == 1
        commit = ws.expect("TENANT_COMMIT")
        assert commit["tenant_sequence"] == 6 and len(commit["payload_sha256"]) == 64
        ws.send("REPLAY_REQUEST", replay_id=2, after_sequence=5)
        assert ws.expect("SUBSCRIBED")["replay_from_sequence"] == 6
        assert ws.expect("TENANT_COMMIT")["replay_id"] == 2
        ws.expect("REPLAY_COMPLETED")
        ws.send("HEARTBEAT", client_confirmed_seq=7)
        assert ws.expect("CLOSE")["code"] == 1002

    marker = key()
    marked, _ = request(EVENTS + "/aaa/erroneous", method="POST", token=token, key=marker,
                        data={"expected_classification_version": 1})
    assert marked["status"] == "ACKNOWLEDGED" and marked["classification_version"] == 2
    assert marked["acknowledgement_version"] == 2
    replay, headers = request(EVENTS + "/aaa/erroneous", method="POST", token=token, key=marker,
                              data={"expected_classification_version": 1})
    assert replay == marked and headers["X-Idempotency-Replayed"] == "true"
    request(EVENTS + "/aaa/erroneous", method="POST", token=token, key=marker,
            data={"expected_classification_version": 2}, expect=422)
    repeated, _ = request(EVENTS + "/aaa/erroneous", method="POST", token=token, key=key(),
                          data={"expected_classification_version": 1})
    assert repeated == marked
    next_page, _ = request(EVENTS + "?" + urlencode({"cursor": cursor}), token=token)
    assert next_page["pagination"]["total_count"] == 3 and len(next_page["items"]) == 2

    export_key = key()
    export_body = {"filters": {"validity": "ALL"}, "format_preset": "EXCEL_SEMICOLON_BOM"}
    export, _ = request(EVENTS + "/exports", method="POST", data=export_body, token=token, key=export_key, expect=202)
    export_replay, replay_headers = request(EVENTS + "/exports", method="POST", data=export_body,
                                            token=token, key=export_key, expect=202)
    assert export == export_replay and replay_headers["X-Idempotency-Replayed"] == "true"
    for _ in range(50):
        status, _ = request(export["status_url"], token=token)
        if status["status"] in ("COMPLETED", "FAILED"):
            break
        time.sleep(0.2)
    assert status["status"] == "COMPLETED", status
    contents, _ = request(status["download_url"], token=token)
    assert contents.startswith(b"\xef\xbb\xbf") and b"\r\n" in contents
    records = list(csv.reader(io.StringIO(contents.decode("utf-8-sig")), delimiter=";"))
    assert len(records) == 5 and all(len(r) == 14 for r in records)
    assert all(r[4] == "'\u00a0=1+1" for r in records[1:])
    assert sql(f"SELECT (absolute_deadline_at=processing_started_at+interval '30 minutes')::int FROM export_jobs WHERE export_id='{export['export_id']}'") == "1"
    sql(f"UPDATE export_jobs SET expires_at=clock_timestamp()-interval '1 second' WHERE export_id='{export['export_id']}'")
    request(status["download_url"], token=token, expect=410)
    expired, _ = request(export["status_url"], token=token)
    assert expired["status"] == "EXPIRED" and expired["failure_code"] == "EXPORT_EXPIRED"

    if os.getenv("CONTROL_TEST_CANCEL") == "1":
        cancel_replay(token)

    sql(f"UPDATE companies SET restriction='BILLING_DEBT' WHERE id='{COMPANY}'")
    restricted, _ = request(ROOT + "/access", token=token)
    assert restricted["access_status"] == "RESTRICTED"
    request(ROOT + f"/support-grants/{grants}/billing-override", method="PATCH", token=token,
            data={"allow_billing_override": True})
    override, _ = request(ROOT + "/access", token=token)
    assert override["access_status"] == "ACTIVE_SUPPORT_OVERRIDE"
    assert override["tenant_permissions"] == ["DEVICES_READ", "EVENTS_READ"]
    request(EVENTS + "/aaa/acknowledge", method="PUT", token=token, expect=403)


    # A support command creates the operation and idempotency result atomically.
    rejected = ROOT + "/devices/camera/commands/DEVICE_RESTART"
    baseline = sql(f"SELECT count(*) FROM device_operations WHERE company_id='{COMPANY}'")
    request(rejected, method="POST", token=read_token, data={}, key=key(), expect=403)
    assert sql(f"SELECT count(*) FROM device_operations WHERE company_id='{COMPANY}'") == baseline

    command = ROOT + "/devices/camera/commands/DEVICE_DIAGNOSTICS"
    command_key = key()
    operation, _ = request(command, method="POST", data={}, token=token, key=command_key, expect=202)
    operation_url = ROOT + "/operations/" + operation["operation_id"]
    request(f"/api/v1/companies/{OTHER}/operations/" + operation["operation_id"], token=token, expect=404)
    for _ in range(100):
        operation_status, _ = request(operation_url, token=token)
        if operation_status["status"] == "TIMED_OUT":
            break
        time.sleep(0.2)
    assert operation_status["status"] == "TIMED_OUT"
    unknown, _ = request(command, method="POST", data={}, token=token, key=command_key, expect=409)
    assert unknown["error"]["code"] == "IDEMPOTENCY_OUTCOME_UNKNOWN"
    assert unknown["error"]["details"]["operation_id"] == operation["operation_id"]

    # Membership removal is delivered to an already-open socket by PostgreSQL NOTIFY.
    sql(f"UPDATE companies SET restriction=NULL WHERE id='{COMPANY}'")
    with WebSocket(EVENTS + "/ws") as ws:
        ws.send("AUTH", access_token=read_token, replay_id=1, after_sequence=7)
        ws.expect("SUBSCRIBED")
        ws.expect("REPLAY_COMPLETED")
        sql(f"DELETE FROM memberships WHERE user_id='{viewer}' AND company_id='{COMPANY}'")
        assert ws.expect("ACCESS_INVALIDATED")["reason"] == "COMPANY_ACCESS_DENIED"
        assert ws.expect("CLOSE")["code"] == 1008

    if os.getenv("CONTROL_TEST_SNAPSHOT_LIMIT") == "1":
        sql(f"""
            BEGIN;
            UPDATE company_stream_counters SET last_sequence=100001 WHERE company_id='{OTHER}';
            INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{OTHER}','OBSERVER');
            INSERT INTO events(company_id,id,created_sequence,event_type_id,category_code,location_id,device_id,occurred_at,received_at,preview_url)
            SELECT '{OTHER}', i::text, i, 'helmet','safety','workshop','camera', clock_timestamp(),clock_timestamp(),''
            FROM generate_series(1,100001) AS x(i);
            INSERT INTO event_versions(company_id,event_id,event_version,classification_version,acknowledgement_version,valid_from_sequence,status,is_erroneous)
            SELECT '{OTHER}', i::text,1,1,1,i,'NEW',false FROM generate_series(1,100001) AS x(i);
            COMMIT;
            ANALYZE events;
            ANALYZE event_versions;
        """)
        failure, _ = request(f"/api/v1/companies/{OTHER}/events/bootstrap", token=token, expect=422)
        assert failure["error"]["code"] == "SNAPSHOT_TOO_LARGE"
        assert sql(f"SELECT count(*) FROM event_snapshots WHERE company_id='{OTHER}'") == "0"
        # Positive ceiling test measures the actual materialization path as well.
        sql(f"DELETE FROM events WHERE company_id='{OTHER}' AND id='100001'")
        started = time.monotonic()
        ceiling, _ = request(f"/api/v1/companies/{OTHER}/events/bootstrap", token=token)
        elapsed = time.monotonic() - started
        assert ceiling['events']['pagination']['total_count'] == 100000
        assert elapsed <= float(os.getenv('CONTROL_MAX_SNAPSHOT_SECONDS', '15')), elapsed
        print(f"100000-item snapshot materialized in {elapsed:.3f}s")

    refreshed, _ = request("/api/auth/refresh", method="POST", data={"refresh_token": tokens["refresh_token"]})
    assert refreshed["refresh_token"] != tokens["refresh_token"]
    request("/api/auth/refresh", method="POST", data={"refresh_token": tokens["refresh_token"]}, expect=401)
    print("Compose smoke passed: access, pagination, ingestion, WebSocket, review, exports, refresh")


if __name__ == "__main__":
    try:
        smoke()
    finally:
        company_ids = ",".join(f"'{id_}'" for id_ in (COMPANY, OTHER))
        sql(f"SET statement_timeout='30s'; DELETE FROM device_operations WHERE company_id IN ({company_ids});"
            f"DELETE FROM company_stream_counters WHERE company_id IN ({company_ids});"
            f"DELETE FROM companies WHERE id IN ({company_ids});")
        if ACCOUNTS:
            users = ",".join(f"'{id_}'" for id_ in ACCOUNTS)
            sql(f"DELETE FROM accounts WHERE id IN ({users});")
