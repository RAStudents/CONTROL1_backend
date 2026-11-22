"""Deterministic backend contract regressions against an isolated Compose fixture.

Run after compose_smoke.py. No production data or existing tenants are changed.
"""
import base64
import csv
import hashlib
import io
import json
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlencode

import compose_smoke as t

COMPANIES = [str(uuid.uuid4()), str(uuid.uuid4())]
A, B = COMPANIES
ROOT = f"/api/v1/companies/{A}"
EVENTS = ROOT + "/events"


def poll(statement, predicate, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = t.sql(statement)
        if predicate(value):
            return value
        time.sleep(0.2)
    raise AssertionError((statement, value))


def scalar(statement):
    return t.sql(statement)


def claims(token):
    return json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=="))


def export_fixture(company, user, session, *, state="PROCESSING", age="0 minutes",
                   lease="5 minutes", attempts=1, queue_age="23 hours"):
    id_ = str(uuid.uuid4())
    # Production constraints remain enabled: historical queue TTL and first-claim
    # deadline are always derived from their actual reference timestamps.
    if state == "QUEUED":
        lifecycle = "NULL,NULL,NULL,NULL"
    else:
        lifecycle = f"now-interval '{age}',now-interval '{age}'+interval '30 minutes','{uuid.uuid4()}',now+interval '{lease}'"
    t.sql(f"""WITH ts AS (SELECT clock_timestamp() now)
      INSERT INTO export_jobs(export_id,company_id,user_id,session_id,status,snapshot_sequence,
       filters_json,format_preset,include_media,created_at,queue_expires_at,processing_started_at,
       absolute_deadline_at,lease_token,lease_until,attempts)
      SELECT '{id_}','{company}','{user}','{session}','{state}',0,'{{}}','STANDARD_COMMA_UTF8',false,
       now-interval '{queue_age}',now-interval '{queue_age}'+interval '24 hours',{lifecycle},{attempts} FROM ts""")
    return id_


def publish(company, payload):
    """Publish an intentionally delayed/invalid stream packet using the real broker."""
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(raw).hexdigest()
    headers = f"NATS/1.0\r\nControl-Payload-Sha256: {digest}\r\n\r\n".encode()
    protocol = (b'CONNECT {"verbose":false,"headers":true,"protocol":1}\r\n' +
                f"HPUB events.company.{company} {len(headers)} {len(headers)+len(raw)}\r\n".encode() +
                headers + raw + b"\r\nPING\r\n")
    # No dynamic shell interpolation: the complete protocol travels over stdin.
    subprocess.run(["docker", "compose", "exec", "-T", "nats", "sh", "-c",
                    "nc -w 1 127.0.0.1 4222"], input=protocol,
                   check=True, stdout=subprocess.DEVNULL, timeout=5)


def conflicting_duplicate_latches_dispatcher(user, token):
    company = str(uuid.uuid4())
    events = f"/api/v1/companies/{company}/events"
    try:
        t.sql(f"INSERT INTO companies(id,name) VALUES ('{company}','Integrity fixture');"
              f"INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{company}','ADMIN')")
        with t.WebSocket(events + "/ws") as first, t.WebSocket(events + "/ws") as second:
            for ws in [first, second]:
                ws.send("AUTH", access_token=token, replay_id=1, after_sequence=0)
                ws.expect("SUBSCRIBED")
                ws.expect("REPLAY_COMPLETED")
            t.request(events + "/ingest", method="POST", token=token, key=t.key(),
                      data={"events": [t.row("duplicate-source")]}, expect=201)
            for ws in [first, second]:
                assert ws.expect("TENANT_COMMIT")["tenant_sequence"] == 1
            payload = json.loads(t.sql(
                f"SELECT payload FROM tenant_outbox WHERE company_id='{company}' AND tenant_sequence=1"
            ))
            payload["event_created"]["device_name"] = "conflicting valid digest"
            publish(company, payload)
            for ws in [first, second]:
                assert ws.expect("RESYNC_REQUIRED")["reason"] == "STREAM_INTEGRITY_VIOLATION"
            boot, _ = t.request(events + "/bootstrap", token=token)
            # A fresh replay cannot bypass the integrity latch while receivers remain attached.
            for ws in [first, second]:
                ws.send("REPLAY_REQUEST", replay_id=2, after_sequence=boot["stream_cursor"])
                assert ws.expect("RESYNC_REQUIRED")["reason"] == "STREAM_INTEGRITY_VIOLATION"
    finally:
        t.sql(f"DELETE FROM company_stream_counters WHERE company_id='{company}';"
              f"DELETE FROM companies WHERE id='{company}'")


def run():
    user, pair = t.create_account("COMPANIES_MANAGE")
    other, other_pair = t.create_account()
    token, other_token = pair["access_token"], other_pair["access_token"]
    sid = claims(token)["sid"]
    sid_other = claims(other_token)["sid"]
    for company in COMPANIES:
        t.sql(f"INSERT INTO companies(id,name) VALUES ('{company}','Contract regression {company}');"
              f"INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{company}','ADMIN'),"
              f"('{other}','{company}','ADMIN')")
    configuration = {"event_types": [{"id": "helmet", "name": "\t=1+1", "icon_key": "helmet"}],
                     "locations": [{"id": "workshop", "name": "Workshop"}],
                     "devices": [{"id": "camera", "name": "Camera", "location_id": "workshop", "health": "OK"}]}
    t.request(EVENTS + "/configuration", method="PUT", token=token, data=configuration)
    ids = ["aaa", "aaa-newer", "aaa-last", "cam_01%test\\2", "camX01anything", "other"]
    t.request(EVENTS + "/ingest", method="POST", token=token, key=t.key(),
              data={"events": [t.row(id_) for id_ in ids]}, expect=201)

    def counter():
        return scalar(f"SELECT last_sequence FROM company_stream_counters WHERE company_id='{A}'")

    # Stable exact-first ordering across every size-1 page, including a concurrent mutation.
    page, _ = t.request(EVENTS + "?limit=1&search_id=aaa", token=token)
    found = [page["items"][0]["id"]]
    assert found == ["aaa"]
    while page["pagination"]["next_cursor"]:
        page, _ = t.request(EVENTS + "?" + urlencode({"cursor": page["pagination"]["next_cursor"], "limit": 1}), token=token)
        found.extend(row["id"] for row in page["items"])
    assert set(found) == {"aaa", "aaa-newer", "aaa-last"} and len(found) == 3
    page, _ = t.request(EVENTS + "?" + urlencode({"search_id": "cam_01%test\\2"}), token=token)
    assert [row["id"] for row in page["items"]] == ["cam_01%test\\2"]
    for query in ["limit=0", "sort=OTHER", "validity=UNKNOWN", "from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z"]:
        err, _ = t.request(EVENTS + "?" + query, token=token, expect=422)
        assert err["error"]["code"] == "FILTER_INVALID" and err["error"]["details"]["invalid_fields"]

    # Concurrent orthogonal mutations, stale no-op, and transactional idempotency.
    def acknowledge():
        return t.request(EVENTS + "/aaa/acknowledge", method="PUT", token=token)[0]
    def erroneous():
        return t.request(EVENTS + "/aaa/erroneous", method="POST", token=other_token,
                         key=t.key(), data={"expected_classification_version": 1})[0]
    before = int(counter())
    with ThreadPoolExecutor(2) as ex:
        responses = [ex.submit(acknowledge), ex.submit(erroneous)]
        for response in responses:
            response.result()
    assert int(counter()) == before + 2
    before = counter()
    acknowledge()
    repeated = erroneous()
    assert repeated["is_erroneous"] and repeated["status"] == "ACKNOWLEDGED" and counter() == before
    marker = t.key()
    def same_key():
        return t.request(EVENTS + "/other/erroneous", method="POST", token=token,
                         key=marker, data={"expected_classification_version": 1})
    with ThreadPoolExecutor(2) as ex:
        responses = list(ex.map(lambda _: same_key(), range(2)))
    assert sum(headers.get("X-Idempotency-Replayed") == "true" for _, headers in responses) == 1
    mismatch, _ = t.request(EVENTS + "/aaa/erroneous", method="POST", token=token,
                            key=marker, data={"expected_classification_version": 1}, expect=422)
    assert mismatch["error"]["code"] == "IDEMPOTENCY_KEY_PAYLOAD_MISMATCH"
    t.sql(f"UPDATE idempotency_keys SET expires_at=clock_timestamp()-interval '1 second' "
          f"WHERE user_id='{user}' AND company_id='{A}' AND idempotency_key='{marker}'")
    _, headers = same_key()
    assert headers.get("X-Idempotency-Replayed") is None
    conflict_key = t.key()
    t.request(EVENTS + "/aaa-last/erroneous", method="POST", token=token, key=conflict_key,
              data={"expected_classification_version": 42}, expect=409)
    assert scalar(f"SELECT count(*) FROM idempotency_keys WHERE company_id='{A}' AND idempotency_key='{conflict_key}'") == "0"

    # Global user quota, independent company quota. Fixtures cannot be claimed by a worker.
    quota_jobs = [export_fixture(A, user, sid) for _ in range(3)]
    export_body = {"filters": {}, "format_preset": "STANDARD_COMMA_UTF8"}
    error, _ = t.request(f"/api/v1/companies/{B}/events/exports", method="POST", token=token,
                         key=t.key(), data=export_body, expect=429)
    assert error["error"]["code"] == "RATE_LIMITED"
    quota_jobs += [export_fixture(A, other, sid_other) for _ in range(7)]
    t.request(EVENTS + "/exports", method="POST", token=other_token, key=t.key(), data=export_body, expect=429)
    # Test fixture teardown; application terminal transitions are tested separately below.
    t.sql(f"DELETE FROM export_jobs WHERE company_id='{A}' AND export_id IN (" +
          ",".join(f"'{id_}'" for id_ in quota_jobs) + ")")

    # First claim starts after a 23-hour queue wait; reclaim preserves first-claim timestamps.
    queued = export_fixture(A, user, sid, state="QUEUED", attempts=0)
    reclaimed = export_fixture(A, user, sid, age="10 minutes", lease="-1 second", attempts=1)
    for id_ in [queued, reclaimed]:
        poll(f"SELECT status FROM export_jobs WHERE export_id='{id_}'", lambda s: s in ("COMPLETED", "FAILED"))
        assert scalar(f"SELECT status FROM export_jobs WHERE export_id='{id_}'") == "COMPLETED"
        assert scalar(f"SELECT (absolute_deadline_at=processing_started_at+interval '30 minutes')::int "
                      f"FROM export_jobs WHERE export_id='{id_}'") == "1"
    assert scalar(f"SELECT (processing_started_at-created_at>interval '22 hours')::int FROM export_jobs WHERE export_id='{queued}'") == "1"
    assert scalar(f"SELECT attempts FROM export_jobs WHERE export_id='{reclaimed}'") == "2"
    assert scalar(f"SELECT (processing_started_at<clock_timestamp()-interval '9 minutes')::int FROM export_jobs WHERE export_id='{reclaimed}'") == "1"

    # Export formats, exact columns, public-media selection and response scoping.
    for preset, media in [("STANDARD_COMMA_UTF8", False), ("EXCEL_SEMICOLON_BOM", True)]:
        body = {**export_body, "format_preset": preset, "include_media_links": media}
        key = t.key()
        job, _ = t.request(EVENTS + "/exports", method="POST", token=token, data=body, key=key, expect=202)
        if media:
            assert job["warnings"][0]["code"] == "PUBLIC_MEDIA_LINKS"
        replay, headers = t.request(EVENTS + "/exports", method="POST", token=token, data=body, key=key, expect=202)
        assert replay == job and headers["X-Idempotency-Replayed"] == "true"
        poll(f"SELECT status FROM export_jobs WHERE export_id='{job['export_id']}'", lambda s: s in ("COMPLETED", "FAILED"))
        status, _ = t.request(job["status_url"], token=token)
        assert status["status"] == "COMPLETED", status
        content, _ = t.request(status["download_url"], token=token)
        assert content.startswith(b"\xef\xbb\xbf") == (preset == "EXCEL_SEMICOLON_BOM")
        rows = list(csv.reader(io.StringIO(content.decode("utf-8-sig")), delimiter=";" if media else ","))
        assert all(len(row) == (16 if media else 14) for row in rows)
        assert all(row[4] == "'\t=1+1" for row in rows[1:])
        t.request(job["status_url"], token=other_token, expect=404)
        t.request(status["download_url"], token=other_token, expect=404)
        t.sql(f"UPDATE export_jobs SET expires_at=clock_timestamp() WHERE export_id='{job['export_id']}'")
        t.request(status["download_url"], token=token, expect=410)
        expired, _ = t.request(job["status_url"], token=token)
        assert expired["status"] == "EXPIRED" and expired["failure_code"] == "EXPORT_EXPIRED"
    t.request(EVENTS + "/exports", method="POST", token=token, key=t.key(),
              data={**export_body, "format_preset": "CSV_V1"}, expect=422)

    # Independent stream generations share one durable payload/hash.
    sequence = int(counter())
    with t.WebSocket(EVENTS + "/ws") as first, t.WebSocket(EVENTS + "/ws") as second:
        for ws, generation in [(first, 1), (second, 7)]:
            ws.send("AUTH", access_token=token, replay_id=generation, after_sequence=sequence)
            assert ws.expect("SUBSCRIBED")["replay_empty"]
            ws.expect("REPLAY_COMPLETED")
        t.request(EVENTS + "/aaa-newer/acknowledge", method="PUT", token=token)
        a, b = first.expect("TENANT_COMMIT"), second.expect("TENANT_COMMIT")
        assert a["replay_id"] == 1 and b["replay_id"] == 7
        assert a["tenant_sequence"] == b["tenant_sequence"] and a["payload_sha256"] == b["payload_sha256"]
        for frame in [a, b]:
            digest = frame.pop("payload_sha256")
            frame.pop("replay_id")
            assert hashlib.sha256(json.dumps(frame, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest() == digest
        assert scalar(f"SELECT count(*) FROM tenant_outbox WHERE company_id='{A}' AND (payload?'replay_id' OR payload?'payload_sha256')") == "0"
        first.send("HEARTBEAT", client_confirmed_seq=int(counter()) + 1)
        assert first.expect("CLOSE")["code"] == 1002
    with t.WebSocket(EVENTS + "/ws") as ws:
        ws.send("AUTH", access_token=token, replay_id=9007199254740992, after_sequence=0)
        assert ws.expect("RESYNC_REQUIRED")["reason"] == "INVALID_REPLAY_ID"

    # Recoverable dispatcher gap must allow rebootstrap on the same socket.
    sequence = int(counter())
    with t.WebSocket(EVENTS + "/ws") as first, t.WebSocket(EVENTS + "/ws") as second:
        for ws in [first, second]:
            ws.send("AUTH", access_token=token, replay_id=1, after_sequence=sequence)
            ws.expect("SUBSCRIBED")
            ws.expect("REPLAY_COMPLETED")
        summary, _ = t.request(EVENTS + "/summary", token=token)
        summary.pop("stream_cursor")
        payload = {"schema_version": 1, "type": "TENANT_COMMIT", "tenant_sequence": sequence + 2049,
                   "event_created": None, "event_updated": None, "filter_options_updated": None,
                   "summary_updated": summary}
        publish(A, payload)
        first.expect("RESYNC_REQUIRED")
        second.expect("RESYNC_REQUIRED")
        boot, _ = t.request(EVENTS + "/bootstrap", token=token)
        for ws in [first, second]:
            ws.send("REPLAY_REQUEST", replay_id=2, after_sequence=boot["stream_cursor"])
            assert ws.expect("SUBSCRIBED")["replay_id"] == 2
            ws.expect("REPLAY_COMPLETED")
        t.request(EVENTS + "/aaa-last/acknowledge", method="PUT", token=token)
        for ws in [first, second]:
            assert ws.expect("TENANT_COMMIT")["replay_id"] == 2

    # Empty replay at the exact JS integer ceiling must never evaluate H+1.
    t.sql(f"UPDATE company_stream_counters SET last_sequence=9007199254740991 WHERE company_id='{B}'")
    with t.WebSocket(f"/api/v1/companies/{B}/events/ws") as ws:
        ws.send("AUTH", access_token=token, replay_id=1, after_sequence=9007199254740991)
        subscribed = ws.expect("SUBSCRIBED")
        assert subscribed["replay_empty"] and subscribed["replay_from_sequence"] is None
        assert ws.expect("REPLAY_COMPLETED")["through_sequence"] == 9007199254740991
    error, _ = t.request(f"/api/v1/companies/{B}/events/ingest", method="POST", token=token, key=t.key(),
                         data={"events": [t.row("overflow")]}, expect=422)
    assert error["error"]["code"] == "SEQUENCE_OUT_OF_RANGE"
    assert scalar(f"SELECT last_sequence FROM company_stream_counters WHERE company_id='{B}'") == "9007199254740991"

    # Restricted-company matrix and no mutation/idempotency side effects on denial.
    grant = str(uuid.uuid4())
    t.sql(f"INSERT INTO support_grants(grant_id,company_id,user_id,mode,ticket_reference,granted_by_user_id,expires_at) "
          f"VALUES ('{grant}','{A}','{user}','SUPPORT','regression','{other}',clock_timestamp()+interval '15 minutes')")
    for restriction in ["MAINTENANCE", "BILLING_DEBT", "ADMIN_BLOCKED", "TERMS_VIOLATION"]:
        for override in [False, True]:
            t.sql(f"UPDATE companies SET restriction='{restriction}' WHERE id='{A}';"
                  f"UPDATE support_grants SET allow_billing_override={str(override).lower()} WHERE grant_id='{grant}'")
            access, headers = t.request(ROOT + "/access", token=token)
            t.request(ROOT + "/access", token=token, extra={"If-None-Match": headers["ETag"]})
            allowed = restriction == "MAINTENANCE" or restriction == "BILLING_DEBT" and override
            assert access["access_status"] == ("ACTIVE_SUPPORT_OVERRIDE" if allowed else "RESTRICTED")
            if not allowed:
                assert not access["tenant_permissions"] and not access["support_permissions"]
            assert "EVENTS_REVIEW" not in access["tenant_permissions"] and "REPORTS_EXPORT" not in access["tenant_permissions"]
            before = counter()
            key = t.key()
            t.request(EVENTS + "/aaa-last/erroneous", method="POST", token=token, key=key,
                      data={"expected_classification_version": 1}, expect=403)
            assert counter() == before
            assert scalar(f"SELECT count(*) FROM idempotency_keys WHERE company_id='{A}' AND idempotency_key='{key}'") == "0"
    t.sql(f"UPDATE companies SET restriction=NULL WHERE id='{A}'")
    conflicting_duplicate_latches_dispatcher(user, token)
    print("Contract regressions passed: mutation races, quota, CSV, WS recovery, integrity latch, frozen matrix")


if __name__ == "__main__":
    try:
        run()
    finally:
        companies = ",".join(f"'{id_}'" for id_ in COMPANIES)
        t.sql(f"DELETE FROM device_operations WHERE company_id IN ({companies});"
              f"DELETE FROM company_stream_counters WHERE company_id IN ({companies});"
              f"DELETE FROM companies WHERE id IN ({companies})")
        if t.ACCOUNTS:
            users = ",".join(f"'{id_}'" for id_ in t.ACCOUNTS)
            t.sql(f"DELETE FROM accounts WHERE id IN ({users})")
