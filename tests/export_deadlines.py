"""Exercise production reaper, live max-attempt lease, and completion deadline CAS."""
import time
import uuid

import compose_smoke as t
from anchors import Barrier
from contract_regressions import claims, export_fixture, poll

COMPANY = str(uuid.uuid4())


def status(id_):
    return f"SELECT status FROM export_jobs WHERE export_id='{id_}'"


def run():
    user, pair = t.create_account()
    session = claims(pair["access_token"])["sid"]
    t.sql(f"INSERT INTO companies(id,name) VALUES ('{COMPANY}','Export deadlines');"
          f"INSERT INTO memberships(user_id,company_id,role) VALUES ('{user}','{COMPANY}','ADMIN')")
    queued = export_fixture(COMPANY, user, session, state="QUEUED", attempts=0, queue_age="25 hours")
    expired = export_fixture(COMPANY, user, session, age="31 minutes")
    maximum = export_fixture(COMPANY, user, session, attempts=3)
    for id_, reason in [(queued, "EXPORT_QUEUE_TTL_EXCEEDED"), (expired, "EXPORT_ABANDONED_TIMEOUT")]:
        poll(status(id_), lambda s: s == "FAILED")
        assert t.sql(f"SELECT failure_code FROM export_jobs WHERE export_id='{id_}'") == reason
    assert t.sql(f"SELECT (processing_started_at IS NULL AND absolute_deadline_at IS NULL)::int FROM export_jobs WHERE export_id='{queued}'") == "1"
    # The same production sweep saw this job at max attempts and did not steal a live lease.
    assert t.sql(status(maximum)) == "PROCESSING"
    t.sql(f"UPDATE export_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE export_id='{maximum}'")
    poll(status(maximum), lambda s: s == "FAILED")
    assert t.sql(f"SELECT failure_code FROM export_jobs WHERE export_id='{maximum}'") == "EXPORT_MAX_ATTEMPTS_EXCEEDED"

    # Reclaim near the deadline; renewal must cap the lease. Hold the actual completion
    # barrier across expiry to make the production completion CAS lose deterministically.
    with Barrier(True, company=COMPANY):
        late = export_fixture(COMPANY, user, session, age="29 minutes 55 seconds", lease="-1 second")
        poll(f"SELECT attempts FROM export_jobs WHERE export_id='{late}'", lambda n: n == "2", seconds=3)
        assert t.sql(f"SELECT (lease_until=absolute_deadline_at)::int FROM export_jobs WHERE export_id='{late}'") == "1"
        time.sleep(6)
        assert t.sql(status(late)) == "PROCESSING"
    poll(status(late), lambda s: s == "FAILED")
    assert t.sql(f"SELECT failure_code FROM export_jobs WHERE export_id='{late}'") == "EXPORT_ABANDONED_TIMEOUT"
    assert t.sql(f"SELECT (storage_path IS NULL AND lease_token IS NULL AND lease_until IS NULL AND finished_at IS NOT NULL)::int FROM export_jobs WHERE export_id='{late}'") == "1"
    print("Export deadlines passed: queue TTL, deadline regardless of live lease, max attempts, capped renewal and late CAS")


if __name__ == "__main__":
    try:
        run()
    finally:
        t.sql(f"DELETE FROM company_stream_counters WHERE company_id='{COMPANY}';DELETE FROM companies WHERE id='{COMPANY}'")
        for user in t.ACCOUNTS:
            t.sql(f"DELETE FROM accounts WHERE id='{user}'")
