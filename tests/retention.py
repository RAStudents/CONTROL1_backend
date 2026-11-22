"""Exercise the background snapshot, export and outbox retention boundaries."""
import hashlib
import json
import time
import uuid

from compose_smoke import sql

COMPANY = str(uuid.uuid4())
USER = str(uuid.uuid4())
LEASE = str(uuid.uuid4())


def payload(sequence):
    dto = {"schema_version": 1, "type": "TENANT_COMMIT", "tenant_sequence": sequence,
           "event_created": None, "event_updated": None,
           "summary_updated": {"summary_as_of_sequence": sequence}, "filter_options_updated": None}
    wire = json.dumps(dto, separators=(",", ":"), sort_keys=True).encode()
    return f"'{wire.decode()}'::jsonb,'{hashlib.sha256(wire).hexdigest()}'"


def until(statement, expected):
    for _ in range(90):
        if sql(statement) == expected:
            return
        time.sleep(0.5)
    raise AssertionError((statement, expected))


def retention():
    sql(f"""
        BEGIN;
        INSERT INTO companies(id,name) VALUES ('{COMPANY}','Retention fixture');
        UPDATE company_stream_counters SET last_sequence=4 WHERE company_id='{COMPANY}';
        INSERT INTO events(company_id,id,created_sequence,event_type_id,category_code,location_id,device_id,occurred_at,received_at,preview_url)
          VALUES ('{COMPANY}','old',1,'event','cat','loc','device',clock_timestamp(),clock_timestamp(),'');
        INSERT INTO event_versions(company_id,event_id,event_version,classification_version,acknowledgement_version,valid_from_sequence,valid_to_sequence,status,is_erroneous)
          VALUES ('{COMPANY}','old',1,1,1,1,2,'NEW',false),('{COMPANY}','old',2,1,1,2,NULL,'NEW',false);
        INSERT INTO tenant_outbox(company_id,tenant_sequence,payload,payload_sha256,created_at,broker_dispatched_at)
          VALUES ('{COMPANY}',1,{payload(1)},clock_timestamp()-interval '73 hours',clock_timestamp()-interval '73 hours'),
            ('{COMPANY}',2,{payload(2)},clock_timestamp()-interval '73 hours',NULL),
            ('{COMPANY}',3,{payload(3)},clock_timestamp(),clock_timestamp()),
            ('{COMPANY}',4,{payload(4)},clock_timestamp()-interval '73 hours',clock_timestamp()-interval '73 hours');
        INSERT INTO outbox_company_leases(company_id,lease_token,lease_until)
          VALUES ('{COMPANY}','{LEASE}',clock_timestamp()+interval '1 hour');
        INSERT INTO event_snapshots(snapshot_id,company_id,user_id,filter_hash,as_of_sequence,total_count,expires_at)
          VALUES ('snapshot-{COMPANY}','{COMPANY}','{USER}','test',1,1,clock_timestamp()+interval '30 minutes');
        INSERT INTO export_jobs(export_id,company_id,user_id,status,snapshot_sequence,filters_json,format_preset,include_media,
            created_at,queue_expires_at,processing_started_at,absolute_deadline_at,lease_token,lease_until,attempts)
        SELECT 'export-{COMPANY}','{COMPANY}','{USER}','PROCESSING',1,'{{}}','STANDARD_COMMA_UTF8',false,
            t,t+interval '24 hours',t,t+interval '30 minutes','{LEASE}',t+interval '5 minutes',1 FROM (SELECT clock_timestamp() t) now;
        COMMIT;
    """)
    sql(f"UPDATE event_versions SET valid_to_sequence=4 WHERE company_id='{COMPANY}' AND event_version=2;"
        f"INSERT INTO event_versions(company_id,event_id,event_version,classification_version,acknowledgement_version,valid_from_sequence,status,is_erroneous) "
        f"VALUES ('{COMPANY}','old',3,1,1,4,'NEW',false)")
    versions = f"SELECT count(*) FROM event_versions WHERE company_id='{COMPANY}' AND event_version=1"
    outbox = f"SELECT string_agg(tenant_sequence::text,',' ORDER BY tenant_sequence) FROM tenant_outbox WHERE company_id='{COMPANY}'"
    until(outbox, "2,3")
    assert sql(versions) == "1"

    sql(f"UPDATE event_snapshots SET expires_at=clock_timestamp() WHERE company_id='{COMPANY}'")
    until(f"SELECT count(*) FROM event_snapshots WHERE company_id='{COMPANY}'", "0")
    assert sql(versions) == "1"

    # After the processing deadline, the export releases its anchor in the same transaction as GC.
    sql(f"""WITH t AS (SELECT clock_timestamp()-interval '31 minutes' started)
        UPDATE export_jobs SET processing_started_at=t.started,absolute_deadline_at=t.started+interval '30 minutes',
            lease_until=t.started+interval '5 minutes' FROM t WHERE company_id='{COMPANY}'""")
    until(versions, "0")
    code = sql(f"SELECT failure_code FROM export_jobs WHERE company_id='{COMPANY}'")
    assert code == "EXPORT_ABANDONED_TIMEOUT", code
    assert sql(outbox) == "2,3"
    assert sql(f"SELECT count(*) FROM event_versions WHERE company_id='{COMPANY}' AND event_version=2") == "1", "out-of-order timestamp allowed premature GC"
    print("Retention passed: snapshot and processing anchors, expired job reaper, separate outbox cleanup")


if __name__ == "__main__":
    try:
        retention()
    finally:
        sql(f"DELETE FROM company_stream_counters WHERE company_id='{COMPANY}';"
            f"DELETE FROM companies WHERE id='{COMPANY}';")
