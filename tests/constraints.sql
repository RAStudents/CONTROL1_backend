-- Executed only in tests/migrations.sh's disposable PostgreSQL database.
INSERT INTO companies (id, name) VALUES ('00000000-0000-0000-0000-000000000003', 'Other tenant');
INSERT INTO events (
    company_id,
    id,
    created_sequence,
    event_type_id,
    category_code,
    location_id,
    device_id,
    occurred_at,
    received_at,
    preview_url
)
SELECT
    id,
    'constraint-event',
    1,
    'event',
    'category',
    'location',
    'device',
    clock_timestamp(),
    clock_timestamp(),
    ''
FROM companies
WHERE id IN ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000003');
INSERT INTO event_versions (
    company_id,
    event_id,
    event_version,
    classification_version,
    acknowledgement_version,
    valid_from_sequence,
    valid_to_sequence,
    status,
    is_erroneous
)
VALUES ('00000000-0000-0000-0000-000000000001', 'constraint-event', 1, 1, 1, 1, 4, 'NEW', false);
INSERT INTO event_snapshots (
    snapshot_id, company_id, user_id, filter_hash, as_of_sequence, total_count, expires_at
)
VALUES (
    'tenant-snapshot',
    '00000000-0000-0000-0000-000000000001',
    '00000000-0000-0000-0000-000000000002',
    'hash',
    0,
    1,
    clock_timestamp() + interval '30 minutes'
);
DO $$
BEGIN
  BEGIN
    INSERT INTO event_snapshot_items(snapshot_id,company_id,position,event_id,occurred_at)
    VALUES ('tenant-snapshot','00000000-0000-0000-0000-000000000003',0,'constraint-event',clock_timestamp());
    RAISE EXCEPTION 'cross-tenant snapshot item accepted';
  EXCEPTION WHEN foreign_key_violation THEN NULL; END;
  BEGIN
    INSERT INTO event_versions(company_id,event_id,event_version,classification_version,acknowledgement_version,valid_from_sequence,valid_to_sequence,status,is_erroneous)
    VALUES ('00000000-0000-0000-0000-000000000001','constraint-event',2,1,1,2,3,'NEW',false);
    RAISE EXCEPTION 'overlapping closed version intervals accepted';
  EXCEPTION WHEN exclusion_violation THEN NULL; END;
  BEGIN
    INSERT INTO event_versions(company_id,event_id,event_version,classification_version,acknowledgement_version,valid_from_sequence,status,is_erroneous)
    VALUES ('00000000-0000-0000-0000-000000000001','constraint-event',2,1,1,4,'ACKNOWLEDGED',false);
    RAISE EXCEPTION 'acknowledgement without operator/time accepted';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    INSERT INTO event_versions(company_id,event_id,event_version,classification_version,acknowledgement_version,valid_from_sequence,status,is_erroneous)
    VALUES ('00000000-0000-0000-0000-000000000001','constraint-event',2,1,1,4,'NEW',true);
    RAISE EXCEPTION 'erroneous without operator/time accepted';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    UPDATE event_snapshots SET as_of_sequence=9007199254740992 WHERE snapshot_id='tenant-snapshot';
    RAISE EXCEPTION 'unsafe JS snapshot integer accepted';
  EXCEPTION WHEN check_violation THEN NULL; END;
  BEGIN
    UPDATE export_jobs SET status='COMPLETED',finished_at=clock_timestamp(),storage_path='not-published.csv',
      expires_at=clock_timestamp()+interval '24 hours',processing_started_at=created_at,
      absolute_deadline_at=created_at+interval '30 minutes' WHERE export_id='queued';
    RAISE EXCEPTION 'COMPLETED without digest and size accepted';
  EXCEPTION WHEN check_violation THEN NULL; END;
  IF EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid IN ('event_versions'::regclass,'event_snapshot_items'::regclass,'export_jobs'::regclass)
    AND NOT convalidated) THEN RAISE EXCEPTION 'unvalidated contract constraint'; END IF;
END $$;
