-- PostgreSQL does not automatically index the referencing side of a foreign key.
-- Event removal must find snapshot references without scanning every snapshot item.
CREATE INDEX IF NOT EXISTS idx_event_snapshot_items_event
    ON event_snapshot_items (company_id, event_id);
