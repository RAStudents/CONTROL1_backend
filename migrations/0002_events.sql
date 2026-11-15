
-- 1. Счётчик последовательности компании (инициализируется нулём при создании компании)
CREATE TABLE IF NOT EXISTS company_stream_counters (
    company_id       UUID        PRIMARY KEY,
    last_sequence    BIGINT      NOT NULL DEFAULT 0
        CONSTRAINT chk_last_sequence_bounds CHECK (last_sequence >= 0 AND last_sequence <= 9007199254740991)
);

-- 2. Неизменяемое ядро события (created_sequence == tenant_sequence создания)
CREATE TABLE IF NOT EXISTS events (
    company_id        UUID        NOT NULL REFERENCES company_stream_counters(company_id) ON DELETE CASCADE,
    id                TEXT        NOT NULL CHECK (char_length(id) BETWEEN 1 AND 64),
    created_sequence  BIGINT      NOT NULL CHECK (created_sequence >= 1),
    event_type_id     TEXT        NOT NULL,
    category_code     TEXT        NOT NULL,
    location_id       TEXT        NOT NULL,
    device_id         TEXT        NOT NULL,
    occurred_at       TIMESTAMPTZ NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL,
    preview_url       TEXT        NOT NULL CHECK (char_length(preview_url) <= 2048),
    video_url         TEXT        NULL CHECK (video_url IS NULL OR char_length(video_url) <= 2048),
    PRIMARY KEY (company_id, id),
    UNIQUE (company_id, created_sequence)
);

CREATE INDEX IF NOT EXISTS idx_events_company_occurred_id
    ON events (company_id, occurred_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_events_company_created_seq
    ON events (company_id, created_sequence);

CREATE INDEX IF NOT EXISTS idx_events_company_lower_id_prefix
    ON events (company_id, lower(id) text_pattern_ops);

-- 3. Версионируемое бизнес-состояние события
CREATE TABLE IF NOT EXISTS event_versions (
    company_id               UUID        NOT NULL,
    event_id                 TEXT        NOT NULL,
    event_version            BIGINT      NOT NULL CHECK (event_version >= 1),
    classification_version   BIGINT      NOT NULL CHECK (classification_version >= 1),
    acknowledgement_version  BIGINT      NOT NULL CHECK (acknowledgement_version >= 1),
    valid_from_sequence      BIGINT      NOT NULL CHECK (valid_from_sequence >= 1),
    valid_to_sequence        BIGINT      NULL,
    status                   TEXT        NOT NULL CHECK (status IN ('NEW', 'ACKNOWLEDGED', 'LEGACY_UNKNOWN')),
    acknowledged_at          TIMESTAMPTZ NULL,
    acknowledged_by          UUID        NULL,
    is_erroneous             BOOLEAN     NOT NULL,
    erroneous_marked_at      TIMESTAMPTZ NULL,
    erroneous_marked_by      UUID        NULL,
    PRIMARY KEY (company_id, event_id, event_version),
    FOREIGN KEY (company_id, event_id) REFERENCES events(company_id, id) ON DELETE CASCADE,
    CONSTRAINT chk_valid_sequence_range
        CHECK (valid_to_sequence IS NULL OR valid_from_sequence < valid_to_sequence)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_versions_single_active
    ON event_versions (company_id, event_id)
    WHERE valid_to_sequence IS NULL;

CREATE INDEX IF NOT EXISTS idx_event_versions_as_of_lookup
    ON event_versions (company_id, event_id, valid_from_sequence, valid_to_sequence);

-- 4. Таблица ключей идемпотентности
CREATE TABLE IF NOT EXISTS idempotency_keys (
    user_id          UUID        NOT NULL,
    company_id       UUID        NOT NULL REFERENCES company_stream_counters(company_id) ON DELETE CASCADE,
    endpoint         TEXT        NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    payload_hash     TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'COMPLETED' CHECK (status = 'COMPLETED'),
    response_code    INTEGER     NOT NULL,
    response_body    JSONB       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, company_id, endpoint, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at
    ON idempotency_keys (expires_at);

-- 5. Экранные снимки пагинации (TTL 30 минут, только IMMUTABLE индексы)
CREATE TABLE IF NOT EXISTS event_snapshots (
    snapshot_id      TEXT        PRIMARY KEY,
    company_id       UUID        NOT NULL REFERENCES company_stream_counters(company_id) ON DELETE CASCADE,
    user_id          UUID        NOT NULL,
    filter_hash      TEXT        NOT NULL,
    as_of_sequence   BIGINT      NOT NULL CHECK (as_of_sequence >= 0),
    total_count      INTEGER     NOT NULL CHECK (total_count >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at       TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_snapshots_watermark
    ON event_snapshots (company_id, expires_at, as_of_sequence);

CREATE TABLE IF NOT EXISTS event_snapshot_items (
    snapshot_id      TEXT        NOT NULL REFERENCES event_snapshots(snapshot_id) ON DELETE CASCADE,
    company_id       UUID        NOT NULL,
    position         INTEGER     NOT NULL CHECK (position >= 0),
    event_id         TEXT        NOT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snapshot_id, position),
    UNIQUE (snapshot_id, event_id),
    FOREIGN KEY (company_id, event_id) REFERENCES events(company_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_event_snapshot_items_cursor
    ON event_snapshot_items (snapshot_id, occurred_at DESC, event_id DESC);

-- 6. Задания асинхронного экспорта CSV
CREATE TABLE IF NOT EXISTS export_jobs (
    export_id          TEXT        PRIMARY KEY,
    company_id         UUID        NOT NULL REFERENCES company_stream_counters(company_id) ON DELETE CASCADE,
    user_id            UUID        NOT NULL,
    status             TEXT        NOT NULL CHECK (status IN ('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'EXPIRED')),
    snapshot_sequence  BIGINT      NOT NULL CHECK (snapshot_sequence >= 0),
    filters_json       JSONB       NOT NULL,
    format_preset      TEXT        NOT NULL,
    include_media      BOOLEAN     NOT NULL,
    processed_records  BIGINT      NULL,
    total_records      BIGINT      NULL,
    storage_path       TEXT        NULL,
    error_code         TEXT        NULL,
    attempts           INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token        UUID        NULL,
    lease_until        TIMESTAMPTZ NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at         TIMESTAMPTZ NULL,
    CONSTRAINT chk_export_processing_lease
        CHECK (status <> 'PROCESSING' OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)),
    CONSTRAINT chk_export_completed_storage
        CHECK (status <> 'COMPLETED' OR (storage_path IS NOT NULL AND expires_at IS NOT NULL AND lease_until IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_export_jobs_active_watermark
    ON export_jobs (company_id, snapshot_sequence)
    WHERE status IN ('QUEUED', 'PROCESSING');

-- 7. Transactional Outbox (1 строка = 1 TENANT_COMMIT envelope) и Lease
CREATE TABLE IF NOT EXISTS tenant_outbox (
    company_id            UUID        NOT NULL REFERENCES company_stream_counters(company_id) ON DELETE CASCADE,
    tenant_sequence       BIGINT      NOT NULL CHECK (tenant_sequence >= 1 AND tenant_sequence <= 9007199254740991),
    payload               JSONB       NOT NULL,
    payload_sha256        TEXT        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    broker_dispatched_at  TIMESTAMPTZ NULL,
    PRIMARY KEY (company_id, tenant_sequence)
);

CREATE INDEX IF NOT EXISTS idx_tenant_outbox_undispatched
    ON tenant_outbox (company_id, tenant_sequence ASC)
    WHERE broker_dispatched_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_tenant_outbox_created_at
    ON tenant_outbox (company_id, created_at, tenant_sequence);

CREATE TABLE IF NOT EXISTS outbox_company_leases (
    company_id       UUID        PRIMARY KEY REFERENCES company_stream_counters(company_id) ON DELETE CASCADE,
    lease_token      UUID        NOT NULL,
    lease_epoch      BIGINT      NOT NULL DEFAULT 1 CHECK (lease_epoch >= 1),
    lease_until      TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION initialize_company_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO company_stream_counters (company_id) VALUES (NEW.id);
    RETURN NEW;
END $$;
CREATE TRIGGER initialize_company_stream AFTER INSERT ON companies
    FOR EACH ROW EXECUTE FUNCTION initialize_company_stream();
INSERT INTO company_stream_counters(company_id) SELECT id FROM companies ON CONFLICT DO NOTHING;

CREATE FUNCTION notify_access_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('access_change', TG_TABLE_NAME);
    RETURN NULL;
END $$;
CREATE TRIGGER access_account AFTER INSERT OR UPDATE OR DELETE ON accounts
    FOR EACH STATEMENT EXECUTE FUNCTION notify_access_change();
CREATE TRIGGER access_company AFTER INSERT OR UPDATE OR DELETE ON companies
    FOR EACH STATEMENT EXECUTE FUNCTION notify_access_change();
CREATE TRIGGER access_membership AFTER INSERT OR UPDATE OR DELETE ON memberships
    FOR EACH STATEMENT EXECUTE FUNCTION notify_access_change();
CREATE TRIGGER access_grant AFTER INSERT OR UPDATE OR DELETE ON support_grants
    FOR EACH STATEMENT EXECUTE FUNCTION notify_access_change();
CREATE TRIGGER access_session AFTER UPDATE OF revoked_at OR DELETE ON sessions
    FOR EACH STATEMENT EXECUTE FUNCTION notify_access_change();
