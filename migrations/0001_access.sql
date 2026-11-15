CREATE TABLE accounts (
    id uuid PRIMARY KEY,
    email text UNIQUE NOT NULL CHECK (email = lower(email)),
    password_hash text NOT NULL,
    frozen boolean NOT NULL DEFAULT false,
    global_permissions text[] NOT NULL DEFAULT '{}',
    default_company_id uuid
);
CREATE TABLE companies (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    business_timezone text NOT NULL DEFAULT 'UTC',
    restriction text CHECK (restriction IN ('MAINTENANCE','BILLING_DEBT','ADMIN_BLOCKED','TERMS_VIOLATION')),
    restriction_details jsonb,
    monitoring_config_status text NOT NULL DEFAULT 'MISSING' CHECK (monitoring_config_status IN ('MISSING','STALE','INVALID','VALID')),
    vendor_permissions text[] NOT NULL DEFAULT '{}'
);
CREATE TABLE memberships (
    user_id uuid REFERENCES accounts ON DELETE CASCADE,
    company_id uuid REFERENCES companies ON DELETE CASCADE,
    permissions text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (user_id, company_id)
);
CREATE TABLE sessions (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES accounts ON DELETE CASCADE,
    refresh_hash text UNIQUE NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz
);
CREATE TABLE support_grants (
    grant_id uuid PRIMARY KEY,
    company_id uuid NOT NULL REFERENCES companies ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES accounts ON DELETE CASCADE,
    mode text NOT NULL,
    ticket_reference text NOT NULL,
    allow_billing_override boolean NOT NULL DEFAULT false,
    granted_by_user_id uuid NOT NULL REFERENCES accounts,
    granted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    UNIQUE (company_id, user_id),
    CHECK (expires_at > granted_at)
);
CREATE TABLE audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id uuid NOT NULL REFERENCES companies ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES accounts,
    action text NOT NULL,
    details jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE login_limits (
    bucket text PRIMARY KEY,
    until_at timestamptz NOT NULL,
    attempts integer NOT NULL
);
CREATE TABLE event_types (
    company_id uuid REFERENCES companies ON DELETE CASCADE,
    id text NOT NULL, name text NOT NULL, icon_key text NOT NULL,
    PRIMARY KEY (company_id, id)
);
CREATE TABLE locations (
    company_id uuid REFERENCES companies ON DELETE CASCADE,
    id text NOT NULL, name text NOT NULL,
    PRIMARY KEY (company_id, id)
);
CREATE TABLE devices (
    company_id uuid REFERENCES companies ON DELETE CASCADE,
    id text NOT NULL, name text NOT NULL, location_id text NOT NULL,
    health text NOT NULL DEFAULT 'UNKNOWN' CHECK (health IN ('OK','UNKNOWN','WARNING','CRITICAL')),
    PRIMARY KEY (company_id, id),
    FOREIGN KEY (company_id, location_id) REFERENCES locations
);
CREATE TABLE event_categories (
    company_id uuid REFERENCES companies ON DELETE CASCADE,
    id text NOT NULL, name text NOT NULL, score integer NOT NULL, trend text NOT NULL,
    PRIMARY KEY (company_id, id)
);
CREATE TABLE device_operations (
    id uuid PRIMARY KEY, company_id uuid NOT NULL, device_id text NOT NULL,
    user_id uuid NOT NULL REFERENCES accounts,
    command text NOT NULL, status text NOT NULL DEFAULT 'PENDING_DELIVERY'
        CHECK (status IN ('PENDING_DELIVERY','EXECUTED','FAILED_ON_DEVICE','TIMED_OUT')),
    delivery_state text NOT NULL DEFAULT 'READY'
        CHECK (delivery_state IN ('READY','IN_PROGRESS','DELIVERED','UNKNOWN')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (company_id, device_id) REFERENCES devices
);
ALTER TABLE device_operations ADD COLUMN delivery_started_at timestamptz;
