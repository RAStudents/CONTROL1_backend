-- RR writers serialize both the global user quota and the tenant quota.
CREATE TABLE export_user_quota (
 user_id uuid PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
 revision bigint NOT NULL DEFAULT 0
);
CREATE INDEX idx_export_jobs_active_user ON export_jobs(user_id)
 WHERE status IN ('QUEUED','PROCESSING');
