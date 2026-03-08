-- Replay jobs table for persistent job storage
CREATE TABLE IF NOT EXISTS replay_jobs (
    job_id       VARCHAR(255) PRIMARY KEY,
    status       VARCHAR(32)  NOT NULL,
    parameters   JSONB        NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ   NOT NULL,
    updated_at   TIMESTAMPTZ,
    message      TEXT
);

CREATE INDEX IF NOT EXISTS idx_replay_jobs_status ON replay_jobs (status);
CREATE INDEX IF NOT EXISTS idx_replay_jobs_created_at ON replay_jobs (created_at);
