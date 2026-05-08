
ALTER TABLE
    jobs
ADD
    COLUMN idempotency_key varchar(36);

CREATE UNIQUE INDEX idx_jobs_idempotency_key ON jobs(job_type, idempotency_key);
