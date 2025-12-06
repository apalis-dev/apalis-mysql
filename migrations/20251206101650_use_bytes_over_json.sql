-- 1. Add a temporary column to store the BLOB version of the job data
ALTER TABLE jobs
    ADD COLUMN job_blob LONGBLOB NULL;

-- 2. Copy converted JSON data into the BLOB column.
--    MySQL will convert JSON → TEXT; TEXT → BLOB is allowed.
UPDATE jobs
    SET job_blob = CAST(job AS CHAR);

-- 3. Drop the old JSON column
ALTER TABLE jobs
    DROP COLUMN job;

-- 4. Rename the new column to replace the original
ALTER TABLE jobs
    CHANGE COLUMN job_blob job LONGBLOB NOT NULL;
