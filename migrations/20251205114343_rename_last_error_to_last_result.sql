-- Add migration script here
ALTER TABLE jobs
RENAME COLUMN last_error TO last_result;
