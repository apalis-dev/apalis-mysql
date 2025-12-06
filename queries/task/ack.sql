UPDATE
    jobs
SET
    status = ?,
    attempts = ?,
    last_result = ?,
    done_at = UNIX_TIMESTAMP()
WHERE
    id = ?
    AND lock_by = ?
