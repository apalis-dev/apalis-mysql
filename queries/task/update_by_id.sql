UPDATE
    jobs
SET
    status = ?,
    attempts = ?,
    done_at = ?,
    lock_by = ?,
    lock_at = ?,
    last_result = ?,
    priority = ?
WHERE
    id = ?
