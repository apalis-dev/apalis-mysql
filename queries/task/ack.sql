UPDATE
    jobs
SET
    status = ?,
    attempts = ?,
    last_result = ?,
    done_at = NOW()
WHERE
    id = ?
    AND lock_by = ?
