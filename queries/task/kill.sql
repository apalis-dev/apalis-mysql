UPDATE
    jobs
SET
    status = 'Killed',
    done_at = UNIX_TIMESTAMP(),
    last_result = ?
WHERE
    id = ?
    AND lock_by = ?
