UPDATE
    jobs
SET
    status = 'Killed',
    done_at = NOW(),
    last_result = ?
WHERE
    id = ?
    AND lock_by = ?
