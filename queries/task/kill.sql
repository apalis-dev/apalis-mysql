UPDATE
    jobs
SET
    status = 'Killed',
    done_at = strftime('%s', 'now'),
    last_result = ?
WHERE
    id = ?
    AND lock_by = ?
