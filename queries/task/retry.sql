UPDATE
    jobs
SET
    status = 'Pending',
    attempt = attempt + 1,
    run_at = ?,
    done_at = NULL,
    lock_by = NULL
WHERE
    id = ?
    AND lock_by = ?
