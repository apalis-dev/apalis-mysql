UPDATE
    jobs
SET
    status = 'Failed',
    attempts = attempts + 1,
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    run_at = ?
WHERE
    id = ?
