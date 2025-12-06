UPDATE
    jobs
SET
    status = 'Running',
    lock_at = NOW(),
    lock_by = ?
WHERE
    id = ?
    AND (
        status = 'Queued'
        OR status = 'Pending'
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
