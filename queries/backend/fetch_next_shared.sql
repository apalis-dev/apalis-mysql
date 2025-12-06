SELECT
    *
FROM
    jobs
WHERE
    JSON_CONTAINS(?, JSON_QUOTE(job_type))
    AND (
        (
            status = 'Pending'
            AND lock_by IS NULL
        )
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
    AND (
        run_at IS NULL
        OR run_at <= ?
    )
ORDER BY
    priority DESC,
    run_at ASC,
    id ASC
LIMIT
    ? FOR
UPDATE
    SKIP LOCKED;
