UPDATE
    jobs
    INNER JOIN (
        SELECT
            id
        FROM
            jobs
        WHERE
            job_type = ?
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
            ?
    ) AS selected_jobs ON jobs.id = selected_jobs.id
SET
    jobs.status = 'Queued',
    jobs.lock_by = ?,
    jobs.lock_at = ?
