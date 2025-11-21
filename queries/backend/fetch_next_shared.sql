UPDATE
    jobs
SET
    status = 'Queued',
    lock_at = strftime('%s', 'now')
WHERE
    ROWID IN (
        SELECT
            ROWID
        FROM
            jobs
        WHERE
            job_type IN (
                SELECT
                    value
                FROM
                    json_each(?)
            )
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
                OR run_at <= strftime('%s', 'now')
            )
            AND ROWID IN (
                SELECT
                    value
                FROM
                    json_each(?)
            )
        ORDER BY
            priority DESC,
            run_at ASC,
            id ASC
        LIMIT
            ?
    ) RETURNING *;
