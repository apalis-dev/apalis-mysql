UPDATE
    jobs
SET
    status = 'Running',
    lock_at = NOW(),
    lock_by = ?
WHERE
    (
        status = 'Pending'
        OR status = 'Queued'
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
    AND run_at <= NOW()
    AND id IN (
        SELECT
            jt.value
        FROM
            JSON_TABLE(
                JSON_EXTRACT(?, '$'), '$[*]'
                COLUMNS (value VARCHAR(36) PATH '$')
            ) AS jt
    );
