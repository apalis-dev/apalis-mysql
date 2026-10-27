UPDATE
    jobs
SET
    status = 'Queued',
    lock_at = NOW(),
    lock_by = ?
WHERE
    id IN (
        SELECT
            jt.value
        FROM
            JSON_TABLE(
                JSON_EXTRACT(?, '$'), '$[*]'
                COLUMNS (value VARCHAR(36) PATH '$')
            ) AS jt
    );
