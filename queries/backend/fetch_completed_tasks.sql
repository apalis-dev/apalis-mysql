SELECT
    id,
    status,
    last_result as result
FROM
    jobs
WHERE
    id COLLATE utf8mb4_unicode_ci IN (
        SELECT
            jt.value COLLATE utf8mb4_unicode_ci
        FROM
            JSON_TABLE(
                CAST(? AS JSON), '$[*]'
                COLUMNS (value VARCHAR(255) PATH '$')
            ) AS jt
    )
    AND (
        status = 'Done'
        OR (
            status = 'Failed'
            AND attempts >= max_attempts
        )
        OR status = 'Killed'
    )
