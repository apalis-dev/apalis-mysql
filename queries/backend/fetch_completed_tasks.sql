SELECT
    id,
    status,
    last_result as result
FROM
    jobs
WHERE
    id IN (
        SELECT
            jt.value
        FROM
            JSON_TABLE(
                ?, '$[*]'
                COLUMNS (value INT PATH '$')
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
