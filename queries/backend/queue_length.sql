Select
    COUNT(*) AS count
FROM
    jobs
WHERE
    (
        status = 'Pending'
        OR (
            status = 'Failed'
            AND attempts < max_attempts
        )
    )
