SELECT
    *
FROM
    workers
ORDER BY
    last_seen DESC
LIMIT
    ? OFFSET ?
