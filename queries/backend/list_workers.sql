SELECT
    *
FROM
    workers
WHERE
    worker_type = ?
ORDER BY
    last_seen DESC
LIMIT
    ? OFFSET ?
