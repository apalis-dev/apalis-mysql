SELECT
    *
FROM
    Workers
ORDER BY
    last_seen DESC
LIMIT
    ? OFFSET ?
