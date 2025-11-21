SELECT
    *
FROM
    jobs
WHERE
    status = ?
ORDER BY
    done_at DESC,
    run_at DESC
LIMIT
    ? OFFSET ?
