SELECT
    *
FROM
    jobs
WHERE
    status = ?
    AND job_type = ?
ORDER BY
    done_at DESC,
    run_at DESC
LIMIT
    ? OFFSET ?
