UPDATE
    jobs
SET
    status = ?,
    attempts = ?,
    last_result = ?,
    done_at = strftime('%s', 'now')
WHERE
    id = ?
    AND lock_by = ?
