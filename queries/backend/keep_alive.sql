UPDATE
    workers
SET
    last_seen = NOW()
WHERE
    id = ? AND worker_type = ?;
