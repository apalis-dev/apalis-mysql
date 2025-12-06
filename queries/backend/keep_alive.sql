UPDATE
    workers
SET
    last_seen = UNIX_TIMESTAMP()
WHERE
    id = ? AND worker_type = ?;
