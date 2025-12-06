INSERT INTO workers (id, worker_type, storage_name, layers, last_seen, started_at)
SELECT ?, ?, ?, ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP()
FROM DUAL
WHERE NOT EXISTS (
    SELECT 1 FROM workers
    WHERE id = ?
      AND UNIX_TIMESTAMP() - last_seen < ?
)
ON DUPLICATE KEY UPDATE
    worker_type = VALUES(worker_type),
    storage_name = VALUES(storage_name),
    layers = VALUES(layers),
    last_seen = VALUES(last_seen),
    started_at = VALUES(started_at);
