UPDATE
    jobs
SET
    status = "Pending",
    done_at = NULL,
    lock_by = NULL,
    lock_at = NULL,
    attempts = attempts + 1,
    last_result = '{"Err": "Re-enqueued due to worker heartbeat timeout."}'
WHERE
    id IN (
        SELECT
            orphaned.id
        FROM (
            SELECT
                jobs.id
            FROM
                jobs
                INNER JOIN workers ON lock_by = workers.id
            WHERE
                (
                    status = "Running"
                    OR status = "Queued"
                )
                AND UNIX_TIMESTAMP() - workers.last_seen >= ?
                AND workers.worker_type = ?
        ) AS orphaned
    );
