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
            jobs.id
        FROM
            jobs
            INNER JOIN Workers ON lock_by = Workers.id
        WHERE
            (
                status = "Running"
                OR status = "Queued"
            )
            AND strftime('%s', 'now') - Workers.last_seen >= ?
            AND Workers.worker_type = ?
    );
