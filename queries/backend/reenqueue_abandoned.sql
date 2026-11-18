UPDATE
    jobs
INNER JOIN workers ON jobs.lock_by = workers.id
SET
    jobs.status = 'Pending',
    jobs.done_at = NULL,
    jobs.lock_by = NULL,
    jobs.lock_at = NULL,
    jobs.attempts = jobs.attempts + 1,
    jobs.last_result = '{"Err": "Re-enqueued due to worker shutdown"}'
WHERE
    jobs.status = 'Queued'
    AND workers.worker_type = ?
    AND workers.id = ?
    AND jobs.id IN (
        SELECT
            jt.value
        FROM
            JSON_TABLE(
                JSON_EXTRACT(?, '$'), '$[*]'
                COLUMNS (value VARCHAR(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci PATH '$')
            ) AS jt
    );
