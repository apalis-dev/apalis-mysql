UPDATE
    jobs
INNER JOIN (
    SELECT
        JSON_UNQUOTE(JSON_EXTRACT(value, '$.task_id')) AS task_id,
        CAST(JSON_UNQUOTE(JSON_EXTRACT(value, '$.attempt')) AS SIGNED) AS attempt,
        JSON_EXTRACT(value, '$.result') AS result,
        JSON_UNQUOTE(JSON_EXTRACT(value, '$.status')) AS status
    FROM
        JSON_TABLE(
            JSON_EXTRACT(?, '$'), '$[*]'
            COLUMNS (value JSON PATH '$')
        ) AS jt
) AS j ON j.task_id = jobs.id
SET
    jobs.status = j.status,
    jobs.attempts = j.attempt,
    jobs.last_result = j.result,
    jobs.done_at = NOW()
WHERE
    jobs.lock_by = ?;
