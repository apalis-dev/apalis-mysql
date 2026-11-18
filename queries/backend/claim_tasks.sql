UPDATE
    jobs
SET
    status = 'Queued',
    lock_at = NOW(),
    lock_by = ?
WHERE
    id IN (
        SELECT
            jt.value
        FROM
            JSON_TABLE(
                JSON_EXTRACT(?, '$'), '$[*]'
                COLUMNS (value VARCHAR(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci PATH '$')
            ) AS jt
    );
