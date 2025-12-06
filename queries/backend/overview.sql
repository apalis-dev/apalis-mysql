SELECT
    1 AS priority,
    'Number' AS type,
    'RUNNING_JOBS' AS statistic,
    CAST(SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) AS REAL) AS value
FROM jobs
UNION ALL
SELECT
    1,
    'Number',
    'PENDING_JOBS',
    CAST(SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS REAL)
FROM jobs
UNION ALL
SELECT
    2,
    'Number',
    'FAILED_JOBS',
    CAST(SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS REAL)
FROM jobs
UNION ALL
SELECT
    2,
    'Number',
    'ACTIVE_JOBS',
    CAST(SUM(CASE WHEN status IN ('Pending', 'Running', 'Queued') THEN 1 ELSE 0 END) AS REAL)
FROM jobs
UNION ALL
SELECT
    2,
    'Number',
    'STALE_RUNNING_JOBS',
    CAST(COUNT(*) AS REAL)
FROM jobs
WHERE status = 'Running'
    AND run_at < UNIX_TIMESTAMP() - 3600
UNION ALL
SELECT
    2,
    'Percentage',
    'KILL_RATE',
    CAST(ROUND(100.0 * SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) / IFNULL(COUNT(*), 1), 2) AS REAL)
FROM jobs
UNION ALL
SELECT
    3,
    'Number',
    'JOBS_PAST_HOUR',
    CAST(COUNT(*) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 3600
UNION ALL
SELECT
    3,
    'Number',
    'JOBS_TODAY',
    CAST(COUNT(*) AS REAL)
FROM jobs
WHERE DATE(FROM_UNIXTIME(run_at)) = CURDATE()
UNION ALL
SELECT
    3,
    'Number',
    'KILLED_JOBS_TODAY',
    CAST(SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS REAL)
FROM jobs
WHERE DATE(FROM_UNIXTIME(run_at)) = CURDATE()
UNION ALL
SELECT
    3,
    'Decimal',
    'AVG_JOBS_PER_MINUTE_PAST_HOUR',
    CAST(ROUND(COUNT(*) / 60.0, 2) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 3600
UNION ALL
SELECT
    4,
    'Number',
    'TOTAL_JOBS',
    CAST(COUNT(*) AS REAL)
FROM jobs
UNION ALL
SELECT
    4,
    'Number',
    'DONE_JOBS',
    CAST(SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) AS REAL)
FROM jobs
UNION ALL
SELECT
    4,
    'Number',
    'COMPLETED_JOBS',
    CAST(SUM(CASE WHEN status IN ('Done', 'Failed', 'Killed') THEN 1 ELSE 0 END) AS REAL)
FROM jobs
UNION ALL
SELECT
    4,
    'Number',
    'KILLED_JOBS',
    CAST(SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS REAL)
FROM jobs
UNION ALL
SELECT
    4,
    'Percentage',
    'SUCCESS_RATE',
    CAST(ROUND(100.0 * SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) / IFNULL(COUNT(*), 1), 2) AS REAL)
FROM jobs
UNION ALL
SELECT
    5,
    'Decimal',
    'AVG_JOB_DURATION_MINS',
    CAST(ROUND(AVG((done_at - run_at) / 60.0), 2) AS REAL)
FROM jobs
WHERE status IN ('Done', 'Failed', 'Killed')
    AND done_at IS NOT NULL
UNION ALL
SELECT
    5,
    'Decimal',
    'LONGEST_RUNNING_JOB_MINS',
    CAST(ROUND(MAX(CASE WHEN status = 'Running' THEN (UNIX_TIMESTAMP() - run_at) / 60.0 ELSE 0 END), 2) AS REAL)
FROM jobs
UNION ALL
SELECT
    5,
    'Number',
    'QUEUE_BACKLOG',
    CAST(SUM(CASE WHEN status = 'Pending' AND run_at <= UNIX_TIMESTAMP() THEN 1 ELSE 0 END) AS REAL)
FROM jobs
UNION ALL
SELECT
    6,
    'Number',
    'JOBS_PAST_24_HOURS',
    CAST(COUNT(*) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 86400
UNION ALL
SELECT
    6,
    'Number',
    'JOBS_PAST_7_DAYS',
    CAST(COUNT(*) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 604800
UNION ALL
SELECT
    6,
    'Number',
    'KILLED_JOBS_PAST_7_DAYS',
    CAST(SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 604800
UNION ALL
SELECT
    6,
    'Percentage',
    'SUCCESS_RATE_PAST_24H',
    CAST(ROUND(100.0 * SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) / IFNULL(COUNT(*), 1), 2) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 86400
UNION ALL
SELECT
    7,
    'Decimal',
    'AVG_JOBS_PER_HOUR_PAST_24H',
    CAST(ROUND(COUNT(*) / 24.0, 2) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 86400
UNION ALL
SELECT
    7,
    'Decimal',
    'AVG_JOBS_PER_DAY_PAST_7D',
    CAST(ROUND(COUNT(*) / 7.0, 2) AS REAL)
FROM jobs
WHERE run_at >= UNIX_TIMESTAMP() - 604800
UNION ALL
SELECT
    8,
    'Timestamp',
    'MOST_RECENT_JOB',
    CAST(MAX(run_at) AS REAL)
FROM jobs
UNION ALL
SELECT
    8,
    'Timestamp',
    'OLDEST_PENDING_JOB',
    CAST(MIN(run_at) AS REAL)
FROM jobs
WHERE status = 'Pending'
    AND run_at <= UNIX_TIMESTAMP()
UNION ALL
SELECT
    8,
    'Number',
    'PEAK_HOUR_JOBS',
    CAST(MAX(hourly_count) AS REAL)
FROM (
    SELECT
        COUNT(*) as hourly_count
    FROM jobs
    WHERE run_at >= UNIX_TIMESTAMP() - 86400
    GROUP BY HOUR(FROM_UNIXTIME(run_at))
) AS hourly
UNION ALL
SELECT
    9,
    'Number',
    'DB_PAGE_SIZE',
    CAST(@@innodb_page_size AS REAL)
UNION ALL
SELECT
    9,
    'Number',
    'DB_PAGE_COUNT',
    CAST(VARIABLE_VALUE AS REAL)
FROM performance_schema.global_status
WHERE VARIABLE_NAME = 'Innodb_pages_written'
UNION ALL
SELECT
    9,
    'Number',
    'DB_SIZE',
    CAST(SUM(data_length + index_length) AS REAL)
FROM information_schema.tables
WHERE table_schema = DATABASE()
ORDER BY priority, statistic;
