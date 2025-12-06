SELECT priority, type, statistic, value
FROM (
    -- Basic counts
    SELECT 1 AS priority, 'Number' AS type, 'RUNNING_JOBS' AS statistic,
        CAST(SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) AS REAL) AS value
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 1, 'Number', 'PENDING_JOBS',
        CAST(SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 2, 'Number', 'FAILED_JOBS',
        CAST(SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 2, 'Number', 'ACTIVE_JOBS',
        CAST(SUM(CASE WHEN status IN ('Pending', 'Queued', 'Running') THEN 1 ELSE 0 END) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 2, 'Number', 'STALE_RUNNING_JOBS',
        CAST(COUNT(*) AS REAL)
    FROM jobs
    WHERE job_type = ? AND status = 'Running'
        AND run_at < UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 HOUR))
    
    UNION ALL
    SELECT 2, 'Percentage', 'KILL_RATE',
        CAST(ROUND(100.0 * SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS REAL)
    FROM jobs WHERE job_type = ?
    
    -- Time-based counts
    UNION ALL
    SELECT 3, 'Number', 'JOBS_PAST_HOUR',
        CAST(COUNT(*) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 HOUR))
    
    UNION ALL
    SELECT 3, 'Number', 'JOBS_TODAY',
        CAST(COUNT(*) AS REAL)
    FROM jobs
    WHERE job_type = ? AND DATE(FROM_UNIXTIME(run_at)) = CURDATE()
    
    UNION ALL
    SELECT 3, 'Number', 'KILLED_JOBS_TODAY',
        CAST(SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS REAL)
    FROM jobs
    WHERE job_type = ? AND DATE(FROM_UNIXTIME(run_at)) = CURDATE()
    
    UNION ALL
    SELECT 3, 'Decimal', 'AVG_JOBS_PER_MINUTE_PAST_HOUR',
        CAST(ROUND(COUNT(*) / 60.0, 2) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 HOUR))
    
    -- Totals
    UNION ALL
    SELECT 4, 'Number', 'TOTAL_JOBS',
        CAST(COUNT(*) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 4, 'Number', 'DONE_JOBS',
        CAST(SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 4, 'Number', 'COMPLETED_JOBS',
        CAST(SUM(CASE WHEN status IN ('Done', 'Failed', 'Killed') THEN 1 ELSE 0 END) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 4, 'Number', 'KILLED_JOBS',
        CAST(SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 4, 'Percentage', 'SUCCESS_RATE',
        CAST(ROUND(100.0 * SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS REAL)
    FROM jobs WHERE job_type = ?
    
    -- Duration metrics
    UNION ALL
    SELECT 5, 'Decimal', 'AVG_JOB_DURATION_MINS',
        CAST(ROUND(AVG((done_at - run_at) / 60.0), 2) AS REAL)
    FROM jobs
    WHERE job_type = ? AND status IN ('Done', 'Failed', 'Killed') AND done_at IS NOT NULL
    
    UNION ALL
    SELECT 5, 'Decimal', 'LONGEST_RUNNING_JOB_MINS',
        CAST(ROUND(MAX(CASE WHEN status = 'Running' THEN (UNIX_TIMESTAMP() - run_at) / 60.0 ELSE 0 END), 2) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 5, 'Number', 'QUEUE_BACKLOG',
        CAST(SUM(CASE WHEN status = 'Pending' AND run_at <= UNIX_TIMESTAMP() THEN 1 ELSE 0 END) AS REAL)
    FROM jobs WHERE job_type = ?
    
    -- Extended time ranges
    UNION ALL
    SELECT 6, 'Number', 'JOBS_PAST_24_HOURS',
        CAST(COUNT(*) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY))
    
    UNION ALL
    SELECT 6, 'Number', 'JOBS_PAST_7_DAYS',
        CAST(COUNT(*) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY))
    
    UNION ALL
    SELECT 6, 'Number', 'KILLED_JOBS_PAST_7_DAYS',
        CAST(SUM(CASE WHEN status = 'Killed' THEN 1 ELSE 0 END) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY))
    
    UNION ALL
    SELECT 6, 'Percentage', 'SUCCESS_RATE_PAST_24H',
        CAST(ROUND(100.0 * SUM(CASE WHEN status = 'Done' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY))
    
    -- Averages over time
    UNION ALL
    SELECT 7, 'Decimal', 'AVG_JOBS_PER_HOUR_PAST_24H',
        CAST(ROUND(COUNT(*) / 24.0, 2) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY))
    
    UNION ALL
    SELECT 7, 'Decimal', 'AVG_JOBS_PER_DAY_PAST_7D',
        CAST(ROUND(COUNT(*) / 7.0, 2) AS REAL)
    FROM jobs
    WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY))
    
    -- Timestamps
    UNION ALL
    SELECT 8, 'Timestamp', 'MOST_RECENT_JOB',
        CAST(MAX(run_at) AS REAL)
    FROM jobs WHERE job_type = ?
    
    UNION ALL
    SELECT 8, 'Timestamp', 'OLDEST_PENDING_JOB',
        CAST(MIN(run_at) AS REAL)
    FROM jobs
    WHERE job_type = ? AND status = 'Pending' AND run_at <= UNIX_TIMESTAMP()
    
    UNION ALL
    SELECT 8, 'Number', 'PEAK_HOUR_JOBS',
        CAST(MAX(hourly_count) AS REAL)
    FROM (
        SELECT COUNT(*) as hourly_count
        FROM jobs
        WHERE job_type = ? AND run_at >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY))
        GROUP BY HOUR(FROM_UNIXTIME(run_at))
    ) AS hourly_jobs
) results
ORDER BY priority, statistic
