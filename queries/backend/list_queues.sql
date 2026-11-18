WITH queue_stats AS (
    SELECT
        job_type,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'title', statistic,
                'stat_type', type,
                'value', value,
                'priority', priority
            )
        ) as stats
    FROM (
        SELECT
            job_type,
            1 AS priority,
            'Number' AS type,
            'RUNNING_JOBS' AS statistic,
            CAST(SUM(IF(status = 'Running', 1, 0)) AS CHAR) AS value
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            1,
            'Number',
            'PENDING_JOBS',
            CAST(SUM(IF(status = 'Pending', 1, 0)) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            1,
            'Number',
            'FAILED_JOBS',
            CAST(SUM(IF(status = 'Failed', 1, 0)) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            2,
            'Number',
            'ACTIVE_JOBS',
            CAST(SUM(IF(status IN ('Pending', 'Queued', 'Running'), 1, 0)) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            2,
            'Number',
            'STALE_RUNNING_JOBS',
            CAST(COUNT(*) AS CHAR)
        FROM jobs
        WHERE status = 'Running'
          AND run_at < DATE_SUB(NOW(), INTERVAL 1 HOUR)
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            2,
            'Percentage',
            'KILL_RATE',
            CAST(ROUND(100.0 * SUM(IF(status = 'Killed', 1, 0)) / NULLIF(COUNT(*), 0), 2) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            3,
            'Number',
            'JOBS_PAST_HOUR',
            CAST(COUNT(*) AS CHAR)
        FROM jobs
        WHERE run_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            3,
            'Number',
            'JOBS_TODAY',
            CAST(COUNT(*) AS CHAR)
        FROM jobs
        WHERE DATE(run_at) = DATE(NOW())
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            3,
            'Number',
            'KILLED_JOBS_TODAY',
            CAST(SUM(IF(status = 'Killed', 1, 0)) AS CHAR)
        FROM jobs
        WHERE DATE(run_at) = DATE(NOW())
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            3,
            'Decimal',
            'AVG_JOBS_PER_MINUTE_PAST_HOUR',
            CAST(ROUND(COUNT(*) / 60.0, 2) AS CHAR)
        FROM jobs
        WHERE run_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            4,
            'Number',
            'TOTAL_JOBS',
            CAST(COUNT(*) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            4,
            'Number',
            'DONE_JOBS',
            CAST(SUM(IF(status = 'Done', 1, 0)) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            4,
            'Number',
            'KILLED_JOBS',
            CAST(SUM(IF(status = 'Killed', 1, 0)) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            4,
            'Percentage',
            'SUCCESS_RATE',
            CAST(ROUND(100.0 * SUM(IF(status = 'Done', 1, 0)) / NULLIF(COUNT(*), 0), 2) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            5,
            'Decimal',
            'AVG_JOB_DURATION_MINS',
            CAST(ROUND(AVG(TIMESTAMPDIFF(SECOND, run_at, done_at) / 60.0), 2) AS CHAR)
        FROM jobs
        WHERE status IN ('Done', 'Failed', 'Killed')
          AND done_at IS NOT NULL
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            5,
            'Decimal',
            'LONGEST_RUNNING_JOB_MINS',
            CAST(ROUND(MAX(IF(status = 'Running', TIMESTAMPDIFF(SECOND, run_at, NOW()) / 60.0, 0)), 2) AS CHAR)
        FROM jobs
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            6,
            'Number',
            'JOBS_PAST_7_DAYS',
            CAST(COUNT(*) AS CHAR)
        FROM jobs
        WHERE run_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        GROUP BY job_type
        UNION ALL
        SELECT
            job_type,
            8,
            'Timestamp',
            'MOST_RECENT_JOB',
            CAST(UNIX_TIMESTAMP(MAX(run_at)) AS CHAR)
        FROM jobs
        GROUP BY job_type
    ) t
    GROUP BY job_type
),
daily_counts AS (
    SELECT
        job_type,
        DATE(run_at) AS day,
        COUNT(*) AS daily_count
    FROM jobs
    WHERE run_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
    GROUP BY job_type, DATE(run_at)
),
queue_activity AS (
    SELECT
        job_type,
        JSON_EXTRACT(
            CONCAT('[', GROUP_CONCAT(daily_count ORDER BY day SEPARATOR ','), ']'),
            '$'
        ) AS activity
    FROM daily_counts
    GROUP BY job_type
),
all_job_types AS (
    SELECT worker_type AS job_type FROM workers
    UNION
    SELECT job_type FROM jobs
)
SELECT
    jt.job_type as name,
    COALESCE(qs.stats, JSON_ARRAY()) as stats,
    COALESCE(
        (
            SELECT JSON_ARRAYAGG(lock_by)
            FROM jobs
            WHERE job_type = jt.job_type
              AND lock_by IS NOT NULL
        ),
        JSON_ARRAY()
    ) as workers,
    COALESCE(qa.activity, JSON_ARRAY()) as activity
FROM all_job_types jt
LEFT JOIN queue_stats qs ON jt.job_type = qs.job_type
LEFT JOIN queue_activity qa ON jt.job_type = qa.job_type
ORDER BY name;
